/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.loadtesting;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load test for the {@link GCSSpannerDV} template at Cloud Spanner's ceiling of 5,000 tables per
 * database.
 *
 * <p>This is a <b>sanity</b> test, not a performance test: every table holds only three rows, so
 * the pipeline is exercised on metadata fan-out (a 5,000-table DDL side input, 5,000 Spanner read
 * operations, ~15,000 small Avro objects, 5,000 {@code TableValidationStats} rows) rather than on
 * data volume.
 *
 * <h2>Source fixture</h2>
 *
 * The Avro source is a <b>static, pre-generated</b> bucket that must not be regenerated:
 *
 * <pre>gs://nokill-avro-to-spanner-dv/5k_table_test</pre>
 *
 * It holds 5,000 prefixes {@code table_0/} … {@code table_4999/} containing 15,000 rows in 14,832
 * objects. The object count is lower than the row count because 84 tables were written as a single
 * three-row file while the rest were written as three single-row files. Assertions must therefore
 * be expressed in rows and tables, never in files or shard counts.
 *
 * <h2>Destination fixture</h2>
 *
 * The Spanner schema and data are created by this test and mirror the source exactly, so a correct
 * pipeline reports every table as {@code MATCH}. For table {@code t} and row {@code r} in 1..3:
 *
 * <pre>
 *   id         = r                       (restarts at 1 in every table)
 *   data       = "table_{t}_row_{r}"
 *   created_at = 2026-01-01T00:00:0{r}Z
 * </pre>
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDV5KTablesLT extends GCSSpannerDVLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDV5KTablesLT.class);

  /**
   * Spanner's hard limit of tables per database. Intentionally a constant and not a system
   * property: a configurable table count would allow this test to run with a handful of tables in
   * CI while still reporting itself as the 5,000-table test.
   */
  private static final int NUM_TABLES = 5000;

  private static final int ROWS_PER_TABLE = 3;

  /**
   * DDL statements per {@code UpdateDatabaseDdl} call. Batches are submitted sequentially: Spanner
   * serialises schema changes per database, so parallel submission adds contention without adding
   * throughput.
   */
  private static final int DDL_BATCH_SIZE = 1000;

  /**
   * Tables per commit when populating rows. 500 tables is 1,500 rows and roughly 4,500 cell
   * mutations, well inside Spanner's 80,000-mutation commit limit.
   */
  private static final int TABLES_PER_COMMIT = 500;

  /** Tables spot-checked after population, in addition to the schema-wide table count. */
  private static final int[] SPOT_CHECK_TABLES = {0, 2499, 4999};

  private static final int FULL_ROW_CHECK_TABLE = 7;

  /**
   * Provisions a 5,000-table Spanner fixture that mirrors the static Avro source, then verifies the
   * fixture landed correctly. Launching the validation job and asserting on its BigQuery output are
   * added in a later change.
   */
  @Test
  public void validate5KTablesWithMatchingRecords() throws Exception {
    createSpannerSchema();
    populateSpannerRows();
    verifySpannerFixture();
  }

  /** Creates {@value #NUM_TABLES} tables in sequential batches of {@value #DDL_BATCH_SIZE}. */
  private void createSpannerSchema() throws Exception {
    phase(
        "spanner-ddl",
        () -> {
          List<String> statements = new ArrayList<>(NUM_TABLES);
          for (int table = 0; table < NUM_TABLES; table++) {
            statements.add(createTableDdl(table));
          }

          for (int start = 0; start < statements.size(); start += DDL_BATCH_SIZE) {
            int end = Math.min(statements.size(), start + DDL_BATCH_SIZE);
            Instant batchStart = Instant.now();

            spannerResourceManager.executeDdlStatements(statements.subList(start, end));

            LOG.info(
                "{} DDL batch [{}, {}) applied in {}s",
                LOG_TAG,
                start,
                end,
                Duration.between(batchStart, Instant.now()).toSeconds());
          }
          LOG.info("{} Created {} tables", LOG_TAG, NUM_TABLES);
        });
  }

  /** Writes {@value #ROWS_PER_TABLE} rows into every table, committing every batch of tables. */
  private void populateSpannerRows() throws Exception {
    phase(
        "spanner-write",
        () -> {
          List<Mutation> batch = new ArrayList<>(TABLES_PER_COMMIT * ROWS_PER_TABLE);
          int commits = 0;

          for (int table = 0; table < NUM_TABLES; table++) {
            for (int row = 1; row <= ROWS_PER_TABLE; row++) {
              batch.add(rowMutation(table, row));
            }

            boolean lastTable = table == NUM_TABLES - 1;
            if (batch.size() >= TABLES_PER_COMMIT * ROWS_PER_TABLE || lastTable) {
              Instant commitStart = Instant.now();
              spannerResourceManager.write(batch);
              commits++;
              LOG.info(
                  "{} Commit {} wrote {} rows (through table_{}) in {}s",
                  LOG_TAG,
                  commits,
                  batch.size(),
                  table,
                  Duration.between(commitStart, Instant.now()).toSeconds());
              batch.clear();
            }
          }
          LOG.info(
              "{} Wrote {} rows across {} tables in {} commits",
              LOG_TAG,
              NUM_TABLES * ROWS_PER_TABLE,
              NUM_TABLES,
              commits);
        });
  }

  /**
   * Confirms the fixture landed as intended before anything downstream depends on it.
   *
   * <p>The information-schema count validates the entire DDL phase in one query. Per-table row
   * counts are spot-checked rather than exhaustively verified, because the pipeline's own {@code
   * TableValidationStats} output verifies all 5,000 tables once the job is added.
   */
  private void verifySpannerFixture() throws Exception {
    phase(
        "spanner-verify",
        () -> {
          // No TABLE_TYPE predicate: the database is created fresh by this test and holds no
          // views, so an unqualified count over the default schema is exact.
          long tableCount =
              spannerResourceManager
                  .runQuery(
                      "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''")
                  .get(0)
                  .getLong(0);
          LOG.info("{} Information schema reports {} tables", LOG_TAG, tableCount);
          assertWithMessage("tables present in the information schema")
              .that(tableCount)
              .isEqualTo((long) NUM_TABLES);

          for (int table : SPOT_CHECK_TABLES) {
            String tableName = tableName(table);
            long rowCount = spannerResourceManager.getRowCount(tableName);
            LOG.info("{} {} contains {} rows", LOG_TAG, tableName, rowCount);
            assertWithMessage("row count of %s", tableName)
                .that(rowCount)
                .isEqualTo((long) ROWS_PER_TABLE);
          }

          String fullCheckTable = tableName(FULL_ROW_CHECK_TABLE);
          List<Struct> rows =
              spannerResourceManager.runQuery(
                  String.format("SELECT id, data, created_at FROM %s ORDER BY id", fullCheckTable));
          LOG.info("{} {} rows: {}", LOG_TAG, fullCheckTable, rows);

          assertThat(rows).hasSize(ROWS_PER_TABLE);
          for (int row = 1; row <= ROWS_PER_TABLE; row++) {
            Struct actual = rows.get(row - 1);
            assertWithMessage("%s id of row %s", fullCheckTable, row)
                .that(actual.getLong("id"))
                .isEqualTo((long) row);
            assertWithMessage("%s data of row %s", fullCheckTable, row)
                .that(actual.getString("data"))
                .isEqualTo(rowData(FULL_ROW_CHECK_TABLE, row));
            assertWithMessage("%s created_at of row %s", fullCheckTable, row)
                .that(actual.getTimestamp("created_at"))
                .isEqualTo(rowTimestamp(row));
          }
        });
  }

  private static String tableName(int table) {
    return "table_" + table;
  }

  /**
   * {@code IF NOT EXISTS} is required rather than cosmetic: the resource manager retries a failed
   * {@code UpdateDatabaseDdl} on quota errors, and a retry of a partially applied batch would
   * otherwise fail with "Duplicate name in schema".
   */
  private static String createTableDdl(int table) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ("
            + "  id INT64 NOT NULL,"
            + "  data STRING(64),"
            + "  created_at TIMESTAMP"
            + ") PRIMARY KEY (id)",
        tableName(table));
  }

  private static Mutation rowMutation(int table, int row) {
    return Mutation.newInsertOrUpdateBuilder(tableName(table))
        .set("id")
        .to(row)
        .set("data")
        .to(rowData(table, row))
        .set("created_at")
        .to(rowTimestamp(row))
        .build();
  }

  private static String rowData(int table, int row) {
    return String.format("table_%d_row_%d", table, row);
  }

  private static Timestamp rowTimestamp(int row) {
    return Timestamp.parseTimestamp(String.format("2026-01-01T00:00:0%dZ", row));
  }
}
