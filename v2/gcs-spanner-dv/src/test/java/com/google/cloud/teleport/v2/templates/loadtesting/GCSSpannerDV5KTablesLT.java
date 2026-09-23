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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load test for the {@link GCSSpannerDV} template validating 5,000 tables against a pre-generated
 * GCS Avro dataset ({@code gs://nokill-avro-to-spanner-dv/5k_table_test}).
 *
 * <p>Each table ({@code table_0} through {@code table_4999}) contains 3 rows:
 *
 * <pre>
 *   id         = r                       (1..3)
 *   data       = "table_{t}_row_{r}"
 *   created_at = 2026-01-01T00:00:0{r}Z
 * </pre>
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDV5KTablesLT extends GCSSpannerDVLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDV5KTablesLT.class);

  private static final int NUM_TABLES = 5000;
  private static final int ROWS_PER_TABLE = 3;
  private static final int DDL_BATCH_SIZE = 1000;
  private static final int TABLES_PER_COMMIT = 500;
  private static final String GCS_INPUT_DIRECTORY = "gs://nokill-avro-to-spanner-dv/5k_table_test";
  private static final Duration JOB_TIMEOUT = Duration.ofMinutes(60);

  @Test
  public void validate5KTablesWithMatchingRecords() throws Exception {
    // 1. Create 5,000 tables in Spanner
    LOG.info("Creating {} tables in Spanner in batches of {}", NUM_TABLES, DDL_BATCH_SIZE);
    List<String> statements = new ArrayList<>(NUM_TABLES);
    for (int table = 0; table < NUM_TABLES; table++) {
      statements.add(
          String.format(
              "CREATE TABLE IF NOT EXISTS table_%d ("
                  + "id INT64 NOT NULL, "
                  + "data STRING(64), "
                  + "created_at TIMESTAMP"
                  + ") PRIMARY KEY (id)",
              table));
    }

    for (int start = 0; start < statements.size(); start += DDL_BATCH_SIZE) {
      int end = Math.min(statements.size(), start + DDL_BATCH_SIZE);
      spannerResourceManager.executeDdlStatements(statements.subList(start, end));
    }

    // 2. Populate 3 rows per table in Spanner to match the static Avro source
    LOG.info("Populating {} rows per table across {} tables", ROWS_PER_TABLE, NUM_TABLES);
    List<Mutation> batch = new ArrayList<>(TABLES_PER_COMMIT * ROWS_PER_TABLE);

    for (int table = 0; table < NUM_TABLES; table++) {
      String tableName = "table_" + table;
      for (int row = 1; row <= ROWS_PER_TABLE; row++) {
        batch.add(
            Mutation.newInsertOrUpdateBuilder(tableName)
                .set("id")
                .to(row)
                .set("data")
                .to(String.format("table_%d_row_%d", table, row))
                .set("created_at")
                .to(Timestamp.parseTimestamp(String.format("2026-01-01T00:00:0%dZ", row)))
                .build());
      }

      if (batch.size() >= TABLES_PER_COMMIT * ROWS_PER_TABLE || table == NUM_TABLES - 1) {
        spannerResourceManager.write(batch);
        batch.clear();
      }
    }

    // 3. Launch validation pipeline and wait for completion
    launchValidationJob(GCS_INPUT_DIRECTORY, JOB_TIMEOUT);

    // 4. Assert BigQuery validation results
    LOG.info("Validating BigQuery results for {} tables", NUM_TABLES);
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        List.of(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ (long) NUM_TABLES,
                /* totalRowsMatched= */ (long) NUM_TABLES * ROWS_PER_TABLE,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    List<TableValidationStatsDto> expectedStats = new ArrayList<>(NUM_TABLES);
    for (int table = 0; table < NUM_TABLES; table++) {
      expectedStats.add(
          new TableValidationStatsDto(
              /* schemaName= */ null,
              /* tableName= */ "table_" + table,
              /* status= */ "MATCH",
              /* sourceRowCount= */ (long) ROWS_PER_TABLE,
              /* destinationRowCount= */ (long) ROWS_PER_TABLE,
              /* matchedRowCount= */ (long) ROWS_PER_TABLE,
              /* mismatchRowCount= */ 0L));
    }
    GCSSpannerDVTestAsserts.assertTableValidationStats(bigQueryResourceManager, expectedStats);
  }
}
