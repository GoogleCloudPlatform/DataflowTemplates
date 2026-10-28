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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link DataStreamToSpanner} Flex template which tests a live SQL Server
 * Datastream migration into a Spanner database whose {@code default_time_zone} is not UTC ({@code
 * Australia/Brisbane}, UTC+10). The test verifies that date/time values written during backfill and
 * during CDC are stored unshifted, i.e. the Spanner database default time zone does not leak into
 * the replicated values.
 *
 * <p>This is the SQL Server counterpart of {@code DataStreamToSpannerTimezoneIT}, with the
 * following SQL Server specific deviations:
 *
 * <ul>
 *   <li>TODO: SQL Server {@code datetime}, {@code datetime2} and {@code smalldatetime} are
 *       delivered by Datastream as an unhandled {@code datetime} Avro RECORD and are currently
 *       routed to the DLQ for TIMESTAMP targets. Those types are therefore excluded here; both
 *       {@code timestamp_column} and {@code datetime_column} (names kept from the reference) are
 *       declared as {@code datetimeoffset} on the source and populated with explicit {@code +00:00}
 *       offsets.
 *   <li>An additional {@code date_column} ({@code date} to {@code DATE}) is replicated to also
 *       cover the DATE mapping in a non-UTC database.
 *   <li>Instead of replaying a pre-recorded Avro file, rows are inserted into the source before the
 *       job is launched (backfill) and after the backfill has landed (CDC).
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDataStreamToSpannerTimezoneIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerDataStreamToSpannerTimezoneIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerTimezoneIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerTimezoneIT/spanner-schema.sql";

  private static final String TABLE1 = "DateData";
  private static final String SQLSERVER_SCHEMA = "dbo";

  private static final String ID = "id";
  private static final String TIMESTAMP_COLUMN = "timestamp_column";
  private static final String DATETIME_COLUMN = "datetime_column";
  private static final String DATE_COLUMN = "date_column";

  private CloudSqlServerResourceManager sqlServerResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up SQL Server test container resource manager...");
    sqlServerResourceManager = setUpSqlServerResourceManager();
    LOG.info("SQL Server resource manager created with URI: {}", sqlServerResourceManager.getUri());
    LOG.info("Setting up Spanner resource manager...");
    spannerResourceManager = setUpSpannerResourceManager();
    LOG.info("Setting up Pub/Sub resource manager...");
    pubsubResourceManager = setUpPubSubResourceManager();
    LOG.info("Setting up Datastream resource manager...");
    datastreamResourceManager = setUpDatastreamResourceManager();
    LOG.info("Setting up GCS resource manager...");
    gcsResourceManager = setUpSpannerITGcsResourceManager();
  }

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources...");
    // Datastream streams must be deleted before the CDC-enabled source database is dropped.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);
    ResourceManagerUtils.cleanResources(
        sqlServerResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testTimezoneHandling() throws Exception {
    LOG.info("Executing SQL Server DDL...");
    executeSqlScript(sqlServerResourceManager, SQLSERVER_DDL_RESOURCE);
    sqlServerResourceManager.enableCdcOnAllTables();

    LOG.info("Creating Spanner DDL (database default_time_zone = Australia/Brisbane)...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    // Backfill data: inserted before the stream exists so it is picked up by the initial snapshot.
    LOG.info("Inserting backfill rows into SQL Server...");
    insertDateData(1, "2024-02-02 00:00:00 +00:00", "2024-02-02 10:00:00 +00:00", "2024-02-02");
    insertDateData(2, "2024-02-02 10:00:00 +00:00", "2024-02-02 20:00:00 +00:00", "2024-02-02");

    SqlServerSource sqlServerSource =
        SqlServerSource.builder(
                sqlServerResourceManager.getHost(),
                sqlServerResourceManager.getUsername(),
                sqlServerResourceManager.getPassword(),
                sqlServerResourceManager.getPort(),
                sqlServerResourceManager.getDatabaseName())
            .setAllowedTables(Map.of(SQLSERVER_SCHEMA, List.of(TABLE1)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "sqlserver-timezone",
            null,
            null,
            "sqlserver-datastream-to-spanner-timezone",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            sqlServerSource);
    assertThatPipeline(jobInfo).isRunning();

    // 1. Wait for the backfill rows to land in Spanner.
    LOG.info("Waiting for backfill rows to be replicated to Spanner...");
    PipelineOperator.Result backfillResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                    .setMinRows(2)
                    .setMaxRows(2)
                    .build());
    assertThatResult(backfillResult).meetsConditions();
    assertBackfillContents();

    // 2. CDC: insert one more row and wait for it to be replicated.
    LOG.info("Inserting CDC row into SQL Server...");
    insertDateData(3, "2024-02-02 20:00:00 +00:00", "2024-02-03 06:00:00 +00:00", "2024-02-03");

    LOG.info("Waiting for CDC row to be replicated to Spanner...");
    PipelineOperator.Result cdcResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                    .setMinRows(3)
                    .setMaxRows(3)
                    .build());
    assertThatResult(cdcResult).meetsConditions();

    assertCdcContents();
  }

  private void insertDateData(
      int id, String timestampColumn, String datetimeColumn, String dateColumn) {
    sqlServerResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s (%s, %s, %s, %s) VALUES (%d, '%s', '%s', '%s')",
            TABLE1,
            ID,
            TIMESTAMP_COLUMN,
            DATETIME_COLUMN,
            DATE_COLUMN,
            id,
            timestampColumn,
            datetimeColumn,
            dateColumn));
  }

  private static Map<String, Object> expectedRow(
      long id, String timestampColumn, String datetimeColumn, String dateColumn) {
    Map<String, Object> row = new HashMap<>();
    row.put(ID, id);
    row.put(TIMESTAMP_COLUMN, Timestamp.parseTimestamp(timestampColumn));
    row.put(DATETIME_COLUMN, Timestamp.parseTimestamp(datetimeColumn));
    row.put(DATE_COLUMN, Date.parseDate(dateColumn));
    return row;
  }

  private static List<Map<String, Object>> expectedBackfillRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(expectedRow(1L, "2024-02-02T00:00:00Z", "2024-02-02T10:00:00Z", "2024-02-02"));
    expectedRows.add(expectedRow(2L, "2024-02-02T10:00:00Z", "2024-02-02T20:00:00Z", "2024-02-02"));
    return expectedRows;
  }

  private void assertBackfillContents() {
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                String.format(
                    "SELECT %s, %s, %s, %s FROM %s",
                    ID, TIMESTAMP_COLUMN, DATETIME_COLUMN, DATE_COLUMN, TABLE1)))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedBackfillRows());
  }

  private void assertCdcContents() {
    List<Map<String, Object>> expectedRows = expectedBackfillRows();
    expectedRows.add(expectedRow(3L, "2024-02-02T20:00:00Z", "2024-02-03T06:00:00Z", "2024-02-03"));

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                String.format(
                    "SELECT %s, %s, %s, %s FROM %s",
                    ID, TIMESTAMP_COLUMN, DATETIME_COLUMN, DATE_COLUMN, TABLE1)))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRows);
  }
}
