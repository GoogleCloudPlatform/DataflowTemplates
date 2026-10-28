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

import com.google.cloud.spanner.Value;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template where the shadow tables are in
 * a different database than the target tables, with focus on PK type coverage and updates. The
 * change events are produced by a real Datastream stream reading SQL Server CDC. This tests
 * handling of INSERT, UPDATE and DELETE events, including updates of primary key columns (the
 * source emits a DELETE followed by an INSERT for those).
 *
 * <p>SQL Server specific deviations from the MySQL reference ({@code
 * SeparateShadowTableDatabasePKFocusedIT}):
 *
 * <ul>
 *   <li>The three initial INSERTs of each table are written before the job is launched, so they are
 *       delivered through the Datastream backfill; UPDATE / DELETE / PK update are applied
 *       afterwards as CDC.
 *   <li>The TIMESTAMP key column is backed by {@code datetimeoffset} (UTC offset) because {@code
 *       datetime}/{@code datetime2} values are delivered as an unhandled Datastream {@code
 *       datetime} Avro RECORD and routed to the DLQ.
 *   <li>Datastream delivers {@code varbinary} values base64 encoded, but {@code
 *       ChangeEventTypeConvertor.toByteArray} hex-decodes any STRING to BYTES input for non
 *       PostgreSQL sources (see {@code SQLServerDatastreamToSpannerDataTypesIT}), so the bytes do
 *       not round-trip. {@code bytes_field} is therefore kept as a nullable non-key column that is
 *       written but not asserted, and the composite PK covers the remaining 7 key types.
 *   <li>After the PK update, in addition to the row-count check, the test waits for the new key to
 *       be visible in Spanner since Datastream CDC latency may exceed the cutover sleep.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerSeparateShadowTableDatabasePKFocusedIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerSeparateShadowTableDatabasePKFocusedIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerSeparateShadowTableDatabasePKFocusedIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerSeparateShadowTableDatabasePKFocusedIT/spanner-schema.sql";
  private static final String GCS_PATH_PREFIX = "sqlserver-separate-shadow-table-database-pk";

  private static final String MY_TABLE = "my_table";
  private static final String ALLTYPES_TABLE = "alltypes";

  private static final String STRING_VALUE = "This is a test string for SQL Server.";
  // The record matcher compares Spanner Value.toString(), which elides strings longer than 36
  // characters, so the expected value must be rendered the same way.
  private static final String EXPECTED_STRING_VALUE = Value.string(STRING_VALUE).toString();
  // TODO: bytes_field is excluded from the PK and from the asserts until the convertor
  // base64-decodes SQL Server binary values instead of hex-decoding them.
  private static final List<String> ALLTYPES_ASSERTED_COLUMNS =
      List.of(
          "bool_field",
          "int64_field",
          "float64_field",
          "string_field",
          "timestamp_field",
          "date_field",
          "numeric_field",
          "val");

  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static CloudSqlServerResourceManager msSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager shadowSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static final Set<SQLServerSeparateShadowTableDatabasePKFocusedIT> testInstances =
      ConcurrentHashMap.newKeySet();

  /**
   * Setup resource managers, write the backfill data and launch the Dataflow job once during the
   * execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (SQLServerSeparateShadowTableDatabasePKFocusedIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up SQL Server test container resource manager...");
        msSqlResourceManager = setUpSqlServerResourceManager();
        LOG.info("SQL Server resource manager created with URI: {}", msSqlResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());
        LOG.info("Setting up shadow table Spanner resource manager...");
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        LOG.info(
            "Shadow Spanner resource manager created with database ID: {}",
            shadowSpannerResourceManager.getDatabaseId());
        LOG.info("Setting up GCS resource manager...");
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        LOG.info("GCS resource manager created with bucket: {}", gcsResourceManager.getBucket());
        LOG.info("Setting up Pub/Sub resource manager...");
        pubsubResourceManager = setUpPubSubResourceManager();
        LOG.info("Pub/Sub resource manager created.");
        LOG.info("Setting up Datastream resource manager...");
        datastreamResourceManager = setUpDatastreamResourceManager();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing SQL Server DDL script...");
        executeSqlScript(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
        msSqlResourceManager.enableCdcOnAllTables();

        LOG.info("Writing backfill data to SQL Server...");
        insertMyTableRows();
        insertAllTypesRows();

        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        SqlServerSource sqlServerSource =
            SqlServerSource.builder(
                    msSqlResourceManager.getHost(),
                    msSqlResourceManager.getUsername(),
                    msSqlResourceManager.getPassword(),
                    msSqlResourceManager.getPort(),
                    msSqlResourceManager.getDatabaseName())
                .setAllowedTables(Map.of("dbo", List.of(MY_TABLE, ALLTYPES_TABLE)))
                .build();

        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        jobParameters.put(
            "shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "sqlserver-shadow-pk",
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                sqlServerSource);
        assertThatPipeline(jobInfo).isRunning();

        initialized = true;
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    // A failing teardown of one instance must not prevent the other instances' Dataflow jobs and
    // the shared resource managers below from being cleaned up.
    for (SQLServerSeparateShadowTableDatabasePKFocusedIT instance : testInstances) {
      try {
        instance.tearDownBase();
      } catch (Exception e) {
        LOG.error("Failed to tear down test instance {}", instance.testName, e);
      }
    }

    // Datastream streams must be deleted before the CDC-enabled source database is dropped.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    ResourceManagerUtils.cleanResources(
        msSqlResourceManager,
        spannerResourceManager,
        shadowSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  /**
   * This test works on a simple schema of 2 columns: id INT (PK), and val int. It tests INSERT,
   * DELETE, UPDATE of val (non-pk) and update of id (PK, the source generates a DELETE and an
   * INSERT event for PK updates).
   */
  @Test
  public void migrationTestSimpleTable() {
    // 1. The 3 INSERTs were written before launch (backfill).
    // 2. Wait on Spanner to have 3 rows.
    LOG.info("Waiting for the backfill of {}...", MY_TABLE);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                rowCountCheck(MY_TABLE, 3));
    assertThatResult(result).meetsConditions();

    // 3. Send an UPDATE val and a DELETE.
    // 4. Wait on Spanner to have 2 rows.
    LOG.info("Applying UPDATE val and DELETE on {}...", MY_TABLE);
    msSqlResourceManager.runSQLUpdate("UPDATE my_table SET val = 20 WHERE id = 2");
    msSqlResourceManager.runSQLUpdate("DELETE FROM my_table WHERE id = 3");
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), rowCountCheck(MY_TABLE, 2));
    assertThatResult(result).meetsConditions();

    // 5. Send a PK UPDATE (the source emits DELETE + INSERT).
    // 6. Wait on Spanner to have 2 rows including the new key.
    LOG.info("Applying PK UPDATE on {}...", MY_TABLE);
    msSqlResourceManager.runSQLUpdate("UPDATE my_table SET id = 10 WHERE id = 1");
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                rowCountCheck(MY_TABLE, 2)
                    .and(
                        rowPresentCheck(
                            "my_table id 10 present", "SELECT id FROM my_table WHERE id = 10")));
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertMyTableContents();
  }

  /**
   * This test works on a schema having a PK consisting of all Spanner key types SQL Server can
   * supply (BOOL, INT64, FLOAT64, STRING, TIMESTAMP, DATE, NUMERIC; BYTES is a non-key column, see
   * the class javadoc). It tests INSERT, DELETE, UPDATE of val (non-pk) and update of a PK column
   * (the source generates a DELETE and an INSERT event for PK updates).
   *
   * <p>TODO: Add JSON type to PK once lock hint via read API is supported.
   */
  @Test
  public void migrationTestAllDataType() {
    // 1. The 3 INSERTs were written before launch (backfill).
    // 2. Wait on Spanner to have 3 rows.
    LOG.info("Waiting for the backfill of {}...", ALLTYPES_TABLE);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                rowCountCheck(ALLTYPES_TABLE, 3));
    assertThatResult(result).meetsConditions();

    // 3. Send an UPDATE val and a DELETE.
    // 4. Wait on Spanner to have 2 rows.
    LOG.info("Applying UPDATE val and DELETE on {}...", ALLTYPES_TABLE);
    msSqlResourceManager.runSQLUpdate("UPDATE alltypes SET val = 20 WHERE int64_field = 2");
    msSqlResourceManager.runSQLUpdate("DELETE FROM alltypes WHERE int64_field = 3");
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), rowCountCheck(ALLTYPES_TABLE, 2));
    assertThatResult(result).meetsConditions();

    // 5. Send a PK UPDATE (the source emits DELETE + INSERT).
    // 6. Wait on Spanner to have 2 rows including the new key.
    LOG.info("Applying PK UPDATE on {}...", ALLTYPES_TABLE);
    msSqlResourceManager.runSQLUpdate("UPDATE alltypes SET int64_field = 10 WHERE int64_field = 1");
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                rowCountCheck(ALLTYPES_TABLE, 2)
                    .and(
                        rowPresentCheck(
                            "alltypes int64_field 10 present",
                            "SELECT int64_field FROM alltypes WHERE int64_field = 10")));
    assertThatResult(result).meetsConditions();

    // Assert specific rows
    assertAllTypesContents();
  }

  private static void insertMyTableRows() {
    msSqlResourceManager.runSQLUpdate("INSERT INTO my_table (id, val) VALUES (1, 10)");
    msSqlResourceManager.runSQLUpdate("INSERT INTO my_table (id, val) VALUES (2, 200)");
    msSqlResourceManager.runSQLUpdate("INSERT INTO my_table (id, val) VALUES (3, 30)");
  }

  private static void insertAllTypesRows() {
    insertAllTypesRow(1, 10);
    insertAllTypesRow(2, 200);
    insertAllTypesRow(3, 30);
  }

  private static void insertAllTypesRow(long int64Field, int val) {
    msSqlResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO alltypes (bool_field, int64_field, float64_field, string_field,"
                + " bytes_field, timestamp_field, date_field, numeric_field, val) VALUES"
                + " (1, %d, 3.14159265359E0, N'%s', CONVERT(VARBINARY(100), 'This is some binary"
                + " data'), '2024-12-20 10:30:00.0000000 +00:00', '2024-12-20', 12345.1234, %d)",
            int64Field, STRING_VALUE, val));
  }

  private static ConditionCheck rowCountCheck(String table, int rows) {
    return SpannerRowsCheck.builder(spannerResourceManager, table)
        .setMinRows(rows)
        .setMaxRows(rows)
        .build();
  }

  /** A condition that passes once the given query returns at least one row from Spanner. */
  private static ConditionCheck rowPresentCheck(String description, String query) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return description;
      }

      @Override
      protected CheckResult check() {
        int found = spannerResourceManager.runQuery(query).size();
        return new CheckResult(
            found > 0, String.format("%s: query returned %d row(s)", description, found));
      }
    };
  }

  private void assertMyTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 2);
    row1.put("val", 20);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 10);
    row2.put("val", 10);
    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from my_table"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertAllTypesContents() {
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    // First record
    Map<String, Object> record1 = new HashMap<>();
    record1.put("bool_field", true);
    record1.put("int64_field", 2L);
    record1.put("float64_field", 3.14159265359);
    record1.put("string_field", EXPECTED_STRING_VALUE);
    record1.put("timestamp_field", "2024-12-20T10:30:00Z");
    record1.put("date_field", "2024-12-20");
    record1.put("numeric_field", "12345.1234");
    record1.put("val", 20);

    // Second record
    Map<String, Object> record2 = new HashMap<>();
    record2.put("bool_field", true);
    record2.put("int64_field", 10L);
    record2.put("float64_field", 3.14159265359);
    record2.put("string_field", EXPECTED_STRING_VALUE);
    record2.put("timestamp_field", "2024-12-20T10:30:00Z");
    record2.put("date_field", "2024-12-20");
    record2.put("numeric_field", "12345.1234");
    record2.put("val", 10);

    expectedRecords.add(record1);
    expectedRecords.add(record2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(ALLTYPES_TABLE, ALLTYPES_ASSERTED_COLUMNS))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }
}
