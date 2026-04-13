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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
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
 * Integration test for live replication from MySQL to Spanner using the retryDLQ mode.
 *
 * <p>Objective: Verify that the retryDLQ batch job correctly processes and retries severe/ Dead
 * Letter Queue (DLQ) events alongside an actively running streaming pipeline (that processes retry/
 * errors).
 *
 * <p>Edge cases covered in this test include:
 *
 * <ul>
 *   <li>Handling retriable errors such as foreign key violations via the regular pipeline.
 *   <li>Processing severe errors introduced by custom transformation failures and check constraints
 *       via the retryDLQ pipeline.
 *   <li>Retrying fixed items successfully in both retry/ and severe/ buckets: e.g. fixing a foreign
 *       key violation by inserting a missing parent row, and using a corrected transformation file.
 *   <li>Ensuring non-fixable items remain correctly logged under their respective error buckets.
 *   <li>Validating schema complexities between Source and Spanner, including mismatched primary
 *       keys, added, deleted, and renamed columns, as well as all datatypes.
 *   <li>Utilizing the schema overrides file to reconcile schema differences.
 *   <li>Utilizing the DLQ Pub/Sub consumer.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerMySQLRetryDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerMySQLRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerMySQLRetryDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "DataStreamToSpannerMySQLRetryDLQIT/mysql-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "DataStreamToSpannerMySQLRetryDLQIT/overrides.json";
  private static final String GCS_PATH_PREFIX = "mysql-datastream-to-spanner-retrydlq-test";

  private static final HashSet<DataStreamToSpannerMySQLRetryDLQIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static CloudMySQLResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerMySQLRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        // Setup MySQL Source
        jdbcResourceManager = CloudMySQLResourceManager.builder(testName).build();

        // Create MySQL Schema using helper
        executeSqlScript(jdbcResourceManager, MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        pubsubResourceManager = setUpPubSubResourceManager();

        // Upload overrides file
        gcsResourceManager.uploadArtifact(
            "input/overrides.json", Resources.getResource(OVERRIDES_FILE_RESOURCE).getPath());

        // Upload custom transformation jar
        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        // Prepare job parameters
        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
        jobParameters.put(
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put(
            "transformationClassName", "com.custom.SpannerToSourceDbRetryTransformation");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put(
            "dlqMaxRetryCount", "1000"); // High retry count to keep items in retry/ bucket

        // Create MySQL Source for Datastream
        MySQLSource mySQLSource =
            MySQLSource.builder(
                    jdbcResourceManager.getHost(),
                    jdbcResourceManager.getUsername(),
                    jdbcResourceManager.getPassword(),
                    jdbcResourceManager.getPort())
                .setAllowedTables(
                    Map.of(
                        jdbcResourceManager.getDatabaseName(),
                        List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        // Launch regular pipeline with Pub/Sub enabled
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager, // Passing this enables Pub/Sub consumer
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                mySQLSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerMySQLRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        gcsResourceManager,
        datastreamResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testDataStreamToSpannerRetryDLQ() throws Exception {
    LOG.info("Starting testDataStreamToSpannerRetryDLQ");
    assertThatPipeline(jobInfo).isRunning();

    // 1. Insert parent rows directly into Spanner. This prevents out-of-order Dataflow failures
    //    since Dataflow processes asynchronously and might process child rows before parent rows.
    LOG.info("Inserting parent rows directly into Spanner");
    spannerResourceManager.write(
        List.of(
            com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
                .set("CustomerId")
                .to(2)
                .set("CustomerName")
                .to("Customer 2")
                .set("CreditLimit")
                .to(1500)
                .set("LegacyRegion")
                .to("Silver")
                .build()));

    // 2. Insert all test data into the source MySQL database. This will generate:
    // - 1 severe error (id=999) due to the custom transformation throwing exception
    // in "bad" mode.
    // - 1 severe error (for customer1) due to check constraint violation (CreditLimit is 500,
    // must be > 1000).
    // - 2 retryable errors (for order 101 and order103) due to missing parent customer (FK
    // violation: Customer 3 and 4
    // do not exist).
    insertDataInMySQL();
    LOG.info("Data inserted into MySQL successfully");

    // 3. Wait for DLQ events to appear in severe bucket.
    // We ignore the retry/ bucket because it is continuously consumed by PubSub.
    LOG.info("Waiting for DLQ events to appear in severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "Orders")
                            .setMinRows(1) // id = 102
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "Customers")
                            .setMinRows(1) // id= 2
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                            .setMinRows(1) // id = 1
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();
    LOG.info("DLQ events successfully generated in severe bucket");

    // Verify the Spanner database to ensure failing rows were NOT migrated, while success cases
    // were.
    LOG.info("Verifying Spanner state before retry job runs");
    assertTrue("id=1 should exist in AllDataTypes", rowExistsInSpanner("AllDataTypes", "id", 1));
    assertTrue(
        "id=999 should NOT exist yet in AllDataTypes",
        !rowExistsInSpanner("AllDataTypes", "id", 999));

    assertTrue("id=2 should exist in Customers", rowExistsInSpanner("Customers", "CustomerId", 2));
    assertTrue(
        "id=1 should NOT exist yet in Customers",
        !rowExistsInSpanner("Customers", "CustomerId", 1));

    assertTrue("id=102 should exist in Orders", rowExistsInSpanner("Orders", "OrderId", 102));
    assertTrue(
        "id=101 should NOT exist yet in Orders", !rowExistsInSpanner("Orders", "OrderId", 101));
    assertTrue(
        "id=103 should NOT exist yet in Orders", !rowExistsInSpanner("Orders", "OrderId", 103));

    // 4. Launch a new Dataflow job in retryDLQ mode to process the DLQ items.
    // This runs ALONGSIDE the regular mode.
    LOG.info("Launching retryDLQ job with schema overrides to process DLQ");
    Map<String, String> retryJobParameters = new HashMap<>();
    retryJobParameters.put("runMode", "retryDLQ");
    retryJobParameters.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryJobParameters.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryJobParameters.put(
        "transformationClassName", "com.custom.SpannerToSourceDbRetryTransformation");
    retryJobParameters.put(
        "transformationCustomParameters", "mode=good"); // Fixes the transformer error
    retryJobParameters.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            null,
            null,
            GCS_PATH_PREFIX + "-retry",
            spannerResourceManager,
            null, // no pubsub subscription needed for retry job
            retryJobParameters,
            null,
            null,
            gcsResourceManager,
            null, // no datastream needed for retry job
            null,
            null // no mysql source needed for retry job
            );
    LOG.info("RetryDLQ job launched: {}", retryJobInfo.jobId());

    assertThatPipeline(retryJobInfo).isRunning();

    // 5. Apply partial fixes to simulate user intervention correcting data before DLQ retry.
    // Insert parent for Orders to fix the foreign key violation.
    LOG.info("Applying partial fixes in Spanner (inserting missing parent row for Orders)");
    spannerResourceManager.write(
        List.of(
            com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
                .set("CustomerId")
                .to(3)
                .set("CustomerName")
                .to("Parent Customer")
                .set("CreditLimit")
                .to(2000)
                .set("LegacyRegion")
                .to("Gold")
                .build()));

    // 6. Wait for the retryDLQ batch job to complete automatically
    LOG.info("Waiting for the retryDLQ job to complete automatically");
    PipelineOperator.Result retryJobResult =
        pipelineOperator().waitUntilDone(createConfig(retryJobInfo, Duration.ofMinutes(15)));
    assertThatResult(retryJobResult).isLaunchFinished();

    LOG.info("Verifying that severe bucket has exactly 1 entry after retryDLQ job completes");
    // 7. Verify that DLQ buckets after retryDLQ job completes.
    // The buckets should now have exactly 1 entry each (for Order 103 in retry and Customer 1 in
    // severe).
    // The other entries were fixed:
    // - Orders 101 FK violation fixed by inserting missing parent row.
    // - AllDataTypes 999 severe error fixed by updating custom transformation to mode="good".
    // Remaining rows:
    // - Customers 1 remains in severe because the check constraint violation was not fixed.
    // - Orders 103 remains in retry because it has missing parent row.
    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .get());

    // Wait for fixed rows to appear in Spanner (Orders 101 via regular pipeline, AllDataTypes 999
    // via retry job)
    LOG.info("Waiting for fixed rows to appear in Spanner");
    PipelineOperator.Result finalWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, "Orders")
                    .setMinRows(2) // id = 102 and 101
                    .build()
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                            .setMinRows(2) // id = 1 and 999
                            .build()));
    assertThatResult(finalWaitResult).meetsConditions();

    // 8. Verify target Spanner database has the correct updated state for error rows
    // We won't assert on rows that were migrated by the regular pipeline as those were already
    // validated
    LOG.info("Verifying target Spanner database contents");

    // AllDataTypes:
    // id=999 should exist (fixed by mode=good)
    LOG.info("Verifying all datatypes for id=999");
    List<com.google.cloud.spanner.Struct> rows =
        spannerResourceManager.runQuery("SELECT * FROM AllDataTypes WHERE id = 999");

    Map<String, Object> expectedRow = createExpectedRowFor999();

    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(expectedRow));

    // Customers:
    // id=3 should exist (inserted as a partial fix)
    // id=1 should NOT exist (check constraint violation wasn't fixed)
    assertTrue("id=3 should exist in Customers", rowExistsInSpanner("Customers", "CustomerId", 3));
    assertTrue(
        "id=1 should NOT exist in Customers", !rowExistsInSpanner("Customers", "CustomerId", 1));

    // Orders:
    // id=101 should exist (FK issue fixed by inserting parent Customer 3)
    // id=103 should NOT exist (Customer 4 doesn't exist)
    assertTrue("id=101 should exist in Orders", rowExistsInSpanner("Orders", "OrderId", 101));
    assertTrue("id=103 should NOT exist in Orders", !rowExistsInSpanner("Orders", "OrderId", 103));

    // Cancel the regular streaming job as the final step.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());
  }

  private boolean rowExistsInSpanner(String tableName, String idColumnName, long id) {
    List<com.google.cloud.spanner.Struct> rows =
        spannerResourceManager.readTableRecords(tableName, List.of(idColumnName));
    for (com.google.cloud.spanner.Struct row : rows) {
      if (row.getLong(idColumnName) == id) {
        return true;
      }
    }
    return false;
  }

  private void insertDataInMySQL() {
    // Insert into Customers
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LoyaltyTier) VALUES (1, 'Customer 1', 500, 'Bronze')"); // Fails check constraint on Spanner (CreditLimit <= 1000)

    // Insert into Orders
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (3, 101, 1000, 'Website')"); // Fails FK on Spanner initially, later fixed by seeding Customer 3
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (2, 102, 1000, 'AppStore')"); // Succeeds (Customer 2 seeded in Spanner)
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (4, 103, 1000, 'AppStore')"); // Fails FK on Spanner (Customer 4 doesn't exist)

    // Insert into AllDataTypes
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (1, 'test1')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col, tinyint_col, tinyint_unsigned_col, text_col, date_col, "
            + "smallint_col, smallint_unsigned_col, mediumint_col, mediumint_unsigned_col, "
            + "bigint_col, bigint_unsigned_col, float_col, double_col, decimal_col, "
            + "datetime_col, time_col, year_col, char_col, tinyblob_col, tinytext_col, "
            + "blob_col, mediumblob_col, mediumtext_col, test_json_col, longblob_col, "
            + "longtext_col, enum_col, bool_col, binary_col, varbinary_col, bit_col, "
            + "bit8_col, bit1_col, boolean_col, int_col, integer_unsigned_col, timestamp_col, set_col) "
            + "VALUES (999, 'test999', 1, 1, 'text999', '2023-01-01', 11, 11, 101, 101, 1001, 1001, 2.5, 3.5, 11.5, "
            + "'2023-01-01 12:00:00', '12:00:01', 2024, 'c1', 'blob1', 'tinytext1', 'blob1', 'mediumblob1', 'mediumtext1', "
            + "'{\"k\": \"v1\"}', 'longblob1', 'longtext1', '1', 1, x'7835383030000000000000000000000000000000', 'varbin1', "
            + "b'0111111111111111111111111111111111111111111111111111111111111111', b'11111111', 1, 1, 1001, 1001, "
            + "'2023-01-01 12:00:00', 'v1')"); // Fails in 'bad' mode (custom transformation error)
  }

  private Map<String, Object> createExpectedRowFor999() {
    Map<String, Object> row = new HashMap<>();
    row.put("id", 999L);
    row.put("varchar_col", "test999");
    row.put("tinyint_col", 1L);
    row.put("tinyint_unsigned_col", 1L);
    row.put("text_col", "text999");
    row.put("date_col", "2023-01-01");
    row.put("smallint_col", 11L);
    row.put("smallint_unsigned_col", 11L);
    row.put("mediumint_col", 101L);
    row.put("mediumint_unsigned_col", 101L);
    row.put("bigint_col", 1001L);
    row.put("bigint_unsigned_col", new BigDecimal("1001"));
    row.put("float_col", 2.5d);
    row.put("double_col", 3.5d);
    row.put("decimal_col", new BigDecimal("11.5"));
    row.put("datetime_col", "2023-01-01T12:00:00Z");
    row.put("time_col", "12:00:01");
    row.put("year_col", "2024");
    row.put("char_col", "c1");

    String b64blob = Base64.getEncoder().encodeToString("blob1".getBytes());
    row.put("tinyblob_col", b64blob);
    row.put("blob_col", b64blob);
    row.put("mediumblob_col", Base64.getEncoder().encodeToString("mediumblob1".getBytes()));
    row.put("tinytext_col", "tinytext1");
    row.put("mediumtext_col", "mediumtext1");
    row.put("test_json_col", "{\"k\":\"v1\"}");
    row.put("longblob_col", Base64.getEncoder().encodeToString("longblob1".getBytes()));
    row.put("longtext_col", "longtext1");
    row.put("enum_col", "1");
    row.put("bool_col", true);

    byte[] binaryBytes = hexStringToByteArray("7835383030000000000000000000000000000000");
    byte[] paddedBytes = new byte[255];
    System.arraycopy(binaryBytes, 0, paddedBytes, 0, binaryBytes.length);
    row.put("binary_col", Base64.getEncoder().encodeToString(paddedBytes));

    row.put("varbinary_col", Base64.getEncoder().encodeToString("varbin1".getBytes()));
    row.put("bit_col", "f/////////8=");
    row.put("bit8_col", "255");
    row.put("bit1_col", true);
    row.put("boolean_col", true);
    row.put("int_col", 1001L);
    row.put("integer_unsigned_col", 1001L);
    row.put("timestamp_col", "2023-01-01T12:00:00Z");
    row.put("set_col", "v1");

    return row;
  }

  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/datastream-to-spanner")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
