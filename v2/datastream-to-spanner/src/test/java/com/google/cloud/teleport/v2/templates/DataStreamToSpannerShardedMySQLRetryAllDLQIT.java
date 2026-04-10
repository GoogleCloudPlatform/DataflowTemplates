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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
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
 * Integration test for live replication from MySQL to Spanner using the retryAllDLQ mode for a
 * sharded setup.
 *
 * <p>Objective: Verify that the retryAllDLQ batch job correctly processes and retries ALL Dead
 * Letter Queue (DLQ) events when the main pipeline is stopped for a sharded setup.
 *
 * <p>Edge cases covered in this test include:
 *
 * <ul>
 *   <li>Handling retriable errors such as foreign key violations via the retryAllDLQ pipeline.
 *   <li>Processing severe errors introduced by custom transformation failures and check constraints
 *       via the retryAllDLQ pipeline.
 *   <li>Retrying fixed items successfully in both retry/ and severe/ buckets: e.g. fixing a foreign
 *       key violation by inserting a missing parent row, and using a corrected transformation file.
 *   <li>Ensuring non-fixable items remain correctly logged under their respective error buckets.
 *   <li>Validating schema complexities between Source and Spanner, including mismatched primary
 *       keys, added, deleted, and renamed columns, as well as all datatypes.
 *   <li>Utilizing the schema overrides file to reconcile schema differences.
 *   <li>Utilizing the static DLQ file-based consumer (instead of the Pub/Sub consumer flow).
 *   <li>Utilizing the Sharding Context File path for a sharded setup without presence of
 *       ShardIdColumn.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerShardedMySQLRetryAllDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerShardedMySQLRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryAllDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryAllDLQIT/mysql-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryAllDLQIT/overrides.json";
  private static final String GCS_PATH_PREFIX = "mysql-datastream-to-spanner-sharded-retryall";

  private static final HashSet<DataStreamToSpannerShardedMySQLRetryAllDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static CloudMySQLResourceManager jdbcResourceManagerShardA;
  public static CloudMySQLResourceManager jdbcResourceManagerShardB;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  private static String streamNameA;
  private static String streamNameB;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerShardedMySQLRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        // Setup MySQL Shards
        jdbcResourceManagerShardA = CloudMySQLResourceManager.builder(testName + "shardA").build();
        executeSqlScript(jdbcResourceManagerShardA, MYSQL_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = CloudMySQLResourceManager.builder(testName + "shardB").build();
        executeSqlScript(jdbcResourceManagerShardB, MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        // Upload overrides file
        gcsResourceManager.uploadArtifact(
            "input/overrides.json", Resources.getResource(OVERRIDES_FILE_RESOURCE).getPath());

        // Upload custom transformation jar
        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        // Create Datastream streams manually
        String gcsPrefix =
            getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), "");

        MySQLSource mySQLSourceA =
            MySQLSource.builder(
                    jdbcResourceManagerShardA.getHost(),
                    jdbcResourceManagerShardA.getUsername(),
                    jdbcResourceManagerShardA.getPassword(),
                    jdbcResourceManagerShardA.getPort())
                .setAllowedTables(
                    Map.of(
                        jdbcResourceManagerShardA.getDatabaseName(),
                        List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        var sourceConfigA =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile-shardA", mySQLSourceA);
        var destinationConfigA =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile-shardA",
                gcsResourceManager.getBucket(),
                gcsPrefix,
                DestinationOutputFormat.AVRO_FILE_FORMAT);
        var streamA =
            datastreamResourceManager.createStream(
                "stream-shardA", sourceConfigA, destinationConfigA);
        datastreamResourceManager.startStream(streamA);
        streamNameA = streamA.getName().substring(streamA.getName().lastIndexOf('/') + 1);

        MySQLSource mySQLSourceB =
            MySQLSource.builder(
                    jdbcResourceManagerShardB.getHost(),
                    jdbcResourceManagerShardB.getUsername(),
                    jdbcResourceManagerShardB.getPassword(),
                    jdbcResourceManagerShardB.getPort())
                .setAllowedTables(
                    Map.of(
                        jdbcResourceManagerShardB.getDatabaseName(),
                        List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        var sourceConfigB =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile-shardB", mySQLSourceB);
        var destinationConfigB =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile-shardB",
                gcsResourceManager.getBucket(),
                gcsPrefix,
                DestinationOutputFormat.AVRO_FILE_FORMAT);
        var streamB =
            datastreamResourceManager.createStream(
                "stream-shardB", sourceConfigB, destinationConfigB);
        datastreamResourceManager.startStream(streamB);
        streamNameB = streamB.getName().substring(streamB.getName().lastIndexOf('/') + 1);

        // Generate Shard Context JSON
        String shardContextJson =
            generateShardContextJson(
                streamNameA,
                jdbcResourceManagerShardA.getDatabaseName(),
                "shard1",
                streamNameB,
                jdbcResourceManagerShardB.getDatabaseName(),
                "shard2");

        gcsResourceManager.createArtifact(
            "input/shardingContext.json", shardContextJson.getBytes(StandardCharsets.UTF_8));

        // Prepare job parameters
        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
        jobParameters.put(
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put(
            "transformationClassName", "com.custom.SpannerToSourceDbRetryTransformation");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put("dlqMaxRetryCount", "20");
        jobParameters.put("dlqRetryMinutes", "60");
        jobParameters.put(
            "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
        jobParameters.put(
            "shardingContextFilePath",
            getGcsPath("input/shardingContext.json", gcsResourceManager));
        jobParameters.put(
            "inputFilePattern", getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager));

        // Launch single regular job
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null, // Pass null for session file as schema doesn't have migration_shard_id
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                null, // Passing null disables Pub/Sub consumer for data and DLQ
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                null // Pass null for jdbcSource as we created streams manually
                );
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerShardedMySQLRetryAllDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
        gcsResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testDataStreamToSpannerShardedRetryAllDLQ() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    // Seed Spanner with Customer 2 (needed for Order 102 on Shard B)
    com.google.cloud.spanner.Mutation customer2 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
            .set("CustomerId")
            .to(2)
            .set("CustomerName")
            .to("Customer 2")
            .set("CreditLimit")
            .to(1500)
            .set("LegacyRegion")
            .to("Silver")
            .build();
    spannerResourceManager.write(customer2);

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

    // Wait for DLQ events in shared directory
    LOG.info("Waiting for DLQ events to appear in shared severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/retry/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        DlqEventsCountCheck.builder(
                                gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                            .setMinEvents(2)
                            .build())
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
    LOG.info("DLQ events successfully generated in corresponding buckets");

    // Verify Spanner state before retry
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

    // Stop the regular pipeline
    LOG.info("Stopping regular pipeline");
    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(5)));

    // Apply partial fixes to simulate user intervention correcting data before DLQ retry.
    LOG.info("Applying partial fixes in Spanner (inserting missing parent row for Orders)");
    com.google.cloud.spanner.Mutation customer3 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
            .set("CustomerId")
            .to(3)
            .set("CustomerName")
            .to("Customer 3")
            .set("CreditLimit")
            .to(2000)
            .set("LegacyRegion")
            .to("Gold")
            .build();
    spannerResourceManager.write(customer3);

    // Launch retryAllDLQ job
    LOG.info("Launching retryAllDLQ job");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryAllDLQ");
    retryParams.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryParams.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryParams.put("transformationClassName", "com.custom.SpannerToSourceDbRetryTransformation");
    retryParams.put("transformationCustomParameters", "mode=good"); // Fixes 999, but not 888
    retryParams.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
    retryParams.put("dlqMaxRetryCount", "20");
    retryParams.put("dlqRetryMinutes", "60");
    retryParams.put(
        "shardingContextFilePath", getGcsPath("input/shardingContext.json", gcsResourceManager));

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            null,
            null,
            GCS_PATH_PREFIX + "-retry",
            spannerResourceManager,
            null, // no pubsub subscription needed for retry job
            retryParams,
            null,
            null,
            gcsResourceManager,
            null, // no datastream needed for retry job
            null,
            null // no mysql source needed for retry job
            );
    LOG.info("RetryAllDLQ job launched: {}", retryJobInfo.jobId());
    assertThatPipeline(retryJobInfo).isRunning();

    // Wait for the retry job to process events and ensure they reach the DLQ correctly BEFORE
    // cancelling
    // The buckets should now have exactly 1 entry each (for Order 103 in retry and Customer 1 in
    // severe).
    // The other entries were fixed:
    // - Orders 101 FK violation fixed by inserting missing parent row.
    // - AllDataTypes 999 severe error fixed by updating custom transformation to mode="good".
    // Remaining rows:
    // - Customers 1 remains in severe because the check constraint violation was not fixed.
    // - Orders 103 remains in retry because it has missing parent row.
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets after retry");
    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/retry/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                    .setMinEvents(1)
                    .setMaxEvents(1)
                    .build());

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(15)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();
    LOG.info("Retry job completed processing successfully");

    // 8. Verify target Spanner database has the correct updated state.
    LOG.info("Verifying target Spanner database contents");

    // AllDataTypes:
    // id=1 should exist
    // id=999 should exist (fixed)

    assertTrue("id=1 should exist in AllDataTypes", rowExistsInSpanner("AllDataTypes", "id", 1));
    assertTrue(
        "id=999 should exist in AllDataTypes", rowExistsInSpanner("AllDataTypes", "id", 999));

    // Customers:
    // id=2 should exist (seeded in Spanner)
    // id=3 should exist (inserted as a partial fix)
    // id=1 should NOT exist (check constraint violation wasn't fixed)
    assertTrue("id=2 should exist in Customers", rowExistsInSpanner("Customers", "CustomerId", 2));
    assertTrue("id=3 should exist in Customers", rowExistsInSpanner("Customers", "CustomerId", 3));
    assertTrue(
        "id=1 should NOT exist in Customers", !rowExistsInSpanner("Customers", "CustomerId", 1));

    // Orders:
    // id=101 should exist (FK issue fixed by inserting parent Customer 3)
    // id=102 should exist (parent Customer 2 was seeded originally)
    // id=103 should NOT exist (Customer 4 doesn't exist)
    assertTrue("id=101 should exist in Orders", rowExistsInSpanner("Orders", "OrderId", 101));
    assertTrue("id=102 should exist in Orders", rowExistsInSpanner("Orders", "OrderId", 102));
    assertTrue("id=103 should NOT exist in Orders", !rowExistsInSpanner("Orders", "OrderId", 103));

    LOG.info("Verified target Spanner database contents successfully");
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

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/datastream-to-spanner")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }

  private void executeSqlScript(CloudMySQLResourceManager resourceManager, String resourceName)
      throws IOException {
    String sql = Resources.toString(Resources.getResource(resourceName), StandardCharsets.UTF_8);
    for (String statement : sql.split(";")) {
      if (!statement.trim().isEmpty()) {
        resourceManager.runSQLUpdate(statement);
      }
    }
  }

  private void insertDataInMySQL() {

    // Insert data in MySQL Shard A (Mapped to shard1 via context, but not stored in Spanner)
    LOG.info("Inserting data in MySQL Shard A");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LoyaltyTier) VALUES (1, 'Customer 1', 500, 'Bronze')"); // Fails check constraint
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (3, 101, 1000, 'Website')"); // Fails FK on Spanner initially, fixed by seeding Customer 3
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (2, 102, 1000, 'AppStore')"); // Succeeds (Customer 2 seeded in Spanner)
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (4, 103, 1000, 'AppStore')"); // Fails FK on Spanner (Customer 4 doesn't exist)

    // Insert data in MySQL Shard B (Mapped to shard2)
    LOG.info("Inserting data in MySQL Shard B");
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (1, 'test1')");
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (999, 'test999')"); // Fails in 'bad'
    // mode
  }

  private String generateShardContextJson(
      String streamA, String dbA, String shardA, String streamB, String dbB, String shardB) {
    return "{\n"
        + "  \"StreamToDbAndShardMap\": {\n"
        + "    \""
        + streamA
        + "\": {\""
        + dbA
        + "\": \""
        + shardA
        + "\"},\n"
        + "    \""
        + streamB
        + "\": {\""
        + dbB
        + "\": \""
        + shardB
        + "\"}\n"
        + "  }\n"
        + "}";
  }
}
