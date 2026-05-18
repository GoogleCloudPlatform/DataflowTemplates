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

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
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
 * Integration test for live replication from MySQL to Spanner in a sharded environment using the
 * retryDLQ mode.
 *
 * <p>Objective: Verify that the retryDLQ batch job correctly processes and retries severe Dead
 * Letter Queue (DLQ) events alongside an actively running streaming pipeline (that processes retry/
 * errors). Validates that rows dynamically route to the correct shards via migration_shard_id
 * native extraction configured via the session file.
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
 *   <li>Utilizing the schema session file to reconcile schema differences. Session file was used
 *       due to presence of ShardIdColumn
 *   <li>Utilizing the DLQ Pub/Sub consumer.
 *   <li>Spanner schema has a dedicated ShardIdColumn migration_shard_id
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerShardedMySQLRetryDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerShardedMySQLRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryDLQIT/mysql-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "DataStreamToSpannerShardedMySQLRetryDLQIT/session.json";
  private static final String GCS_PATH_PREFIX = "mysql-datastream-to-spanner-sharded-retrydlq";

  private static final HashSet<DataStreamToSpannerShardedMySQLRetryDLQIT> testInstances =
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
    synchronized (DataStreamToSpannerShardedMySQLRetryDLQIT.class) {
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

        pubsubResourceManager = setUpPubSubResourceManager();

        // Upload session file
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());

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

        SourceConfig sourceConfigA =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile-shardA", mySQLSourceA);
        DestinationConfig destinationConfigA =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile-shardA",
                gcsResourceManager.getBucket(),
                gcsPrefix,
                DestinationOutputFormat.AVRO_FILE_FORMAT);
        Stream streamA =
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

        SourceConfig sourceConfigB =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile-shardB", mySQLSourceB);
        DestinationConfig destinationConfigB =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile-shardB",
                gcsResourceManager.getBucket(),
                gcsPrefix,
                DestinationOutputFormat.AVRO_FILE_FORMAT);
        Stream streamB =
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
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put(
            "dlqMaxRetryCount", "1000"); // High retry count to keep items in retry/ bucket
        jobParameters.put(
            "shardingContextFilePath",
            getGcsPath("input/shardingContext.json", gcsResourceManager));
        jobParameters.put(
            "inputFilePattern", getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager));
        jobParameters.put(
            "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));

        // Launch regular pipeline with Pub/Sub enabled
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE, // Pass session file
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
                null // Pass null for jdbcSource as we created streams manually
                );
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerShardedMySQLRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
        gcsResourceManager,
        datastreamResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testDataStreamToSpannerShardedRetryDLQ() throws Exception {
    LOG.info("Starting testDataStreamToSpannerShardedRetryDLQ");
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
                .set("migration_shard_id")
                .to("shard1") // Belongs to Shard A
                .build()));

    // 2. Insert all test data into the source MySQL databases. This will generate:
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
    // We ignore the retry bucket because it is continuously consumed by PubSub.
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
    assertTrue(
        "id=1 should exist in AllDataTypes on shard2",
        rowExistsInSpanner("AllDataTypes", "id", 1, "shard2"));
    assertTrue(
        "id=999 should NOT exist yet in AllDataTypes on shard2",
        !rowExistsInSpanner("AllDataTypes", "id", 999, "shard2"));

    assertTrue(
        "id=2 should exist in Customers on shard1",
        rowExistsInSpanner("Customers", "CustomerId", 2, "shard1"));
    assertTrue(
        "id=1 should NOT exist yet in Customers on shard1",
        !rowExistsInSpanner("Customers", "CustomerId", 1, "shard1"));

    assertTrue(
        "id=102 should exist in Orders on shard1",
        rowExistsInSpanner("Orders", "OrderId", 102, "shard1"));
    assertTrue(
        "id=101 should NOT exist yet in Orders on shard1",
        !rowExistsInSpanner("Orders", "OrderId", 101, "shard1"));
    assertTrue(
        "id=103 should NOT exist yet in Orders on shard1",
        !rowExistsInSpanner("Orders", "OrderId", 103, "shard1"));

    // 4. Launch a new Dataflow job in retryDLQ mode to process the DLQ items.
    // This runs ALONGSIDE the regular mode.
    LOG.info("Launching retryDLQ job with session file to process DLQ");
    Map<String, String> retryJobParameters = new HashMap<>();
    retryJobParameters.put("runMode", "retryDLQ");
    retryJobParameters.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryJobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
    retryJobParameters.put(
        "transformationCustomParameters", "mode=good"); // Fixes the transformer error
    retryJobParameters.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
    retryJobParameters.put(
        "shardingContextFilePath", getGcsPath("input/shardingContext.json", gcsResourceManager));

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            SESSION_FILE_RESOURCE, // Pass session file
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
                .set("migration_shard_id")
                .to("shard1") // Belongs to Shard A
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
    assertTrue(
        "id=999 should exist in AllDataTypes on shard2",
        rowExistsInSpanner("AllDataTypes", "id", 999, "shard2"));

    // Customers:
    // id=3 should exist (inserted as a partial fix)
    // id=1 should NOT exist (check constraint violation wasn't fixed)
    assertTrue(
        "id=3 should exist in Customers on shard1",
        rowExistsInSpanner("Customers", "CustomerId", 3, "shard1"));
    assertTrue(
        "id=1 should NOT exist in Customers on shard1",
        !rowExistsInSpanner("Customers", "CustomerId", 1, "shard1"));

    // Orders:
    // id=101 should exist (FK issue fixed by inserting parent Customer 3)
    // id=103 should NOT exist (Customer 4 doesn't exist)
    assertTrue(
        "id=101 should exist in Orders on shard1",
        rowExistsInSpanner("Orders", "OrderId", 101, "shard1"));
    assertTrue(
        "id=103 should NOT exist in Orders on shard1",
        !rowExistsInSpanner("Orders", "OrderId", 103, "shard1"));

    // Cancel the regular streaming job as the final step.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());
  }

  private boolean rowExistsInSpanner(
      String tableName, String idColumnName, long id, String shardId) {
    List<com.google.cloud.spanner.Struct> rows =
        spannerResourceManager.readTableRecords(
            tableName, List.of(idColumnName, "migration_shard_id"));
    for (com.google.cloud.spanner.Struct row : rows) {
      if (row.getLong(idColumnName) == id && row.getString("migration_shard_id").equals(shardId)) {
        return true;
      }
    }
    return false;
  }

  private void insertDataInMySQL() {
    // Insert data in MySQL Shard A
    LOG.info("Inserting data in MySQL Shard A");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LoyaltyTier) VALUES (1, 'Customer 1', 500, 'Bronze')"); // Fails check constraint on Spanner (CreditLimit <= 1000)
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (3, 101, 1000, 'Website')"); // Fails FK on Spanner initially, later fixed by seeding Customer 3
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (2, 102, 1000, 'AppStore')"); // Succeeds (Customer 2 seeded in Spanner)
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (4, 103, 1000, 'AppStore')"); // Fails FK on Spanner (Customer 4 doesn't exist)

    // Insert data in MySQL Shard B
    LOG.info("Inserting data in MySQL Shard B");
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (1, 'test1')");
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (999, 'test999')"); // Fails in 'bad'
    // mode (custom
    // transformation
    // error)
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
