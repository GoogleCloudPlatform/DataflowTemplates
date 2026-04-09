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
 * Integration test for live replication from MySQL to Spanner using the retryDLQ mode.
 *
 * <p>Objective: Verify that the retryDLQ batch job correctly processes and retries severe Dead
 * Letter Queue (DLQ) events alongside an actively running streaming pipeline (that processes retry/
 * errors).
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

    // 1. Insert parent rows directly into Spanner.
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

    // 5. Apply partial fixes in Spanner (inserting missing parent row for Orders to fix FK).
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

    // 8. Verify target Spanner database contents.
    LOG.info("Verifying target Spanner database contents");

    // AllDataTypes:
    // id=1 should exist
    // id=999 should exist (fixed by mode=good)
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
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LoyaltyTier) VALUES (1, 'Customer 1', 500, 'Bronze')"); // Fails check constraint on Spanner

    // Insert into Orders
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (3, 101, 1000, 'Website')"); // Fails FK on Spanner initially, fixed by seeding Customer 3
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (2, 102, 1000, 'AppStore')"); // Succeeds (Customer 2 seeded in Spanner)
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (4, 103, 1000, 'AppStore')"); // Fails FK on Spanner (Customer 4 doesn't exist)

    // Insert into AllDataTypes
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (1, 'test1')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (999, 'test999')"); // Fails in 'bad'
    // mode
  }

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/datastream-to-spanner")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
