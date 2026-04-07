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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for reverse replication from Spanner to MySQL using the retryDLQ mode for
 * sharded clusters.
 *
 * <p>Objective: Verify that the retryDLQ batch job correctly processes and retries severe Dead
 * Letter Queue (DLQ) events alongside an actively running streaming pipeline (that processes retry/
 * errors). Validates that rows dynamically route to the correct shards via migration_shard_id
 * native extraction configured via the session file.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBShardedMySQLRetryDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBShardedMySQLRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryDLQIT/mysql-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryDLQIT/session.json";

  private static final HashSet<SpannerToSourceDBShardedMySQLRetryDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MySQLResourceManager jdbcResourceManagerShardA;
  public static MySQLResourceManager jdbcResourceManagerShardB;
  public static GcsResourceManager gcsResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBShardedMySQLRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDBShardedMySQLRetryDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = MySQLResourceManager.builder(testName + "shardA").build();
        createMySQLSchema(
            jdbcResourceManagerShardA,
            SpannerToSourceDBShardedMySQLRetryDLQIT.MYSQL_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = MySQLResourceManager.builder(testName + "shardB").build();
        createMySQLSchema(
            jdbcResourceManagerShardB,
            SpannerToSourceDBShardedMySQLRetryDLQIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcsMulti();

        // Upload session file
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters("mode=bad")
                .build();

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        pubsubResourceManager = setUpPubSubResourceManager();
        SubscriptionName subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);

        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
                // keeping retryCount high so it retries continuously and stays in the retry/ bucket
                // till end of this test
                put("dlqMaxRetryCount", "1000");
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                getClass().getSimpleName(),
                null, // no need to pass custom shard id fetcher class as spanner has ShardIdColumn
                null,
                null,
                customTransformation,
                MYSQL_SOURCE_TYPE,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBShardedMySQLRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSrcDBRetryDLQ() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    // Insert parent rows directly into MySQL to prevent out-of-order Dataflow failures.
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (2, 'Customer 2', 1500, 'Silver')");

    // Insert test data into Spanner. This will generate:
    // - 2 severe errors (for id=999 and id=888) due to the custom transformation throwing exception
    // in "bad" mode.
    // - 1 retryable error (for order101) due to missing parent customer (FK violation: Customer 3
    // does not exist).
    // - 1 retryable error (for customer1) due to check constraint violation (CreditLimit is 500,
    // must be > 1000).
    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

    // Wait for DLQ events to appear in the severe bucket. The retry bucket should not be
    // asserted on because it is continuously deleted and re-written by the PubSub subscriptions.
    LOG.info("Waiting for DLQ events to appear in severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "Orders")
                            .setMinRows(1) // id = 102
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManagerShardA, "AllDataTypes")
                            .setMinRows(1) // id = 1
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "Customers")
                            .setMinRows(1) // id = 2
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();

    // Verify the MySQL database to ensure failing rows were NOT migrated, while success cases were.
    LOG.info("Verifying MySQL state before retry job runs");
    List<Map<String, Object>> shardACustomersRows =
        jdbcResourceManagerShardA.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardACustomersIds =
        shardACustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue(
        "id=1 should NOT exist yet on Shard A", !shardACustomersIds.contains(1)); // the failing one

    List<Map<String, Object>> shardAOrdersRows =
        jdbcResourceManagerShardA.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardAOrdersIds =
        shardAOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue(
        "id=101 should NOT exist yet on Shard A",
        !shardAOrdersIds.contains(101)); // the failing one

    List<Map<String, Object>> shardBOrdersRows =
        jdbcResourceManagerShardB.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardBOrdersIds =
        shardBOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=102 should exist on Shard B", shardBOrdersIds.contains(102));

    List<Map<String, Object>> shardAAllDataTypesRows =
        jdbcResourceManagerShardA.runSQLQuery("SELECT id FROM AllDataTypes");
    List<Integer> shardAAllDataTypesIds =
        shardAAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist on Shard A", shardAAllDataTypesIds.contains(1));

    // Launch a new Dataflow batch job in retryDLQ mode to process the DLQ items.
    // This runs ALONGSIDE the regular mode. The regular pipeline should NOT be cancelled before
    // running this.
    LOG.info("Launching retryDLQ job with session file to process DLQ");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryDLQ");
    retryParams.put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            null,
            getClass().getSimpleName(),
            null,
            null,
            null,
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters("mode=semi-fixed") // Fixes one of the simulated severe errors
                .build(),
            MYSQL_SOURCE_TYPE,
            retryParams);

    assertThatPipeline(retryJobInfo).isRunning();

    LOG.info("Applying partial fixes in MySQL (inserting missing parent row for Orders)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (3, 'Parent Customer A', 2000, 'Gold')");

    // Wait for the retryDLQ batch job to complete automatically
    LOG.info("Waiting for the retryDLQ job to complete automatically");
    PipelineOperator.Result retryJobResult =
        pipelineOperator().waitUntilDone(createConfig(retryJobInfo, Duration.ofMinutes(15)));
    assertThatResult(retryJobResult).isLaunchFinished();

    LOG.info("Verifying that severe bucket has exactly 1 entry after retryDLQ job completes");
    // The severe bucket should now have exactly 1 entry (for id=888).
    // The other entry (id=999) was fixed because the retry job was launched with
    // custom transformation mode="semi-fixed", which allows id=999 to pass but still fails id=888.
    // The retry bucket will also have 1 entry (Orders 101 FK violation was fixed by inserting
    // parent row)
    // But we don't assert that here, as DLQPubSubConsumer repeatedly deletes and rewrites the
    // retry/ bucket which would result in flakiness
    // Remaining rows:
    // - Customers 1 remains in retry because the check constraint violation was not fixed.
    // - AllDataTypes 888 remains in severe because mode='semi-fixed' only fixes row 999, not 888.

    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .get());

    // Verify target MySQL database for both the fixed rows and for the absence of non-fixed errors
    LOG.info("Verifying final target MySQL database contents across shards");

    shardACustomersRows = jdbcResourceManagerShardA.runSQLQuery("SELECT CustomerId FROM Customers");
    shardACustomersIds =
        shardACustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    // id=1 should NOT exist on Shard A (check constraint violation wasn't fixed: CreditLimit was
    // 500 but must be > 1000)
    assertTrue("id=1 should NOT exist on Shard A", !shardACustomersIds.contains(1));
    assertTrue("id=3 should exist on Shard A", shardACustomersIds.contains(3));

    List<Map<String, Object>> shardBCustomersRows =
        jdbcResourceManagerShardB.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardBCustomersIds =
        shardBCustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=2 should exist on Shard B", shardBCustomersIds.contains(2));

    shardAOrdersRows = jdbcResourceManagerShardA.runSQLQuery("SELECT OrderId FROM Orders");
    shardAOrdersIds =
        shardAOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should exist on Shard A", shardAOrdersIds.contains(101));

    shardBOrdersRows = jdbcResourceManagerShardB.runSQLQuery("SELECT OrderId FROM Orders");
    shardBOrdersIds =
        shardBOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=102 should exist on Shard B", shardBOrdersIds.contains(102));

    shardAAllDataTypesRows =
        jdbcResourceManagerShardA.runSQLQuery("SELECT id, varchar_col FROM AllDataTypes");
    shardAAllDataTypesIds =
        shardAAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist on Shard A", shardAAllDataTypesIds.contains(1));
    assertTrue("id=999 should exist on Shard A", shardAAllDataTypesIds.contains(999));

    List<Map<String, Object>> shardBAllDataTypesRows =
        jdbcResourceManagerShardB.runSQLQuery("SELECT id FROM AllDataTypes");
    List<Integer> shardBAllDataTypesIds =
        shardBAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=888 should NOT exist on Shard B", !shardBAllDataTypesIds.contains(888));

    // Cancel the regular streaming job as the final step.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());
  }

  private Integer getIntValueCaseInsensitive(Map<String, Object> map, String key) {
    for (String k : map.keySet()) {
      if (k.equalsIgnoreCase(key)) {
        Object val = map.get(k);
        if (val instanceof Number) {
          return ((Number) val).intValue();
        }
      }
    }
    return null;
  }

  private void createAndUploadShardConfigToGcsMulti() throws IOException {
    Shard shardA = new Shard();
    shardA.setLogicalShardId("testShardA");
    shardA.setUser(jdbcResourceManagerShardA.getUsername());
    shardA.setHost(jdbcResourceManagerShardA.getHost());
    shardA.setPassword(jdbcResourceManagerShardA.getPassword());
    shardA.setPort(String.valueOf(jdbcResourceManagerShardA.getPort()));
    shardA.setDbName(jdbcResourceManagerShardA.getDatabaseName());
    JsonObject jsObjA = (JsonObject) new Gson().toJsonTree(shardA).getAsJsonObject();
    jsObjA.remove("secretManagerUri");

    Shard shardB = new Shard();
    shardB.setLogicalShardId("testShardB");
    shardB.setUser(jdbcResourceManagerShardB.getUsername());
    shardB.setHost(jdbcResourceManagerShardB.getHost());
    shardB.setPassword(jdbcResourceManagerShardB.getPassword());
    shardB.setPort(String.valueOf(jdbcResourceManagerShardB.getPort()));
    shardB.setDbName(jdbcResourceManagerShardB.getDatabaseName());
    JsonObject jsObjB = (JsonObject) new Gson().toJsonTree(shardB).getAsJsonObject();
    jsObjB.remove("secretManagerUri");

    JsonArray ja = new JsonArray();
    ja.add(jsObjA);
    ja.add(jsObjB);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  private void insertDataInSpanner() {
    com.google.cloud.spanner.Mutation customer1 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
            .set("CustomerId")
            .to(1)
            .set("CustomerName")
            .to("Customer 1")
            .set("CreditLimit")
            .to(500) // this will fail due to check constraint at source
            .set("LoyaltyTier")
            .to("Bronze")
            .set("migration_shard_id")
            .to("testShardA")
            .build();
    com.google.cloud.spanner.Mutation order101 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Orders")
            .set("OrderId")
            .to(101)
            .set("CustomerId")
            .to(3) // fails due to no parent row in Customers
            .set("OrderValue")
            .to(1000)
            .set("OrderSource")
            .to("Website")
            .set("migration_shard_id")
            .to("testShardA")
            .build();
    com.google.cloud.spanner.Mutation order102 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Orders")
            .set("OrderId")
            .to(102)
            .set("CustomerId")
            .to(2)
            .set("OrderValue")
            .to(1000)
            .set("OrderSource")
            .to("AppStore")
            .set("migration_shard_id")
            .to("testShardB")
            .build();

    com.google.cloud.spanner.Mutation allTypes1 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(1)
            .set("boolean_col")
            .to(true)
            .set("varchar_col")
            .to("test1")
            .set("bit8_col")
            .to(11)
            .set("bit1_col")
            .to(true)
            .set("migration_shard_id")
            .to("testShardA")
            .build();
    com.google.cloud.spanner.Mutation allTypes999 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(999) // Bad transformer fails on purpose, semi-fixed transformer doesn't
            .set("boolean_col")
            .to(false)
            .set("varchar_col")
            .to("test999")
            .set("bit8_col")
            .to(22)
            .set("bit1_col")
            .to(false)
            .set("migration_shard_id")
            .to("testShardA")
            .build();
    com.google.cloud.spanner.Mutation allTypes888 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(888) // Bad and semi-fixed transformer fail on purpose
            .set("boolean_col")
            .to(true)
            .set("varchar_col")
            .to("test888")
            .set("bit8_col")
            .to(33)
            .set("bit1_col")
            .to(true)
            .set("migration_shard_id")
            .to("testShardB")
            .build();

    spannerResourceManager.write(
        List.of(customer1, order101, order102, allTypes1, allTypes999, allTypes888));
  }

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/spanner-to-sourcedb")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
