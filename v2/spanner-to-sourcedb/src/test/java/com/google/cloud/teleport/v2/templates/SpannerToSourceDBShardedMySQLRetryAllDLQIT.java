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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
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
 * Integration test for reverse replication from Spanner to MySQL using the retryAllDLQ mode.
 *
 * <p>Objective: Verify that the retryAllDLQ batch job correctly processes and retries ALL Dead
 * Letter Queue (DLQ) events when the main pipeline is stopped inside a sharded topology using
 * custom shard logic.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBShardedMySQLRetryAllDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBShardedMySQLRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryAllDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryAllDLQIT/mysql-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "SpannerToSourceDBShardedMySQLRetryAllDLQIT/overrides.json";
  private static final HashSet<SpannerToSourceDBShardedMySQLRetryAllDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MySQLResourceManager jdbcResourceManagerShardA;
  public static MySQLResourceManager jdbcResourceManagerShardB;
  public static GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBShardedMySQLRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDBShardedMySQLRetryAllDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = MySQLResourceManager.builder(testName + "shardA").build();
        createMySQLSchema(
            jdbcResourceManagerShardA,
            SpannerToSourceDBShardedMySQLRetryAllDLQIT.MYSQL_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = MySQLResourceManager.builder(testName + "shardB").build();
        createMySQLSchema(
            jdbcResourceManagerShardB,
            SpannerToSourceDBShardedMySQLRetryAllDLQIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        // Use generic multi-shard logic instead of base IT helper
        createAndUploadShardConfigToGcsMulti();

        // Upload overrides file
        gcsResourceManager.uploadArtifact(
            "input/overrides.json", Resources.getResource(OVERRIDES_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", // Use relative path!
                    "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters("mode=bad")
                .build();

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put(
                    "schemaOverridesFilePath",
                    getGcsPath("input/overrides.json", gcsResourceManager));
                put("dlqMaxRetryCount", "20");
                put("dlqRetryMinutes", "60");
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                null, // Passing null disables Pub/Sub consumer, leaving retry DLQ items statically
                getClass().getSimpleName(),
                "input/customShard.jar",
                "com.custom.CustomShardIdFetcherForRetryIT",
                null,
                customTransformation,
                MYSQL_SOURCE_TYPE,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBShardedMySQLRetryAllDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        jdbcResourceManagerShardB,
        spannerMetadataResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testSpannerToSrcDBRetryAllDLQ() throws Exception {
    LOG.info("Starting testSpannerToSrcDBRetryAllDLQ for sharded execution");
    assertThatPipeline(jobInfo).isRunning();

    // 1. Insert parent rows directly into MySQL.
    LOG.info("Inserting parent rows directly into MySQL");
    // customer2 routes to ShardB (2%2==0)
    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (2, 'Customer 2', 1500, 'Silver')");

    // 2. Insert test data into the source Spanner database. This will generate:
    // - 2 severe errors (for id=999 and id=888) due to the custom transformation throwing exception
    // in "bad" mode.
    // - 1 retryable error (for order101) due to missing parent customer (FK violation: Customer 3
    // does not exist).
    // - 1 retryable error (for customer1) due to check constraint violation (CreditLimit is 500,
    // must be > 1000).
    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

    // 3. Wait for DLQ events to appear in corresponding buckets.
    // Total events expected:
    // - Customers: 1
    // - Orders: 1
    // - AllDataTypes: 2
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                            .setMinEvents(2)
                            .build())
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
    LOG.info("DLQ events appeared in corresponding buckets");

    // 4. Stop the regular pipeline.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(15)));
    LOG.info("Regular pipeline stopped successfully");

    // 5. Apply partial fixes to simulate user intervention correcting data before DLQ retry.
    LOG.info("Applying partial fixes in MySQL (inserting missing parent row for Orders)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (3, 'Parent Customer A', 2000, 'Gold')");

    // 6. Launch a new Dataflow job in retryAllDLQ mode.
    LOG.info("Launching retryAllDLQ job with schema overrides to process DLQ");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryAllDLQ");
    retryParams.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryParams.put("dlqMaxRetryCount", "20");
    retryParams.put("dlqRetryMinutes", "60");

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            null,
            getClass().getSimpleName(),
            "input/customShard.jar",
            "com.custom.CustomShardIdFetcherForRetryIT",
            null,
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters("mode=semi-fixed")
                .build(),
            MYSQL_SOURCE_TYPE,
            retryParams);
    LOG.info("RetryAllDLQ job launched: {}", retryJobInfo.jobId());

    assertThatPipeline(retryJobInfo).isRunning();

    // 7. Wait for the retry job to process events and ensure they reach the DLQ correctly BEFORE
    // cancelling.
    // The buckets should now have exactly 1 entry each (for Customers 1 in retry and id=888 in
    // severe).
    // The other entries were fixed:
    // - Orders 101 FK violation fixed by inserting missing parent row on Shard A.
    // - AllDataTypes 999 severe error fixed by updating custom transformation to mode="semi-fixed".
    // Remaining rows:
    // - Customers 1 remains in retry because the check constraint violation was not fixed.
    // - AllDataTypes 888 remains in severe because mode='semi-fixed' only fixes row 999, not 888.
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets after retry");
    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(1)
                    .setMaxEvents(1)
                    .build());

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(15)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();
    LOG.info("Retry job completed processing successfully");

    // 8. Verify target MySQL database has the correct updated state.
    LOG.info("Verifying MySQL data across logical shards");

    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardA, "AllDataTypes")
            .setMinRows(2)
            .setMaxRows(2)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "AllDataTypes")
            .setMinRows(0)
            .setMaxRows(0)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "Customers")
            .setMinRows(1)
            .setMaxRows(1)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardA, "Customers")
            .setMinRows(1)
            .setMaxRows(1)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardA, "Orders")
            .setMinRows(1)
            .setMaxRows(1)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "Orders")
            .setMinRows(1)
            .setMaxRows(1)
            .build()
            .get());

    List<Map<String, Object>> shardAAllTypes =
        jdbcResourceManagerShardA.runSQLQuery("SELECT id FROM AllDataTypes");
    List<Integer> shardAAllTypesIds =
        shardAAllTypes.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();

    List<Map<String, Object>> shardBAllTypes =
        jdbcResourceManagerShardB.runSQLQuery("SELECT id FROM AllDataTypes");
    List<Integer> shardBAllTypesIds =
        shardBAllTypes.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();

    // 1(mod2!=0) -> ShardA
    assertTrue("id=1 should exist on Shard A", shardAAllTypesIds.contains(1));
    // 999(mod2!=0) -> ShardA
    assertTrue("id=999 should exist on Shard A", shardAAllTypesIds.contains(999));
    // 888(mod2==0) -> ShardB (Fails repeatedly, so shouldn't exist anywhere)
    assertTrue("id=888 should NOT exist on Shard B", !shardBAllTypesIds.contains(888));

    List<Map<String, Object>> shardACust =
        jdbcResourceManagerShardA.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardACustIds =
        shardACust.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();

    List<Map<String, Object>> shardBCust =
        jdbcResourceManagerShardB.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardBCustIds =
        shardBCust.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();

    // 1(mod2!=0) -> ShardA (failed check constraint: CreditLimit was 500 but must be > 1000)
    assertTrue("id=1 should NOT exist on Shard A", !shardACustIds.contains(1));
    // 3(mod2!=0) -> ShardA
    assertTrue("id=3 should exist on Shard A", shardACustIds.contains(3));
    // 2(mod2==0) -> ShardB
    assertTrue("id=2 should exist on Shard B", shardBCustIds.contains(2));

    List<Map<String, Object>> shardAOrders =
        jdbcResourceManagerShardA.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardAOrderIds =
        shardAOrders.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();

    List<Map<String, Object>> shardBOrders =
        jdbcResourceManagerShardB.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardBOrderIds =
        shardBOrders.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();

    // order101 (Cust 3, mod2!=0) -> ShardA
    assertTrue("id=101 should exist on Shard A", shardAOrderIds.contains(101));
    // order102 (Cust 2, mod2==0) -> ShardB
    assertTrue("id=102 should exist on Shard B", shardBOrderIds.contains(102));
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
    jsObjA.remove("secretManagerUri"); // remove field secretManagerUri

    Shard shardB = new Shard();
    shardB.setLogicalShardId("testShardB");
    shardB.setUser(jdbcResourceManagerShardB.getUsername());
    shardB.setHost(jdbcResourceManagerShardB.getHost());
    shardB.setPassword(jdbcResourceManagerShardB.getPassword());
    shardB.setPort(String.valueOf(jdbcResourceManagerShardB.getPort()));
    shardB.setDbName(jdbcResourceManagerShardB.getDatabaseName());
    JsonObject jsObjB = (JsonObject) new Gson().toJsonTree(shardB).getAsJsonObject();
    jsObjB.remove("secretManagerUri"); // remove field secretManagerUri

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
            .build();
    com.google.cloud.spanner.Mutation allTypes999 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(999) // Bad transformer fails on purpose, good transformer doesnt
            .set("boolean_col")
            .to(false)
            .set("varchar_col")
            .to("test999")
            .set("bit8_col")
            .to(22)
            .set("bit1_col")
            .to(false)
            .build();
    com.google.cloud.spanner.Mutation allTypes888 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(888) // Bad and good transformer fail on purpose
            .set("boolean_col")
            .to(true)
            .set("varchar_col")
            .to("test888")
            .set("bit8_col")
            .to(33)
            .set("bit1_col")
            .to(true)
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
