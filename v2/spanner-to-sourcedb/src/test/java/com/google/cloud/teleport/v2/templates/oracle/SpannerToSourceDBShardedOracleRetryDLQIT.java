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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
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
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBShardedOracleRetryDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBShardedOracleRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToSourceDBShardedOracleRetryDLQIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToSourceDBShardedOracleRetryDLQIT/oracle-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/SpannerToSourceDBShardedOracleRetryDLQIT/session.json";

  private static final HashSet<SpannerToSourceDBShardedOracleRetryDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager jdbcResourceManagerShardA;
  public static OracleResourceManager jdbcResourceManagerShardB;
  public static GcsResourceManager gcsResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBShardedOracleRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDBShardedOracleRetryDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = SharedOracleReverseITContainer.getInstance();
        testUsernameShardA = setupOracleIsolatedUser(jdbcResourceManagerShardA);
        createOracleSchema(
            jdbcResourceManagerShardA,
            SpannerToSourceDBShardedOracleRetryDLQIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsernameShardA);

        jdbcResourceManagerShardB = SharedOracleReverseITContainer.getInstance();
        testUsernameShardB = setupOracleIsolatedUser(jdbcResourceManagerShardB);
        createOracleSchema(
            jdbcResourceManagerShardB,
            SpannerToSourceDBShardedOracleRetryDLQIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsernameShardB);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(
            gcsResourceManager,
            Map.of(
                "testShardA", jdbcResourceManagerShardA, "testShardB", jdbcResourceManagerShardB));

        // Upload session file
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
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
                null,
                null,
                null,
                customTransformation,
                "oracle",
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBShardedOracleRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSrcDBRetryDLQ() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO \""
            + testUsernameShardB
            + "\".\"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\", \"LegacyRegion\") VALUES (2, 'Customer 2', 1500, 'Silver')");

    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

    LOG.info("Waiting for DLQ events to appear in severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManagerShardB,
                                "\"" + testUsernameShardB + "\".\"Orders\"")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManagerShardA,
                                "\"" + testUsernameShardA + "\".\"AllDataTypes\"")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManagerShardB,
                                "\"" + testUsernameShardB + "\".\"Customers\"")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();

    LOG.info("Verifying Oracle state before retry job runs");
    List<Map<String, Object>> shardACustomersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA,
            testUsernameShardA,
            "SELECT \"CustomerId\" FROM \"Customers\"");
    List<Integer> shardACustomersIds =
        shardACustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=1 should NOT exist yet on Shard A", !shardACustomersIds.contains(1));

    List<Map<String, Object>> shardAOrdersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA, testUsernameShardA, "SELECT \"OrderId\" FROM \"Orders\"");
    List<Integer> shardAOrdersIds =
        shardAOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should NOT exist yet on Shard A", !shardAOrdersIds.contains(101));

    List<Map<String, Object>> shardBOrdersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardB, testUsernameShardB, "SELECT \"OrderId\" FROM \"Orders\"");
    List<Integer> shardBOrdersIds =
        shardBOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=102 should exist on Shard B", shardBOrdersIds.contains(102));

    List<Map<String, Object>> shardAAllDataTypesRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA, testUsernameShardA, "SELECT \"id\" FROM \"AllDataTypes\"");
    List<Integer> shardAAllDataTypesIds =
        shardAAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist on Shard A", shardAAllDataTypesIds.contains(1));

    LOG.info("Launching retryDLQ job with session file to process DLQ");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryDLQ");
    retryParams.put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));

    //     CustomTransformationImplFetcher.clearInstance();
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
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters("mode=semi-fixed")
                .build(),
            "oracle",
            retryParams);

    assertThatPipeline(retryJobInfo).isRunning();

    LOG.info("Applying partial fixes in Oracle (inserting missing parent row for Orders)");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \""
            + testUsernameShardA
            + "\".\"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\", \"LegacyRegion\") VALUES (3, 'Parent Customer A', 2000, 'Gold')");

    LOG.info("Waiting for the retryDLQ job to complete automatically");
    PipelineOperator.Result retryJobResult =
        pipelineOperator().waitUntilDone(createConfig(retryJobInfo, Duration.ofMinutes(15)));
    assertThatResult(retryJobResult).isLaunchFinished();

    LOG.info("Verifying that severe bucket has exactly 1 entry after retryDLQ job completes");
    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
            .setMinEvents(1)
            .build()
            .get());

    LOG.info("Waiting for fixed rows to appear in Oracle");
    PipelineOperator.Result finalWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                JDBCRowsCheck.builder(
                        jdbcResourceManagerShardA, "\"" + testUsernameShardA + "\".\"Orders\"")
                    .setMinRows(1)
                    .build()
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManagerShardA,
                                "\"" + testUsernameShardA + "\".\"AllDataTypes\"")
                            .setMinRows(2)
                            .build()));
    assertThatResult(finalWaitResult).meetsConditions();

    LOG.info("Verifying final target Oracle database contents across shards");

    shardACustomersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA,
            testUsernameShardA,
            "SELECT \"CustomerId\" FROM \"Customers\"");
    shardACustomersIds =
        shardACustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=1 should NOT exist on Shard A", !shardACustomersIds.contains(1));
    assertTrue("id=3 should exist on Shard A", shardACustomersIds.contains(3));

    List<Map<String, Object>> shardBCustomersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardB,
            testUsernameShardB,
            "SELECT \"CustomerId\" FROM \"Customers\"");
    List<Integer> shardBCustomersIds =
        shardBCustomersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=2 should exist on Shard B", shardBCustomersIds.contains(2));

    shardAOrdersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA, testUsernameShardA, "SELECT \"OrderId\" FROM \"Orders\"");
    shardAOrdersIds =
        shardAOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should exist on Shard A", shardAOrdersIds.contains(101));

    shardBOrdersRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardB, testUsernameShardB, "SELECT \"OrderId\" FROM \"Orders\"");
    shardBOrdersIds =
        shardBOrdersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=102 should exist on Shard B", shardBOrdersIds.contains(102));

    shardAAllDataTypesRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardA,
            testUsernameShardA,
            "SELECT \"id\", \"varchar_col\" FROM \"AllDataTypes\"");
    shardAAllDataTypesIds =
        shardAAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist on Shard A", shardAAllDataTypesIds.contains(1));
    assertTrue("id=999 should exist on Shard A", shardAAllDataTypesIds.contains(999));

    List<Map<String, Object>> shardBAllDataTypesRows =
        runIsolatedSQLQuery(
            jdbcResourceManagerShardB, testUsernameShardB, "SELECT \"id\" FROM \"AllDataTypes\"");
    List<Integer> shardBAllDataTypesIds =
        shardBAllDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=888 should NOT exist on Shard B", !shardBAllDataTypesIds.contains(888));

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

  private void insertDataInSpanner() {
    com.google.cloud.spanner.Mutation customer1 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
            .set("CustomerId")
            .to(1)
            .set("CustomerName")
            .to("Customer 1")
            .set("CreditLimit")
            .to(500)
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
            .to(3)
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
            .to(999)
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
            .to(888)
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
