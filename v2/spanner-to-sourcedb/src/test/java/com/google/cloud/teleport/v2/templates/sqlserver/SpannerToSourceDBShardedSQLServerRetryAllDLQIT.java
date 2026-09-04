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

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_SQLSERVER;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
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
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
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
 * Integration test for reverse replication from Spanner to SQL Server using retryAllDLQ mode for
 * sharded clusters.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBShardedSQLServerRetryAllDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBShardedSQLServerRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SpannerToSourceDBShardedSQLServerRetryAllDLQIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/SpannerToSourceDBShardedSQLServerRetryAllDLQIT/sqlserver-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "sqlserver/SpannerToSourceDBShardedSQLServerRetryAllDLQIT/session.json";
  private static final HashSet<SpannerToSourceDBShardedSQLServerRetryAllDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MSSQLResourceManager jdbcResourceManagerShardA;
  public static MSSQLResourceManager jdbcResourceManagerShardB;
  public static GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBShardedSQLServerRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(
                SpannerToSourceDBShardedSQLServerRetryAllDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManagerShardA = setUpMSSQLResourceManager(testName + "shardA");
        createSQLServerSchema(
            jdbcResourceManagerShardA,
            SpannerToSourceDBShardedSQLServerRetryAllDLQIT.SQLSERVER_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = setUpMSSQLResourceManagerShardB(testName + "shardB");
        createSQLServerSchema(
            jdbcResourceManagerShardB,
            SpannerToSourceDBShardedSQLServerRetryAllDLQIT.SQLSERVER_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        createAndUploadShardConfigToGcs(
            gcsResourceManager,
            Map.of(
                "testShardA", jdbcResourceManagerShardA,
                "testShardB", jdbcResourceManagerShardB));

        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters("mode=bad")
                .build();

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
                put("dlqMaxRetryCount", "20");
                put("dlqRetryMinutes", "60");
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                null,
                getClass().getSimpleName(),
                "input/customShard.jar",
                "com.custom.CustomShardIdFetcherForRetryIT",
                null,
                customTransformation,
                SOURCE_SQLSERVER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBShardedSQLServerRetryAllDLQIT instance : testInstances) {
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

    jdbcResourceManagerShardB.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (2, 'Customer 2', 1500, 'Silver')");

    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

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
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManagerShardA, "AllDataTypes")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManagerShardB, "Customers")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();

    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(15)));

    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (3, 'Parent Customer A', 2000, 'Gold')");

    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryAllDLQ");
    retryParams.put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
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
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters("mode=semi-fixed")
                .build(),
            SOURCE_SQLSERVER,
            retryParams);

    assertThatPipeline(retryJobInfo).isRunning();

    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(1)
                    .setMaxEvents(1)
                    .build())
            .and(JDBCRowsCheck.builder(jdbcResourceManagerShardA, "Orders").setMinRows(1).build())
            .and(
                JDBCRowsCheck.builder(jdbcResourceManagerShardA, "AllDataTypes")
                    .setMinRows(2)
                    .build());

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(15)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();

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

    assertTrue("id=1 should exist on Shard A", shardAAllTypesIds.contains(1));
    assertTrue("id=999 should exist on Shard A", shardAAllTypesIds.contains(999));
    assertTrue("id=888 should NOT exist on Shard B", !shardBAllTypesIds.contains(888));

    List<Map<String, Object>> shardACust =
        jdbcResourceManagerShardA.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardACustIds =
        shardACust.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();

    List<Map<String, Object>> shardBCust =
        jdbcResourceManagerShardB.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> shardBCustIds =
        shardBCust.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();

    assertTrue("id=1 should NOT exist on Shard A", !shardACustIds.contains(1));
    assertTrue("id=3 should exist on Shard A", shardACustIds.contains(3));
    assertTrue("id=2 should exist on Shard B", shardBCustIds.contains(2));

    List<Map<String, Object>> shardAOrders =
        jdbcResourceManagerShardA.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardAOrderIds =
        shardAOrders.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();

    List<Map<String, Object>> shardBOrders =
        jdbcResourceManagerShardB.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> shardBOrderIds =
        shardBOrders.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();

    assertTrue("id=101 should exist on Shard A", shardAOrderIds.contains(101));
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
            .to(999)
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
            .to(888)
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
