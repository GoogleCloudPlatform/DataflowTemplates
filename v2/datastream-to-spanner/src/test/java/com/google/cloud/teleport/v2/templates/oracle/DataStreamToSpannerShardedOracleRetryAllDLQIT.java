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
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
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
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
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
 * Integration test for live replication from Oracle to Spanner using the retryAllDLQ mode for a
 * sharded setup.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerShardedOracleRetryAllDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerShardedOracleRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/DataStreamToSpannerShardedOracleRetryAllDLQIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/DataStreamToSpannerShardedOracleRetryAllDLQIT/oracle-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "oracle/DataStreamToSpannerShardedOracleRetryAllDLQIT/overrides.json";
  private static final String GCS_PATH_PREFIX = "oracle-datastream-to-spanner-sharded-retryall";

  private static final HashSet<DataStreamToSpannerShardedOracleRetryAllDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static CloudOracleResourceManager jdbcResourceManagerShardA;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String streamNameA;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerShardedOracleRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        String oracleUser = System.getProperty("cloudProxyUsername", "system");
        String oraclePassword = System.getProperty("cloudProxyPassword", "TestPassword123");

        jdbcResourceManagerShardA =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName)
                    .setUsername(oracleUser)
                    .setPassword(oraclePassword)
                    .setDatabaseName("XEPDB1")
                    .setHost(System.getProperty("hostIp"))
                    .setPort(1521)
                    .build();
        try {
          jdbcResourceManagerShardA.runSQLUpdate("DROP TABLE \"Customers\"");
        } catch (Exception e) {
        }
        try {
          jdbcResourceManagerShardA.runSQLUpdate("DROP TABLE \"Orders\"");
        } catch (Exception e) {
        }
        try {
          jdbcResourceManagerShardA.runSQLUpdate("DROP TABLE \"AllDataTypes\"");
        } catch (Exception e) {
        }
        executeSqlScript(jdbcResourceManagerShardA, ORACLE_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardA.runSQLUpdate(
            "ALTER TABLE \"Customers\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        jdbcResourceManagerShardA.runSQLUpdate(
            "ALTER TABLE \"Orders\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        jdbcResourceManagerShardA.runSQLUpdate(
            "ALTER TABLE \"AllDataTypes\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        datastreamResourceManager =
            org.apache.beam.it.gcp.datastream.DatastreamResourceManager.builder(
                    testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        org.apache.beam.it.gcp.datastream.OracleSource jdbcSource =
            org.apache.beam.it.gcp.datastream.OracleSource.builder(
                    System.getProperty("hostIp"), oracleUser, oraclePassword, 1521, "XEPDB1")
                .setAllowedTables(
                    java.util.Map.of(
                        oracleUser, java.util.List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        com.google.cloud.datastream.v1.SourceConfig sourceConfig =
            datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

        com.google.cloud.datastream.v1.DestinationConfig destinationConfig =
            datastreamResourceManager.buildGCSDestinationConfig(
                "gcs-profile",
                gcsResourceManager.getBucket(),
                GCS_PATH_PREFIX + "/cdc/",
                org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat
                    .AVRO_FILE_FORMAT);

        com.google.cloud.datastream.v1.Stream stream =
            datastreamResourceManager.createStream(
                "test_stream_"
                    + org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric(5)
                        .toLowerCase(),
                sourceConfig,
                destinationConfig);

        datastreamResourceManager.startStream(stream);
        streamNameA = stream.getName().substring(stream.getName().lastIndexOf('/') + 1);

        String shardConfig =
            generateSourceConfig(
                streamNameA, oracleUser, "shard1", streamNameA, oracleUser, "shard2");

        gcsResourceManager.createArtifact(
            "input/shardingConfig.conf",
            shardConfig.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        gcsResourceManager.uploadArtifact(
            "input/overrides.json",
            com.google.common.io.Resources.getResource(
                    "oracle/DataStreamToSpannerShardedOracleRetryAllDLQIT/overrides.json")
                .getPath());
        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        java.util.Map<String, String> jobParameters = new java.util.HashMap<>();
        jobParameters.put(
            "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
        jobParameters.put(
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put("dlqMaxRetryCount", "20");
        jobParameters.put("dlqRetryMinutes", "60");
        jobParameters.put(
            "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
        jobParameters.put(
            "sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));
        // Explicitly set inputFilePattern for manual stream
        jobParameters.put(
            "inputFilePattern",
            "gs://" + gcsResourceManager.getBucket() + "/" + GCS_PATH_PREFIX + "/cdc/");

        if (System.getProperty("jdbcDriverJars") != null) {
          String driverPath = System.getProperty("jdbcDriverJars");
          jobParameters.put("jdbcDriverJars", driverPath);
        }

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                null,
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                null);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerShardedOracleRetryAllDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManagerShardA,
        gcsResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testDataStreamToSpannerShardedRetryAllDLQ() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    // 1. Insert parent rows directly into Spanner.
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

    // 2. Insert all test data into Oracle.
    insertDataInOracle();
    LOG.info("Data inserted into Oracle successfully");

    // 3. Wait for DLQ events to appear in corresponding buckets.
    LOG.info("Waiting for DLQ events to appear in shared severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(25)),
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
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "Customers")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build())
                    .and(
                        SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                            .setMinRows(1)
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();
    LOG.info("DLQ events successfully generated in corresponding buckets");

    // Verify Spanner state
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

    // 4. Stop the regular pipeline
    LOG.info("Stopping regular pipeline");
    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(5)));
    LOG.info("Regular pipeline stopped successfully");

    // 5. Apply partial fixes
    LOG.info("Applying partial fixes in Spanner");
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

    // 6. Launch retryAllDLQ job
    LOG.info("Launching retryAllDLQ job");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryAllDLQ");
    retryParams.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryParams.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryParams.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
    retryParams.put("transformationCustomParameters", "mode=good");
    retryParams.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
    retryParams.put("dlqMaxRetryCount", "20");
    retryParams.put("dlqRetryMinutes", "60");
    retryParams.put("sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));

    // Proprietary Drivers
    if (System.getProperty("jdbcDriverJars") != null) {
      String driverPath = System.getProperty("jdbcDriverJars");
      retryParams.put("jdbcDriverJars", driverPath);
    }

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            null,
            null,
            GCS_PATH_PREFIX + "-retry",
            spannerResourceManager,
            null,
            retryParams,
            null,
            null,
            gcsResourceManager,
            null,
            null,
            null);
    LOG.info("RetryAllDLQ job launched: {}", retryJobInfo.jobId());
    assertThatPipeline(retryJobInfo).isRunning();

    // 7. Wait for the retry job
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets after retry");
    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/retry/")
            .setMinEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                    .setMinEvents(1)
                    .build())
            .and(
                SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                    .setMinRows(2)
                    .build())
            .and(SpannerRowsCheck.builder(spannerResourceManager, "Orders").setMinRows(2).build());

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(15)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();
    LOG.info("Retry job completed processing successfully");

    // 8. Verify Spanner state
    assertTrue(
        "id=999 should exist in AllDataTypes", rowExistsInSpanner("AllDataTypes", "id", 999));
    assertTrue("id=3 should exist in Customers", rowExistsInSpanner("Customers", "CustomerId", 3));
    assertTrue(
        "id=1 should NOT exist in Customers", !rowExistsInSpanner("Customers", "CustomerId", 1));
    assertTrue("id=101 should exist in Orders", rowExistsInSpanner("Orders", "OrderId", 101));
    assertTrue("id=103 should NOT exist in Orders", !rowExistsInSpanner("Orders", "OrderId", 103));
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
    return "/home/dhwanilpatel_google_com/MyStorage/OracleSupport/DataflowTemplates/v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }

  private void insertDataInOracle() {
    LOG.info("Inserting data in Oracle");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\", \"LoyaltyTier\") VALUES (1, 'Customer 1', 500, 'Bronze')");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\") VALUES (3, 101, 1000, 'Website')");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\") VALUES (2, 102, 1000, 'AppStore')");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\") VALUES (4, 103, 1000, 'AppStore')");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar_col\") VALUES (1, 'test1')");
    jdbcResourceManagerShardA.runSQLUpdate(
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar_col\") VALUES (999, 'test999')");
  }

  private String generateSourceConfig(
      String streamA, String dbA, String shardA, String streamB, String dbB, String shardB) {
    return "{\n"
        + "  \"shardConfigs\": [\n"
        + "    {\n"
        + "      \"logicalShardId\": \""
        + shardA
        + "\",\n"
        + "      \"dbName\": \""
        + dbA
        + "\",\n"
        + "      \"streamId\": \""
        + streamA
        + "\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"logicalShardId\": \""
        + shardB
        + "\",\n"
        + "      \"dbName\": \""
        + dbB
        + "\",\n"
        + "      \"streamId\": \""
        + streamB
        + "\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }
}
