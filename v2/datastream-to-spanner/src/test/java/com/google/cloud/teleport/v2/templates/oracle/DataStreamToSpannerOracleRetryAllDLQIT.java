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
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerOracleRetryAllDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerOracleRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryAllDLQIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryAllDLQIT/oracle-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "oracle/DataStreamToSpannerOracleRetryAllDLQIT/overrides.json";
  private static final String GCS_PATH_PREFIX = "oracle-datastream-to-spanner-retryalldlq-test";

  private static final HashSet<DataStreamToSpannerOracleRetryAllDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static CloudOracleResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerOracleRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        // Setup Oracle Source
        CloudOracleResourceManager sysUser = setUpOracleResourceManager();

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder sysBuilder =
            CloudOracleResourceManager.builder(testName);
        sysBuilder.setHost(sysUser.getHost());
        sysBuilder.setPort(sysUser.getPort());
        sysBuilder.setUsername("sys as sysdba");
        sysBuilder.setPassword(System.getProperty("cloudProxyPassword"));
        sysBuilder.setDatabaseName(sysUser.getDatabaseName());
        CloudOracleResourceManager trueSysUser =
            (CloudOracleResourceManager) new SpannerOracleResourceManager(sysBuilder);

        String oracleUser = System.getProperty("cloudProxyUsername", "system");
        String oraclePassword = System.getProperty("cloudProxyPassword", "TestPassword123");

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder =
            CloudOracleResourceManager.builder(testName);
        builder.setHost(sysUser.getHost());
        builder.setPort(sysUser.getPort());
        builder.setUsername(oracleUser);
        builder.setPassword(oraclePassword);
        builder.setDatabaseName("/XEPDB1");

        jdbcResourceManager =
            (CloudOracleResourceManager) new SpannerOracleResourceManager(builder);

        // Create Oracle Schema using helper
        executeSqlScript(jdbcResourceManager, ORACLE_SCHEMA_FILE_RESOURCE);

        // Add Supplemental Log Data required by Datastream CDC
        List.of("Customers", "Orders", "AllDataTypes")
            .forEach(
                tableName -> {
                  jdbcResourceManager.runSQLUpdate(
                      String.format(
                          "ALTER TABLE \"%s\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableName));
                });

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

        // Prepare job parameters
        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
        jobParameters.put(
            "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
        jobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put("dlqMaxRetryCount", "20");
        jobParameters.put("dlqRetryMinutes", "1");

        OracleSource oracleSource =
            OracleSource.builder(
                    jdbcResourceManager.getHost(),
                    jdbcResourceManager.getUsername(),
                    jdbcResourceManager.getPassword(),
                    jdbcResourceManager.getPort(),
                    jdbcResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(
                        jdbcResourceManager.getUsername().toUpperCase(),
                        List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        // Launch regular pipeline using ITBase method
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
                oracleSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerOracleRetryAllDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, jdbcResourceManager, gcsResourceManager, datastreamResourceManager);
  }

  @Test
  public void testDataStreamToSpannerRetryAllDLQ() throws Exception {
    LOG.info("Starting testDataStreamToSpannerRetryAllDLQ");
    assertThatPipeline(jobInfo).isRunning();

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

    insertDataInOracle();
    LOG.info("Data inserted into Oracle successfully");

    LOG.info("Waiting for DLQ events to appear in retry and severe buckets");
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

    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(15)));

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

    Map<String, String> retryJobParameters = new HashMap<>();
    retryJobParameters.put("runMode", "retryAllDLQ");
    retryJobParameters.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryJobParameters.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryJobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
    retryJobParameters.put("transformationCustomParameters", "mode=good");
    retryJobParameters.put("dlqMaxRetryCount", "20");
    retryJobParameters.put("dlqRetryMinutes", "1");
    retryJobParameters.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "-retry",
            null,
            null,
            GCS_PATH_PREFIX + "-retry",
            spannerResourceManager,
            null,
            retryJobParameters,
            null,
            null,
            gcsResourceManager,
            null,
            null,
            null);

    assertThatPipeline(retryJobInfo).isRunning();

    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/retry/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
                    .setMinEvents(1)
                    .setMaxEvents(1)
                    .build())
            .and(
                SpannerRowsCheck.builder(spannerResourceManager, "AllDataTypes")
                    .setMinRows(2)
                    .build())
            .and(SpannerRowsCheck.builder(spannerResourceManager, "Orders").setMinRows(2).build());

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(30)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();

    assertTrue(
        "id=999 should exist in AllDataTypes", rowExistsInSpanner("AllDataTypes", "id", 999));

    List<com.google.cloud.spanner.Struct> rows =
        spannerResourceManager.runQuery("SELECT * FROM AllDataTypes WHERE id = 999");
    Map<String, Object> expectedRow = createExpectedRowFor999();
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(expectedRow));

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

  private void insertDataInOracle() {
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\","
            + " \"LoyaltyTier\") VALUES (1, 'Customer 1', 500, 'Bronze')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (3, 101, 1000, 'Website')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (2, 102, 1000, 'AppStore')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"Orders\" (\"CustomerId\", \"OrderId\", \"OrderValue\", \"OrderSource\")"
            + " VALUES (4, 103, 1000, 'AppStore')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar2_col\") VALUES (1, 'test1')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \"AllDataTypes\" (\"id\", \"varchar2_col\") VALUES (999, 'test999')");
  }

  private Map<String, Object> createExpectedRowFor999() {
    Map<String, Object> row = new HashMap<>();
    row.put("id", 999L);
    row.put("varchar2_col", "test999");
    return row;
  }

  private String getCustomShardJarPath() {
    return "/home/dhwanilpatel_google_com/MyStorage/OracleSupport/DataflowTemplates/v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
