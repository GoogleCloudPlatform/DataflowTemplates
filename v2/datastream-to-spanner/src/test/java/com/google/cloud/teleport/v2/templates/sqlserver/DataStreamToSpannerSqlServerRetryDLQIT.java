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
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
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

/** Integration test for live replication from SQL Server to Spanner using the retryDLQ mode. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerSqlServerRetryDLQIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerSqlServerRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/DataStreamToSpannerSqlServerRetryDLQIT/spanner-schema.sql";
  private static final String SQLSERVER_SCHEMA_FILE_RESOURCE =
      "sqlserver/DataStreamToSpannerSqlServerRetryDLQIT/sqlserver-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "sqlserver/DataStreamToSpannerSqlServerRetryDLQIT/overrides.json";
  private static final String GCS_PATH_PREFIX = "sqlserver-datastream-to-spanner-retrydlq-test";

  private static final HashSet<DataStreamToSpannerSqlServerRetryDLQIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static CloudSqlServerResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerSqlServerRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        // Setup SQL Server Source
        jdbcResourceManager = setUpSqlServerResourceManager();

        // Create SQL Server Schema using helper
        executeSqlScript(jdbcResourceManager, SQLSERVER_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        datastreamResourceManager = setUpDatastreamResourceManager();

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
        jobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
        jobParameters.put("transformationCustomParameters", "mode=bad");
        jobParameters.put("dlqMaxRetryCount", "1000");
        jobParameters.put("datastreamSourceType", "sqlserver");

        SqlServerSource sqlServerSource =
            SqlServerSource.builder(
                    jdbcResourceManager.getHost(),
                    jdbcResourceManager.getUsername(),
                    jdbcResourceManager.getPassword(),
                    jdbcResourceManager.getPort(),
                    jdbcResourceManager.getDatabaseName())
                .setAllowedTables(Map.of("dbo", List.of("Customers", "Orders", "AllDataTypes")))
                .build();

        // Launch regular pipeline using ITBase method
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                jobParameters,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                sqlServerSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (DataStreamToSpannerSqlServerRetryDLQIT instance : testInstances) {
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

    // 1. Insert parent rows directly into Spanner
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

    // 2. Insert test data into SQL Server
    insertDataInSqlServer();
    LOG.info("Data inserted into SQL Server successfully");

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
    LOG.info("DLQ events successfully generated in severe bucket");

    // 4. Launch retryDLQ job
    LOG.info("Launching retryDLQ job with schema overrides to process DLQ");
    Map<String, String> retryJobParameters = new HashMap<>();
    retryJobParameters.put("runMode", "retryDLQ");
    retryJobParameters.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryJobParameters.put(
        "transformationJarPath", getGcsPath("input/customShard.jar", gcsResourceManager));
    retryJobParameters.put("transformationClassName", "com.custom.CustomTransformationForDLQIT");
    retryJobParameters.put("transformationCustomParameters", "mode=good");
    retryJobParameters.put("dlqMaxRetryCount", "20");
    retryJobParameters.put("dlqRetryMinutes", "60");
    retryJobParameters.put(
        "deadLetterQueueDirectory", getGcsPath(GCS_PATH_PREFIX + "/dlq/", gcsResourceManager));
    retryJobParameters.put("datastreamSourceType", "sqlserver");

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
    LOG.info("RetryDLQ job launched: {}", retryJobInfo.jobId());
    assertThatPipeline(retryJobInfo).isRunning();

    // 5. Apply partial fixes
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
    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, GCS_PATH_PREFIX + "/dlq/severe/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .get());

    // 7. Verify Spanner state
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

  private void insertDataInSqlServer() {
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LoyaltyTier) VALUES (1, 'Customer 1', 500, 'Bronze')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (3, 101, 1000, 'Website')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (2, 102, 1000, 'AppStore')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Orders (CustomerId, OrderId, OrderValue, OrderSource) VALUES (4, 103, 1000, 'AppStore')");

    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col) VALUES (1, 'test1')");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO AllDataTypes (id, varchar_col, tinyint_col, text_col, date_col, smallint_col, bigint_col, float_col, decimal_col, datetime_col, time_col, char_col, binary_col, varbinary_col, bit_col, int_col) "
            + "VALUES (999, 'test999', 1, 'text999', '2023-01-01', 11, 1001, 2.5, 11.5, '2023-01-01 12:00:00', '12:00:01', 'c1', 0x1234, 0x5678, 1, 1001)");
  }

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/datastream-to-spanner")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
