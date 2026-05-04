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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template with 5000 tables using parallel DDL
 * execution.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDb5kIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb5kIT.class);

  private static final int NUM_TABLES = 4999;

  private SpannerResourceManager spannerResourceManager;
  private SpannerResourceManager spannerMetadataResourceManager;
  private MySQLResourceManager jdbcResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();
    pubsubResourceManager = setUpPubSubResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testLargeSchemaLaunch() throws Exception {
    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

    SubscriptionName subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);

    LOG.info("Collecting {} Spanner DDL statements...", NUM_TABLES);
    List<String> spannerDdls = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      spannerDdls.add(
          String.format("CREATE TABLE table_%d (id INT64 NOT NULL) PRIMARY KEY(id)", i));
    }

    LOG.info("Executing Spanner DDLs in parallel batches...");
    int batchSize = 100;
    ExecutorService ddlExecutor = Executors.newFixedThreadPool(3);
    for (int i = 0; i < spannerDdls.size(); i += batchSize) {
      int end = Math.min(spannerDdls.size(), i + batchSize);
      List<String> batch = spannerDdls.subList(i, end);
      ddlExecutor.submit(
          () -> {
            try {
              spannerResourceManager.executeDdlStatements(batch);
            } catch (Exception e) {
              LOG.error("Failed to execute Spanner DDL batch", e);
            }
          });
    }
    ddlExecutor.shutdown();
    if (!ddlExecutor.awaitTermination(60, TimeUnit.MINUTES)) {
      throw new RuntimeException("Spanner DDL timed out after 60 minutes");
    }

    LOG.info("Creating change stream in Spanner...");
    spannerResourceManager.executeDdlStatement(
        "CREATE CHANGE STREAM allstream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d', allow_txn_exclusion = true)");

    // OPTIMIZE MYSQL:
    // Disable synchronous flushing and binary logging to speed up table creation.
    try (Connection conn =
            DriverManager.getConnection(
                jdbcResourceManager.getUri(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 0");
      stmt.execute("SET GLOBAL sync_binlog = 0");
    } catch (Exception e) {
      LOG.warn("Failed to set MySQL global optimization flags. Proceeding anyway.", e);
    }

    LOG.info("Creating {} tables in MySQL in parallel batches...", NUM_TABLES);
    int mysqlBatchSize = 100;
    ExecutorService mysqlExecutor = Executors.newFixedThreadPool(5);
    for (int i = 1; i <= NUM_TABLES; i += mysqlBatchSize) {
      final int start = i;
      final int end = Math.min(NUM_TABLES, i + mysqlBatchSize - 1);
      mysqlExecutor.submit(
          () -> {
            try (Connection conn =
                    DriverManager.getConnection(
                        jdbcResourceManager.getUri(),
                        jdbcResourceManager.getUsername(),
                        jdbcResourceManager.getPassword());
                Statement stmt = conn.createStatement()) {
              for (int j = start; j <= end; j++) {
                String mySqlDdl =
                    String.format(
                        "CREATE TABLE table_%d (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)", j);
                stmt.addBatch(mySqlDdl);
              }
              stmt.executeBatch();
            } catch (Exception e) {
              LOG.error("Failed to execute MySQL DDL batch for tables {}-{}", start, end, e);
            }
          });
    }
    mysqlExecutor.shutdown();
    if (!mysqlExecutor.awaitTermination(30, TimeUnit.MINUTES)) {
      throw new RuntimeException("MySQL table creation timed out");
    }
    LOG.info("Created {} tables on Source", NUM_TABLES);

    // Restore MySQL settings
    try (Connection conn =
            DriverManager.getConnection(
                jdbcResourceManager.getUri(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1");
      stmt.execute("SET GLOBAL sync_binlog = 1");
    } catch (Exception e) {
      LOG.warn("Failed to restore MySQL global flags.", e);
    }

    LOG.info("Launching Dataflow job...");
    Map<String, String> jobParameters = new HashMap<>();

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            subscriptionName.toString(),
            null,
            null,
            null,
            null,
            null,
            MYSQL_SOURCE_TYPE,
            jobParameters);

    assertThatPipeline(jobInfo).isRunning();

    // 1. Write row in Spanner
    LOG.info("Writing a row to Spanner...");
    com.google.cloud.spanner.Mutation m =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("table_1")
            .set("id")
            .to(42L)
            .build();
    spannerResourceManager.write(m);
    LOG.info("Successfully wrote a row to Spanner.");

    // 2. Assert the row in MySQL
    LOG.info("Waiting for row to be replicated to MySQL...");
    org.apache.beam.it.common.PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, java.time.Duration.ofHours(1)),
                () -> jdbcResourceManager.getRowCount("table_1") == 1);
    org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult(result).meetsConditions();

    java.util.List<java.util.Map<String, Object>> rows = jdbcResourceManager.readTable("table_1");
    com.google.common.truth.Truth.assertThat(rows).hasSize(1);
    com.google.common.truth.Truth.assertThat(rows.get(0).get("id").toString()).isEqualTo("42");
    LOG.info("Validation successful! Row correctly replicated to MySQL.");
  }
}
