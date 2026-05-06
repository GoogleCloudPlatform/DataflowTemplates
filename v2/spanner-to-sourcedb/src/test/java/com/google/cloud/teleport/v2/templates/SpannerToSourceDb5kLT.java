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
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
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
 * Load test for {@link SpannerToSourceDb} Flex template with 5000 tables using parallel DDL
 * execution.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDb5kLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb5kLT.class);

  private static final int NUM_TABLES = 5000;
  private static final String TEMPLATE_SPEC_PATH =
      com.google.common.base.MoreObjects.firstNonNull(
          org.apache.beam.it.common.TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Spanner_to_SourceDb");

  private MySQLResourceManager jdbcResourceManager;
  private SpannerResourceManager spannerChangeStreamMetadataResourceManager;
  private Instant startTime;

  @Before
  public void setUp() throws IOException {
    LOG.info("Began Setup for 5K Table test");
    super.setUp();
    startTime = Instant.now();

    // Initialize Resource Managers directly to avoid base class constraints
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();
    jdbcResourceManagers.add(jdbcResourceManager);

    spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, project, region)
            .maybeUseStaticInstance()
            .setMonitoringClient(monitoringClient)
            .build();

    spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, project, region)
            .maybeUseStaticInstance()
            .build();
    spannerMetadataResourceManager.ensureUsableAndCreateResources();

    spannerChangeStreamMetadataResourceManager =
        SpannerResourceManager.builder("rr-cs-meta-" + testName, project, region)
            .maybeUseStaticInstance()
            .build();
    spannerChangeStreamMetadataResourceManager.ensureUsableAndCreateResources();

    gcsResourceManager = createSpannerLTGcsResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        spannerChangeStreamMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
    if (jdbcResourceManagers != null) {
      for (JDBCResourceManager rm : jdbcResourceManagers) {
        ResourceManagerUtils.cleanResources(rm);
      }
    }
    LOG.info(
        "CleanupCompleted for 5K Table test. Test took {}",
        Duration.between(startTime, Instant.now()));
  }

  @Test
  public void test5kTables() throws Exception {
    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManagers);

    SubscriptionName subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""));

    LOG.info("Collecting {} Spanner DDL statements...", NUM_TABLES);
    List<String> spannerDdls = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      spannerDdls.add(
          String.format("CREATE TABLE table_%d (id INT64 NOT NULL) PRIMARY KEY(id)", i));
    }

    LOG.info("Executing Spanner DDLs...");
    spannerResourceManager.executeDdlStatements(spannerDdls);

    LOG.info("Creating change stream in Spanner...");
    spannerResourceManager.executeDdlStatement(
        "CREATE CHANGE STREAM allstream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d', allow_txn_exclusion = true)");

    // OPTIMIZE MYSQL:
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 0");
      stmt.execute("SET GLOBAL sync_binlog = 0");
    } catch (Exception e) {
      LOG.warn("Failed to set MySQL global optimization flags. Proceeding anyway.", e);
    }

    LOG.info("Creating table_1 in MySQL as master...");
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE table_1 (id BIGINT UNSIGNED NOT NULL PRIMARY KEY)");
    } catch (Exception e) {
      LOG.error("Failed to create master table table_1 in MySQL", e);
      throw new RuntimeException(e);
    }

    LOG.info("Creating remaining {} tables in MySQL using LIKE...", NUM_TABLES - 1);
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      for (int j = 2; j <= NUM_TABLES; j++) {
        String mySqlDdl = String.format("CREATE TABLE table_%d LIKE table_1", j);
        stmt.addBatch(mySqlDdl);
      }
      stmt.executeBatch();
    } catch (Exception e) {
      LOG.error("Failed to create tables in MySQL", e);
      throw new RuntimeException(e);
    }
    LOG.info("Created {} tables on Source", NUM_TABLES);

    // Restore MySQL settings
    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1");
      stmt.execute("SET GLOBAL sync_binlog = 1");
    } catch (Exception e) {
      LOG.warn("Failed to restore MySQL global flags.", e);
    }

    LOG.info("Launching Dataflow job...");
    Map<String, String> params = new HashMap<>();
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("spannerProjectId", project);
    params.put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
    params.put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
    params.put("sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager));
    params.put("changeStreamName", "allstream");
    params.put("dlqGcsPubSubSubscription", subscriptionName.toString());
    params.put("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager));
    params.put("maxShardConnections", "5");
    params.put("maxNumWorkers", "1");
    params.put("numWorkers", "1");
    params.put("sourceType", MYSQL_SOURCE_TYPE);
    params.put("workerMachineType", "n2-standard-4");
    params.put(
        "changeStreamMetadataDatabase", spannerChangeStreamMetadataResourceManager.getDatabaseId());

    String jobName = PipelineUtils.createJobName("rrev-it" + testName);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, TEMPLATE_SPEC_PATH);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    options.addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE");

    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());

    assertThatPipeline(jobInfo).isRunning();

    // 1. Write row in Spanner
    LOG.info("Writing a row to Spanner...");
    Mutation m = Mutation.newInsertOrUpdateBuilder("table_1").set("id").to(42L).build();
    spannerResourceManager.write(m);
    LOG.info("Successfully wrote a row to Spanner.");

    // 2. Assert the row in MySQL
    LOG.info("Waiting for row to be replicated to MySQL...");
    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofHours(1)),
            () -> jdbcResourceManager.getRowCount("table_1") == 1);
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> rows = jdbcResourceManager.readTable("table_1");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id").toString()).isEqualTo("42");
    LOG.info("Validation successful! Row correctly replicated to MySQL.");
  }

  private static Connection getJdbcConnection(MySQLResourceManager mySQLResourceManager)
      throws SQLException {
    return DriverManager.getConnection(
        mySQLResourceManager.getUri(),
        mySQLResourceManager.getUsername(),
        mySQLResourceManager.getPassword());
  }
}
