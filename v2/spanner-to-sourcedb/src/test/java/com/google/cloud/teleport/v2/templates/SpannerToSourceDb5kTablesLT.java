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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
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
public class SpannerToSourceDb5kTablesLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb5kTablesLT.class);

  private static final int NUM_TABLES = 5000;

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

    subscriptionName =
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

    try (Connection conn = getJdbcConnection(jdbcResourceManager);
        Statement stmt = conn.createStatement()) {
      stmt.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1");
      stmt.execute("SET GLOBAL sync_binlog = 1");
    } catch (Exception e) {
      LOG.warn("Failed to restore MySQL global flags.", e);
    }

    LOG.info("Launching Dataflow job...");
    Map<String, String> params = new HashMap<>();
    params.put("maxShardConnections", "5");
    params.put(
        "changeStreamMetadataDatabase", spannerChangeStreamMetadataResourceManager.getDatabaseId());

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            1, // numWorkers
            1, // maxWorkers
            null, // customTransformation
            MYSQL_SOURCE_TYPE,
            "input/shard.json",
            null, // sessionFileName
            params);

    assertThatPipeline(jobInfo).isRunning();

    // 1. Write row in every table in Spanner
    LOG.info("Writing a row to all {} tables in Spanner...", NUM_TABLES);
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      mutations.add(Mutation.newInsertOrUpdateBuilder("table_" + i).set("id").to(42L).build());
      if (mutations.size() >= 1000) {
        spannerResourceManager.write(mutations);
        mutations.clear();
      }
    }
    if (!mutations.isEmpty()) {
      spannerResourceManager.write(mutations);
    }
    LOG.info("Successfully wrote a row to all tables in Spanner.");

    // 2. Assert the rows in MySQL
    LOG.info("Constructing batch query for verification...");
    List<String> batchQueries = new ArrayList<>();
    int batchSize = 100; // Smaller batch size to be safe against stack overrun
    for (int i = 1; i <= NUM_TABLES; i += batchSize) {
      StringBuilder queryBuilder = new StringBuilder("SELECT (");
      int end = Math.min(i + batchSize - 1, NUM_TABLES);
      for (int j = i; j <= end; j++) {
        queryBuilder.append("(SELECT COUNT(*) FROM table_").append(j).append(")");
        if (j < end) {
          queryBuilder.append(" + ");
        }
      }
      queryBuilder.append(") as count");
      batchQueries.add(queryBuilder.toString());
    }

    LOG.info("Waiting for rows to be replicated to MySQL...");
    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofHours(1), Duration.ofMinutes(2)),
            () -> {
              ExecutorService executor = Executors.newFixedThreadPool(10);
              try {
                List<Callable<Integer>> tasks = new ArrayList<>();
                for (String query : batchQueries) {
                  tasks.add(
                      () -> {
                        try (Connection conn = getJdbcConnection(jdbcResourceManager);
                            Statement stmt = conn.createStatement();
                            ResultSet rs = stmt.executeQuery(query)) {
                          if (rs.next()) {
                            int count = rs.getInt("count");
                            return count;
                          }
                        } catch (Exception e) {
                          LOG.warn("Error executing batch query", e);
                        }
                        return 0;
                      });
                }

                List<Future<Integer>> futures = executor.invokeAll(tasks);
                int totalCount = 0;
                for (Future<Integer> future : futures) {
                  totalCount += future.get();
                }

                LOG.info("Current total row count across all tables: {}", totalCount);
                return totalCount == NUM_TABLES;
              } catch (Exception e) {
                LOG.warn("Error checking row count", e);
              } finally {
                executor.shutdown();
              }
              return false;
            });
    assertThatResult(result).meetsConditions();
    LOG.info("Validation successful! Rows correctly replicated to all tables in MySQL.");
  }

  private static Connection getJdbcConnection(MySQLResourceManager mySQLResourceManager)
      throws SQLException {
    return DriverManager.getConnection(
        mySQLResourceManager.getUri(),
        mySQLResourceManager.getUsername(),
        mySQLResourceManager.getPassword());
  }
}
