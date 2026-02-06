/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.loadtesting;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
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
 * A load test for {@link SourceDbToSpanner} Flex template which tests 5,000 tables migration.
 *
 * <p>This test verifies the template's ability to handle a massive number of tables by:
 *
 * <ol>
 *   <li>Generating 5,000 tables in a source MySQL database.
 *   <li>Executing 5,000 DDL statements on the destination Spanner database in parallel.
 *   <li>Migrating the data using the Flex template with a constant-size graph.
 *   <li>Validating the results using a high-concurrency verification process.
 * </ol>
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQL5KTablesLT extends SourceDbToSpannerLTBase {
  private static final Logger LOG = LoggerFactory.getLogger(MySQL5KTablesLT.class);
  private Instant startTime;

  private MySQLResourceManager mySQLResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Began Setup for 5K Table test");
    super.setUp();
    startTime = Instant.now();

    // Initialize Resource Managers directly to avoid base class constraints
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setMonitoringClient(monitoringClient)
            .build();

    gcsResourceManager =
        GcsResourceManager.builder(
                TestProperties.artifactBucket(), getClass().getSimpleName(), CREDENTIALS)
            .build();

    this.dialect = SQLDialect.MYSQL;
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, mySQLResourceManager, gcsResourceManager);
    LOG.info(
        "CleanupCompleted for 5K Table test. Test took {}",
        Duration.between(startTime, Instant.now()));
  }

  /**
   * Tests the bulk migration of 5,000 tables from MySQL to Spanner.
   *
   * @throws Exception if any part of the test setup or execution fails.
   */
  @Test
  public void mySQLToSpannerBulkFiveThousandTablesTest() throws Exception {
    int numTables = 5000;

    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "BIGINT UNSIGNED");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    // OPTIMIZE MYSQL:
    // We disable synchronous flushing and binary logging to speed up the creation and
    // population of 5,000 tables on the source database.
    try (Connection jdbcConnection = getJdbcConnection(mySQLResourceManager);
        PreparedStatement pstmt =
            jdbcConnection.prepareStatement("SET GLOBAL innodb_flush_log_at_trx_commit = 0;")) {
      pstmt.executeUpdate();
    }
    try (Connection jdbcConnection = getJdbcConnection(mySQLResourceManager);
        PreparedStatement pstmt = jdbcConnection.prepareStatement("SET GLOBAL sync_binlog = 0;")) {
      pstmt.executeUpdate();
    }

    List<String> spannerDdlStatements = new ArrayList<>();

    LOG.info("Creating {} tables on Source and collecting Spanner DDLs", numTables);
    for (int i = 0; i < numTables; i++) {
      String tableName = "table_" + i;
      mySQLResourceManager.createTable(tableName, schema);

      try (Connection jdbcConnection = getJdbcConnection(mySQLResourceManager);
          PreparedStatement pstmt =
              jdbcConnection.prepareStatement("INSERT INTO " + tableName + " (id) VALUES (42)")) {
        pstmt.executeUpdate();
      }
      spannerDdlStatements.add("CREATE TABLE " + tableName + " (id INT64) PRIMARY KEY (id)");

      if (i % 500 == 0) {
        LOG.info("Created {} tables so far on Source", i);
      }
    }

    // Restore MySQL durability settings to ensure a realistic state for the template.
    try (Connection jdbcConnection = getJdbcConnection(mySQLResourceManager);
        PreparedStatement pstmt =
            jdbcConnection.prepareStatement("SET GLOBAL innodb_flush_log_at_trx_commit = 1;")) {
      pstmt.executeUpdate();
    }

    try (Connection jdbcConnection = getJdbcConnection(mySQLResourceManager);
        PreparedStatement pstmt = jdbcConnection.prepareStatement("SET GLOBAL sync_binlog = 1;")) {
      pstmt.executeUpdate();
    }

    // PARALLEL DDL EXECUTION:
    // Batch and execute spanner DDL statements in parallel to reduce setup time.
    LOG.info("Executing Spanner DDLs in parallel batches");
    int batchSize = 100;
    ExecutorService ddlExecutor = Executors.newFixedThreadPool(3);
    for (int i = 0; i < spannerDdlStatements.size(); i += batchSize) {
      int end = Math.min(spannerDdlStatements.size(), i + batchSize);
      List<String> batch = spannerDdlStatements.subList(i, end);
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

    // Prepare parameters and launch job
    Map<String, String> params = getCommonParameters();
    params.putAll(
        getJdbcParameters(
            mySQLResourceManager.getUri(),
            mySQLResourceManager.getUsername(),
            mySQLResourceManager.getPassword(),
            "com.mysql.jdbc.Driver"));
    params.put("maxConnections", "16");
    params.put("numWorkers", "16");
    params.put("maxNumWorkers", "16");
    params.put("workerMachineType", "n2-standard-4");

    LaunchConfig.Builder options = LaunchConfig.builder(testName, SPEC_PATH).setParameters(params);

    PipelineLauncher.LaunchInfo jobInfo = launchJob(options);

    // Wait for completion
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(60L)));
    assertThatResult(result).isLaunchFinished();

    // High-concurrency validation
    validateResult(spannerResourceManager, numTables);

    // Collect and export metrics
    collectAndExportMetrics(jobInfo);
  }

  private static Connection getJdbcConnection(MySQLResourceManager mySQLResourceManager)
      throws SQLException {
    try {
      return DriverManager.getConnection(
          mySQLResourceManager.getUri(),
          mySQLResourceManager.getUsername(),
          mySQLResourceManager.getPassword());
    } catch (Exception e) {
      LOG.error("Could not open connection to MySql", e);
      throw e;
    }
  }

  /**
   * Validates that all 5,000 tables were correctly migrated and contain the expected data.
   *
   * <p>To handle the large number of tables, validation is performed in parallel using a large
   * thread pool.
   *
   * @param resourceManager the Spanner resource manager.
   * @param numTables the total number of tables to validate.
   * @throws InterruptedException if validation is interrupted.
   */
  private void validateResult(SpannerResourceManager resourceManager, int numTables)
      throws InterruptedException {
    LOG.info("Validating {} tables on Spanner", numTables);
    ExecutorService executor = Executors.newFixedThreadPool(100);
    for (int i = 0; i < numTables; i++) {
      String tableName = "table_" + i;
      executor.submit(
          () -> {
            assertThat(resourceManager.getRowCount(tableName)).isEqualTo(1L);
            List<Struct> rows = resourceManager.readTableRecords(tableName, "id");
            assertThat(rows.get(0).getLong("id")).isEqualTo(42L);
          });
    }
    executor.shutdown();
    if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
      throw new RuntimeException("Validation timed out after 30 minutes");
    }
    LOG.info("Validation successful for all {} tables", numTables);
  }
}
