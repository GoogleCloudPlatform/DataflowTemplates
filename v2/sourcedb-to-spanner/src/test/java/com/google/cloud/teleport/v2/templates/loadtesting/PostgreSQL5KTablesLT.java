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
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A load test for {@link SourceDbToSpanner} Flex template which tests 5,000 tables migration for
 * PostgreSQL.
 *
 * <p>This test follows the same massive-scale verification strategy as {@link MySQL5KTablesLT},
 * tailored for PostgreSQL specific optimizations and drivers.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQL5KTablesLT extends SourceDbToSpannerLTBase {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQL5KTablesLT.class);
  private Instant startTime;

  private PostgresResourceManager postgresResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Began Setup for 5K Table test (PostgreSQL)");
    super.setUp();
    startTime = Instant.now();

    // Initialize Resource Managers directly to avoid base class constraints
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setMonitoringClient(monitoringClient)
            .build();

    gcsResourceManager =
        GcsResourceManager.builder(
                TestProperties.artifactBucket(), getClass().getSimpleName(), CREDENTIALS)
            .build();

    this.dialect = SQLDialect.POSTGRESQL;
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, postgresResourceManager, gcsResourceManager);
    LOG.info(
        "CleanupCompleted for 5K Table test. Test took {}",
        Duration.between(startTime, Instant.now()));
  }

  /**
   * Tests the bulk migration of 5,000 tables from PostgreSQL to Spanner.
   *
   * @throws Exception if any part of the test setup or execution fails.
   */
  @Test
  public void postgresToSpannerBulkFiveThousandTablesTest() throws Exception {
    int numTables = 5000;

    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "BIGINT");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    // OPTIMIZE POSTGRESQL:
    // We disable synchronous commit to speed up the massive number of INSERT and DDL operations
    // required for 5,000 tables.
    try (Connection jdbcConnection = getJdbcConnection(postgresResourceManager);
        PreparedStatement pstmt =
            jdbcConnection.prepareStatement("SET synchronous_commit = off;")) {
      pstmt.executeUpdate();
    }

    List<String> spannerDdlStatements = new ArrayList<>();

    LOG.info("Creating {} tables on Source and collecting Spanner DDLs", numTables);
    for (int i = 0; i < numTables; i++) {
      String tableName = "table_" + i;
      postgresResourceManager.createTable(tableName, schema);

      try (Connection jdbcConnection = getJdbcConnection(postgresResourceManager);
          PreparedStatement pstmt =
              jdbcConnection.prepareStatement("INSERT INTO " + tableName + " (id) VALUES (42)")) {
        pstmt.executeUpdate();
      }
      spannerDdlStatements.add("CREATE TABLE " + tableName + " (id INT64) PRIMARY KEY (id)");

      if (i % 500 == 0) {
        LOG.info("Created {} tables so far on Source", i);
      }
    }

    // Restore PostgreSQL durability settings.
    try (Connection jdbcConnection = getJdbcConnection(postgresResourceManager);
        PreparedStatement pstmt = jdbcConnection.prepareStatement("SET synchronous_commit = on;")) {
      pstmt.executeUpdate();
    }

    // Parallelize Spanner DDL execution.
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
            postgresResourceManager.getUri(),
            postgresResourceManager.getUsername(),
            postgresResourceManager.getPassword(),
            "org.postgresql.Driver"));
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

  private static Connection getJdbcConnection(PostgresResourceManager postgresResourceManager)
      throws SQLException {
    try {
      return DriverManager.getConnection(
          postgresResourceManager.getUri(),
          postgresResourceManager.getUsername(),
          postgresResourceManager.getPassword());
    } catch (Exception e) {
      LOG.error("Could not open connection to PostgreSQL", e);
      throw e;
    }
  }

  /**
   * Validates that all 5,000 tables were correctly migrated and contain the expected data.
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
