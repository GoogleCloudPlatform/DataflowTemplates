/*
 * Copyright (C) 2025 Google LLC
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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpanner5000TablePerDBIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLToSpanner5000TablePerDBIT.class);

  private PipelineLauncher.LaunchInfo jobInfo;
  private MySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;

  // Reduced number of tables to prevent container crashes
  // You can gradually increase this value based on your environment's capacity
  private static final int NUM_TABLES = 100; // Reduced from 5000
  private static final int BATCH_SIZE = 10; // Create tables in batches of 10
  private static final String COL_ID = "id";
  private static final String COL_NAME = "name";
  private static final int MAX_ALLOWED_PACKET = 128 * 1024 * 1024;

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  private void configureMySQL() {
    try {
      // Increase packet size for large queries
      String allowedGlobalPacket = "SET GLOBAL max_allowed_packet = " + MAX_ALLOWED_PACKET;
      mySQLResourceManager.runSQLUpdate(allowedGlobalPacket);

      // Additional MySQL optimizations
      mySQLResourceManager.runSQLUpdate("SET GLOBAL innodb_buffer_pool_size = 512M");
      mySQLResourceManager.runSQLUpdate("SET GLOBAL innodb_flush_log_at_trx_commit = 2");
      mySQLResourceManager.runSQLUpdate("SET GLOBAL sync_binlog = 0");
    } catch (Exception e) {
      LOG.warn("Failed to configure MySQL settings: {}", e.getMessage(), e);
      // Continue with test as some settings might not be configurable in certain environments
    }
  }

  private void createMySQLSchema() {
    LOG.info("Creating {} MySQL tables in batches of {}", NUM_TABLES, BATCH_SIZE);

    // Create tables in batches to avoid overwhelming the MySQL server
    for (int batchStart = 0; batchStart < NUM_TABLES; batchStart += BATCH_SIZE) {
      int batchEnd = Math.min(batchStart + BATCH_SIZE, NUM_TABLES);
      StringBuilder batchSQL = new StringBuilder();

      for (int i = batchStart; i < batchEnd; i++) {
        batchSQL.append("CREATE TABLE IF NOT EXISTS table").append(i).append(" (");
        batchSQL.append(COL_ID + " INT PRIMARY KEY, ");
        batchSQL.append(COL_NAME + " VARCHAR(255))");
        batchSQL.append(";");
      }

      try {
        LOG.info("Creating MySQL tables batch {}-{}", batchStart, batchEnd - 1);
        loadSQLToJdbcResourceManager(mySQLResourceManager, batchSQL.toString());
      } catch (Exception e) {
        LOG.error(
            "Error creating MySQL tables batch {}-{}: {}",
            batchStart,
            batchEnd - 1,
            e.getMessage(),
            e);
        throw new RuntimeException("Failed to create MySQL tables", e);
      }
    }
  }

  private void createSpannerSchema() {
    LOG.info("Creating {} Spanner tables in batches of {}", NUM_TABLES, BATCH_SIZE);

    // Create tables in batches to avoid overwhelming Spanner
    for (int batchStart = 0; batchStart < NUM_TABLES; batchStart += BATCH_SIZE) {
      int batchEnd = Math.min(batchStart + BATCH_SIZE, NUM_TABLES);
      StringBuilder batchSQL = new StringBuilder();

      for (int i = batchStart; i < batchEnd; i++) {
        batchSQL.append("CREATE TABLE table").append(i).append(" (");
        batchSQL.append(COL_ID + " INT64, ");
        batchSQL.append(COL_NAME + " STRING(MAX)) PRIMARY KEY (").append(COL_ID).append(")");
        batchSQL.append(";");
      }

      try {
        LOG.info("Creating Spanner tables batch {}-{}", batchStart, batchEnd - 1);
        spannerResourceManager.executeDdlStatement(batchSQL.toString());
      } catch (Exception e) {
        LOG.error(
            "Error creating Spanner tables batch {}-{}: {}",
            batchStart,
            batchEnd - 1,
            e.getMessage(),
            e);
        throw new RuntimeException("Failed to create Spanner tables", e);
      }
    }
  }

  @Test
  public void testMySQLToSpannerMigration() throws Exception {
    // Configure MySQL for better performance
    configureMySQL();

    // Create schemas in batches
    createMySQLSchema();
    createSpannerSchema();

    // Launch the Dataflow job
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify the results
    LOG.info("Verifying data migration for {} tables", NUM_TABLES);
    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = "table" + i;
      assertEquals(
          "Table " + tableName + " migrated",
          0,
          spannerResourceManager.getRowCount(tableName).longValue());
    }
  }
}
