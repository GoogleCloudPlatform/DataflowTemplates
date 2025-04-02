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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @Ignore("SQL instance running inside github runner is crashing")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpanner5000TablePerDBIT extends SourceDbToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLToSpanner5000TablePerDBIT.class);
  private CloudMySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PipelineLauncher.LaunchInfo jobInfo;

  private static final int NUM_TABLES = 5000;
  private static final int BATCH_SIZE = 100; // Number of tables per batch
  private static final int THREAD_POOL_SIZE = 10; // Number of concurrent threads for table creation

  // Retry configuration
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_RETRY_DELAY_MS = 1000; // 1 second
  private static final double BACKOFF_MULTIPLIER = 2.0; // Exponential backoff factor

  @Before
  public void setUp() {
    mySQLResourceManager = setUpCloudMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testMySQLToSpannerMigration() throws Exception {
    // Create tables with data in MySQL and empty tables in Spanner
    createMySQLTables();
    createSpannerTables();

    // Launch the migration job
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    // Wait for job completion
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify tables in Spanner
    verifySpannerTables();
  }

  /**
   * Creates MySQL tables in batches using multiple threads with retry capabilities. Each table will
   * have one record inserted right after creation.
   */
  private void createMySQLTables() throws Exception {
    LOG.info("Creating {} MySQL tables with data in batches", NUM_TABLES);

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    List<Future<?>> futures = new ArrayList<>();

    // Track success and failure counts
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    // Process tables in batches
    for (int batchStart = 0; batchStart < NUM_TABLES; batchStart += BATCH_SIZE) {
      final int start = batchStart;
      final int end = Math.min(start + BATCH_SIZE, NUM_TABLES);

      futures.add(
          executor.submit(
              () -> {
                try {
                  boolean success =
                      withRetry(
                          () -> createMySQLTablesBatch(start, end),
                          String.format("MySQL tables batch %d-%d", start, end));
                  if (success) {
                    successCount.incrementAndGet();
                  } else {
                    failureCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  LOG.error("Fatal error creating MySQL tables batch {}-{}", start, end, e);
                  throw new RuntimeException(e);
                }
              }));
    }

    // Wait for all batches to complete
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.error("Error in table creation batch", e.getCause());
        throw new RuntimeException("Failed to create all MySQL tables", e.getCause());
      }
    }

    executor.shutdown();
    boolean completed = executor.awaitTermination(30, TimeUnit.MINUTES);
    if (!completed) {
      throw new RuntimeException("Timed out while creating MySQL tables");
    }

    LOG.info(
        "MySQL tables creation complete: {} successful batches, {} failed batches",
        successCount.get(),
        failureCount.get());

    // If any batches failed after retries, throw an exception
    if (failureCount.get() > 0) {
      throw new RuntimeException(
          String.format(
              "Failed to create %d batches of MySQL tables after %d retries",
              failureCount.get(), MAX_RETRIES));
    }

    LOG.info("Successfully created {} MySQL tables with data", NUM_TABLES);
  }

  /**
   * Creates a batch of MySQL tables with retry capability. Returns true if successful, false if it
   * failed after all retries.
   */
  private boolean createMySQLTablesBatch(int startIdx, int endIdx) throws Exception {
    StringBuilder batchSql = new StringBuilder();

    // Build batch SQL statement with CREATE TABLE and INSERT
    for (int i = startIdx; i < endIdx; i++) {
      String tableName = getTableName(i);
      batchSql.append(generateMySQLTableDDL(tableName, i));
      batchSql.append("\n");
    }

    try {
      loadSQLToJdbcResourceManager(mySQLResourceManager, batchSql.toString());
      return true;
    } catch (Exception e) {
      LOG.error("Error creating MySQL tables batch {}-{}: {}", startIdx, endIdx, e.getMessage());
      throw e;
    }
  }

  /**
   * Generates DDL for creating a MySQL table and inserting one record.
   *
   * @param tableName The name of the table to create
   * @param index The index of the table (used to generate unique data values)
   * @return SQL statements for both table creation and data insertion
   */
  private String generateMySQLTableDDL(String tableName, int index) {
    String randomValue = UUID.randomUUID().toString().substring(0, 8);

    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ("
            + "  id INT PRIMARY KEY,"
            + "  name VARCHAR(255),"
            + "  value VARCHAR(255),"
            + "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            + ");"
            + "INSERT INTO %s (id, name, value) VALUES (%d, 'name-%d', 'value-%s');",
        tableName, tableName, index + 1, index + 1, randomValue);
  }

  /** Creates Spanner tables in batches with retry capability. */
  private void createSpannerTables() throws Exception {
    LOG.info("Creating {} Spanner tables in batches with retry capability", NUM_TABLES);

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    // Process tables in batches
    for (int batchStart = 0; batchStart < NUM_TABLES; batchStart += BATCH_SIZE) {
      int batchEnd = Math.min(batchStart + BATCH_SIZE, NUM_TABLES);

      int finalBatchStart = batchStart;
      boolean success =
          withRetry(
              () -> createSpannerTablesBatch(finalBatchStart, batchEnd),
              String.format("Spanner tables batch %d-%d", batchStart, batchEnd));

      if (success) {
        successCount.incrementAndGet();
      } else {
        failureCount.incrementAndGet();
      }
    }

    LOG.info(
        "Spanner tables creation complete: {} successful batches, {} failed batches",
        successCount.get(),
        failureCount.get());

    // If any batches failed after retries, throw an exception
    if (failureCount.get() > 0) {
      throw new RuntimeException(
          String.format(
              "Failed to create %d batches of Spanner tables after %d retries",
              failureCount.get(), MAX_RETRIES));
    }

    LOG.info("Successfully created {} Spanner tables", NUM_TABLES);
  }

  /** Creates a batch of Spanner tables. Returns true if successful. */
  private boolean createSpannerTablesBatch(int startIdx, int endIdx) {
    List<String> batchDdls = new ArrayList<>();

    for (int i = startIdx; i < endIdx; i++) {
      String tableName = getTableName(i);
      batchDdls.add(generateSpannerTableDDL(tableName));
    }

    try {
      spannerResourceManager.executeDdlStatements(batchDdls);
      return true;
    } catch (Exception e) {
      LOG.error("Error creating Spanner tables batch {}-{}: {}", startIdx, endIdx, e.getMessage());
      throw e;
    }
  }

  /** Generates DDL for creating a Spanner table. */
  private String generateSpannerTableDDL(String tableName) {
    return String.format(
        "CREATE TABLE %s ("
            + "  id INT64 NOT NULL,"
            + "  name STRING(255),"
            + "  value STRING(255),"
            + "  created_at TIMESTAMP,"
            + "  ) PRIMARY KEY(id)",
        tableName);
  }

  /**
   * Generic retry mechanism that attempts an operation up to MAX_RETRIES times with exponential
   * backoff.
   *
   * @param operation The operation to retry
   * @param operationName Name of the operation for logging
   * @return true if the operation succeeded, false if it failed after all retries
   */
  private <T> boolean withRetry(Callable<T> operation, String operationName) {
    int attempts = 0;
    long delay = INITIAL_RETRY_DELAY_MS;

    while (attempts < MAX_RETRIES) {
      attempts++;
      try {
        LOG.info("Attempting {} (attempt {}/{})", operationName, attempts, MAX_RETRIES);
        operation.call();
        LOG.info("Successfully completed {} on attempt {}", operationName, attempts);
        return true;
      } catch (Exception e) {
        if (attempts >= MAX_RETRIES) {
          LOG.error(
              "Operation {} failed after {} attempts. Last error: {}",
              operationName,
              MAX_RETRIES,
              e.getMessage());
          return false;
        }

        LOG.warn(
            "Operation {} failed on attempt {}/{}. Error: {}. Retrying in {} ms...",
            operationName,
            attempts,
            MAX_RETRIES,
            e.getMessage(),
            delay);

        try {
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOG.error("Retry interrupted for operation {}", operationName);
          return false;
        }

        // Apply exponential backoff for next retry
        delay = (long) (delay * BACKOFF_MULTIPLIER);
      }
    }

    return false;
  }

  /** Verifies all tables were created in Spanner and have 1 row each (migrated from MySQL). */
  private void verifySpannerTables() {
    LOG.info("Verifying {} Spanner tables", NUM_TABLES);

    // Track verification statistics
    AtomicInteger verifiedCount = new AtomicInteger(0);
    AtomicInteger failedCount = new AtomicInteger(0);

    IntStream.range(0, NUM_TABLES)
        .forEach(
            i -> {
              String tableName = getTableName(i);
              try {
                assertEquals(
                    String.format("Table %s should have exactly 1 row", tableName),
                    1,
                    spannerResourceManager.getRowCount(tableName).longValue());
                verifiedCount.incrementAndGet();
              } catch (Exception e) {
                LOG.error("Failed to verify table {}: {}", tableName, e.getMessage());
                failedCount.incrementAndGet();
              }
            });

    LOG.info(
        "Spanner table verification complete: {} verified, {} failed",
        verifiedCount.get(),
        failedCount.get());

    if (failedCount.get() > 0) {
      throw new AssertionError(
          String.format("Failed to verify %d Spanner tables", failedCount.get()));
    }
  }

  /** Returns the table name for the given index. */
  private String getTableName(int index) {
    return "table" + (index + 1);
  }
}
