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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.DatabaseClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for generating fuzzy CDC load against a database. The class also provides a
 * method to assert that the data in the MySQL database exactly matches the data in a Spanner
 * database, which is crucial for verifying CDC pipeline correctness.
 *
 * <p>Note: This class is to be used in tests only and should not be used in production.
 */
public class FuzzyCDCLoadGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(FuzzyCDCLoadGenerator.class);

  private final Map<Integer, Boolean> ids = new ConcurrentHashMap<>();
  private Random random;
  // create a private automic integer variable
  private java.util.concurrent.atomic.AtomicInteger totalEventCount =
      new java.util.concurrent.atomic.AtomicInteger(0);

  public FuzzyCDCLoadGenerator() {}

  /** Constructor for testing purposes. */
  FuzzyCDCLoadGenerator(Random random) {
    this.random = random;
  }

  Random getRandom() {
    if (random == null) {
      return ThreadLocalRandom.current();
    }
    return random;
  }

  /** Generate rows and apply random choices using threads. */
  public int generateLoad(
      int totalRows,
      int burstIterations,
      double updateProbability,
      String jdbcUrl,
      String username,
      String password) {
    LOG.info("Creating a thread pool with {} threads.", totalRows);
    ExecutorService executor = Executors.newFixedThreadPool(totalRows);
    try {
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < totalRows; i++) {
        futures.add(
            executor.submit(
                () -> {
                  // Each thread gets its own connection to ensure thread safety
                  try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
                    processSingleRow(conn, burstIterations, updateProbability);
                  } catch (SQLException e) {
                    LOG.error("Thread processing failed", e);
                    throw new RuntimeException(e);
                  }
                }));
      }

      // Wait for all threads to finish
      for (Future<?> future : futures) {
        future.get(2, TimeUnit.HOURS); // Wait for each task to complete
      }
    } catch (Exception e) {
      LOG.error("Task execution failed", e);
      if (e instanceof InterruptedException) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Task execution failed", e);
    } finally {
      executor.shutdown();
    }
    LOG.info("Created {} number of events in the database.", totalEventCount.get());
    return totalEventCount.get();
  }

  /** Overloaded generateLoad for use in integration tests with a resource manager. */
  public int generateLoad(
      int totalRows,
      int burstIterations,
      double updateProbability,
      CloudSqlResourceManager resourceManager) {
    return generateLoad(
        totalRows,
        burstIterations,
        updateProbability,
        resourceManager.getUri(),
        resourceManager.getUsername(),
        resourceManager.getPassword());
  }

  /** Process a single row: Insert -> Burst Operation. */
  private void processSingleRow(Connection conn, int burstIterations, double updateProbability)
      throws SQLException {
    // 1. Create Unique Random ID
    int id;
    User user;
    do {
      user = User.generateRandom();
      id = user.id;
    } while (ids.putIfAbsent(id, true) != null);

    // 2. Insert initial row
    user.insert(conn);
    totalEventCount.incrementAndGet();

    for (int i = 0; i < burstIterations; ++i) {
      // Perform update with updateProbability or delete and insert the row otherwise.
      if (getRandom().nextDouble() < updateProbability) {
        // Update in memory
        user.mutateRandomly();
        // Update in DB
        user.update(conn, getRandom());
        totalEventCount.incrementAndGet();
      } else {
        user.delete(conn);
        totalEventCount.incrementAndGet();

        // Insert the row
        user.mutateRandomly();
        user.insert(conn);
        totalEventCount.incrementAndGet();
        ++i; // increment the counter to account for the insert operation.
      }
    }
  }

  /** Overloaded generateLoad to generate load on Spanner. */
  public int generateLoad(
      int totalRows,
      int burstIterations,
      double updateProbability,
      SpannerResourceManager spannerResourceManager) {
    LOG.info("Creating a thread pool with {} threads.", totalRows);
    ExecutorService executor = Executors.newFixedThreadPool(totalRows);
    DatabaseClient databaseClient = spannerResourceManager.getDatabaseClient();
    try {
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < totalRows; i++) {
        futures.add(
            executor.submit(
                () -> {
                  // Each thread gets its own connection to ensure thread safety
                  try {
                    processSingleRow(databaseClient, burstIterations, updateProbability);
                  } catch (Exception e) {
                    LOG.error("Thread processing failed", e);
                    throw new RuntimeException(e);
                  }
                }));
      }

      // Wait for all threads to finish
      for (Future<?> future : futures) {
        future.get(2, TimeUnit.HOURS); // Wait for each task to complete
      }
    } catch (Exception e) {
      LOG.error("Task execution failed", e);
      if (e instanceof InterruptedException) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Task execution failed", e);
    } finally {
      executor.shutdown();
    }
    return totalEventCount.get();
  }

  private void processSingleRow(
      DatabaseClient databaseClient, int burstIterations, double updateProbability) {
    int id;
    User user;
    do {
      user = User.generateRandom();
      id = user.id;
    } while (ids.putIfAbsent(id, true) != null);

    // 2. Insert initial row
    user.insert(databaseClient);
    totalEventCount.incrementAndGet();

    for (int i = 0; i < burstIterations; ++i) {
      // Perform update with updateProbability or delete and insert the row otherwise.
      if (getRandom().nextDouble() < updateProbability) {
        // Update in memory
        user.mutateRandomly();
        // Update in DB
        user.update(databaseClient);
        totalEventCount.incrementAndGet();
      } else {
        user.delete(databaseClient);
        totalEventCount.incrementAndGet();

        // Insert the row
        user.mutateRandomly();
        user.insert(databaseClient);
        totalEventCount.incrementAndGet();
        ++i; // increment the counter to account for the insert operation.
      }
    }
  }

  /* Fetch all rows from MySQL and Spanner and assert that they match exactly. */
  public void assertRows(
      SpannerResourceManager spannerResourceManager,
      CloudSqlResourceManager sourceDBResourceManager) {
    Map<Integer, User> sourceUsers;
    Map<Integer, User> spannerUsers;
    try {
      sourceUsers = User.fetchAll(sourceDBResourceManager);
      spannerUsers = User.fetchAll(spannerResourceManager);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    int missingIdCount = 0;
    // Log missing/extra rows for debugging
    for (Map.Entry<Integer, User> entry : sourceUsers.entrySet()) {
      Integer id = entry.getKey();
      User user = entry.getValue();
      if (!spannerUsers.containsKey(id)) {
        LOG.error("Row with ID {} present in Source but not in Spanner: {}", id, user);
        missingIdCount++;
      }
    }
    for (Map.Entry<Integer, User> entry : spannerUsers.entrySet()) {
      Integer id = entry.getKey();
      User user = entry.getValue();
      if (!sourceUsers.containsKey(id)) {
        LOG.error("Row with ID {} present in Spanner but not in Source: {}", id, user);
        missingIdCount++;
      }
    }
    assertThat(missingIdCount).isEqualTo(0);
    // If missingIdCount is zero then number of rows in Spanner and Source are same. Hence, no need
    // to assert number of rows are equal.

    // Assert that each row is identical
    for (Map.Entry<Integer, User> entry : sourceUsers.entrySet()) {
      Integer id = entry.getKey();
      User sourceUser = entry.getValue();
      User spannerUser = spannerUsers.get(id);

      if (!sourceUser.equals(spannerUser)) {
        LOG.error("Mismatch for user ID {}: Source={}, Spanner={}", id, sourceUser, spannerUser);
      }
      assertThat(spannerUser).isEqualTo(sourceUser);
    }
  }
}
