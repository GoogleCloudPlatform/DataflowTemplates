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

public class FuzzyCDCLoadGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(FuzzyCDCLoadGenerator.class);

  private final Map<Integer, Boolean> ids = new ConcurrentHashMap<>();
  private Random random;
  // create a private automic integer variable
  private java.util.concurrent.atomic.AtomicInteger counter =
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
      int totalRows, int burstIterations, String jdbcUrl, String username, String password) {
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
                    processSingleRow(conn, burstIterations);
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
    LOG.info("Created {} number of events in the database.", counter.get());
    return counter.get();
  }

  /** Overloaded generateLoad for use in integration tests with a resource manager. */
  public int generateLoad(
      int totalRows, int burstIterations, CloudSqlResourceManager resourceManager) {
    return generateLoad(
        totalRows,
        burstIterations,
        resourceManager.getUri(),
        resourceManager.getUsername(),
        resourceManager.getPassword());
  }

  /** Process a single row: Insert -> Burst Operation. */
  private void processSingleRow(Connection conn, int burstIterations) throws SQLException {
    // 1. Create Unique Random ID
    int id;
    User user;
    do {
      id = getRandom().nextInt(2_000_000_000);
      user = User.generateRandom(id);
    } while (ids.putIfAbsent(id, true) != null);

    // 2. Insert initial row
    user.insert(conn);
    counter.incrementAndGet();
    boolean isRowPresent = true;

    for (int i = 0; i < burstIterations; ++i) {
      // If row is present, then update the row with 75% probability and delete the row with 25%
      // probability
      if (isRowPresent) {
        if (getRandom().nextDouble() < 0.75) {
          // Update in memory
          user.mutateRandomly();
          // Update in DB
          user.update(conn, getRandom());
          counter.incrementAndGet();
        } else {
          user.delete(conn);
          isRowPresent = false;
          counter.incrementAndGet();
        }
      } else {
        // If row is not present, then insert the row with 75% probability and do nothing with 25%
        // probability
        if (getRandom().nextDouble() < 0.75) {
          // Insert the row
          user.mutateRandomly();
          user.insert(conn);
          isRowPresent = true;
          counter.incrementAndGet();
        }
        // Else: Do nothing (row remains absent)
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
