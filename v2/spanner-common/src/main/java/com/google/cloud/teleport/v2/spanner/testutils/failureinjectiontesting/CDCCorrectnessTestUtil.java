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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCCorrectnessTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CDCCorrectnessTestUtil.class);

  private final Map<Integer, Boolean> ids = new ConcurrentHashMap<>();
  private Random random;
  // create a private automic integer variable
  private java.util.concurrent.atomic.AtomicInteger counter =
      new java.util.concurrent.atomic.AtomicInteger(0);

  public CDCCorrectnessTestUtil() {}

  /** Constructor for testing purposes. */
  CDCCorrectnessTestUtil(Random random) {
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
      id = getRandom().nextInt(1000000);
      user = User.generateRandom(id);
    } while (ids.putIfAbsent(id, true) != null);

    // 2. Insert initial row
    insertUser(conn, user);
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
          updateUser(conn, user);
          counter.incrementAndGet();
        } else {
          deleteUser(conn, user.id);
          isRowPresent = false;
          counter.incrementAndGet();
        }
      } else {
        // If row is not present, then insert the row with 75% probability and do nothing with 25%
        // probability
        if (getRandom().nextDouble() < 0.75) {
          // Insert the row
          user.mutateRandomly();
          insertUser(conn, user);
          isRowPresent = true;
          counter.incrementAndGet();
        }
        // Else: Do nothing (row remains absent)
      }
    }
    System.out.print(".");
  }

  public void performInserts(int numRows, AbstractJDBCResourceManager resourceManager)
      throws SQLException {

    try (Connection conn =
        DriverManager.getConnection(
            resourceManager.getUri(),
            resourceManager.getUsername(),
            resourceManager.getPassword())) {
      for (int k = 0; k < numRows; k++) {
        // 1. Create Unique Random ID
        int id;
        User user;
        do {
          id = getRandom().nextInt(1000000);
          user = User.generateRandom(id);
        } while (ids.putIfAbsent(id, true) != null);

        // 2. row
        insertUser(conn, user);
      }
    } catch (SQLException e) {
      LOG.error("Connection to SourceDb failed", e);
      throw e;
    }
  }

  private static final String INSERT_SQL =
      "INSERT INTO `Users` (id, first_name, last_name, age, status, col1, col2) VALUES (?, ?, ?, ?, ?, ?, ?)";
  private static final String DELETE_SQL = "DELETE FROM `Users` WHERE id=?";
  private static final String SELECT_ALL_SQL =
      "SELECT id, first_name, last_name, age, status, col1, col2 FROM Users";

  void insertUser(Connection conn, User u) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
      ps.setInt(1, u.id);
      ps.setString(2, u.firstName);
      ps.setString(3, u.lastName);
      ps.setInt(4, u.age);
      ps.setInt(5, u.status ? 1 : 0); // tinyint mapping
      ps.setLong(6, u.col1);
      ps.setLong(7, u.col2);
      ps.executeUpdate();
    }
  }

  void updateUser(Connection conn, User u) throws SQLException {
    // Randomly select a column to update.
    String columnToUpdate =
        User.UPDATABLE_COLUMNS.get(getRandom().nextInt(User.UPDATABLE_COLUMNS.size()));

    String sql = "UPDATE `Users` SET " + columnToUpdate + " = ? WHERE id = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      switch (columnToUpdate) {
        case "first_name":
          ps.setString(1, u.firstName);
          break;
        case "last_name":
          ps.setString(1, u.lastName);
          break;
        case "age":
          ps.setInt(1, u.age);
          break;
        case "status":
          ps.setInt(1, u.status ? 1 : 0);
          break;
        case "col1":
          ps.setLong(1, u.col1);
          break;
        case "col2":
          ps.setLong(1, u.col2);
          break;
      }
      ps.setInt(2, u.id);
      ps.executeUpdate();
    }
  }

  void deleteUser(Connection conn, int id) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
      ps.setInt(1, id);
      ps.executeUpdate();
    }
  }

  Map<Integer, User> fetchAllUsers(AbstractJDBCResourceManager resourceManager)
      throws SQLException {
    Map<Integer, User> users = new HashMap<>();
    try (Connection conn =
        DriverManager.getConnection(
            resourceManager.getUri(),
            resourceManager.getUsername(),
            resourceManager.getPassword())) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(SELECT_ALL_SQL);
      while (rs.next()) {
        User u = new User();
        u.id = rs.getInt("id");
        u.firstName = rs.getString("first_name");
        u.lastName = rs.getString("last_name");
        u.age = rs.getInt("age");
        u.status = rs.getInt("status") == 1; // tinyint mapping
        u.col1 = rs.getLong("col1");
        u.col2 = rs.getLong("col2");
        users.put(u.id, u);
      }
    }
    return users;
  }

  Map<Integer, User> fetchAllUsers(SpannerResourceManager spannerResourceManager) {
    ImmutableList<com.google.cloud.spanner.Struct> result =
        spannerResourceManager.runQuery(SELECT_ALL_SQL);
    Map<Integer, User> users = new HashMap<>();

    for (com.google.cloud.spanner.Struct rs : result) {
      User u = new User();
      u.id = (int) rs.getLong("id");
      u.firstName = rs.isNull("first_name") ? null : rs.getString("first_name");
      u.lastName = rs.isNull("last_name") ? null : rs.getString("last_name");
      u.age = rs.isNull("age") ? 0 : (int) rs.getLong("age");
      u.status = !rs.isNull("status") && rs.getBoolean("status");
      u.col1 = rs.isNull("col1") ? 0 : rs.getLong("col1");
      u.col2 = rs.isNull("col2") ? 0 : rs.getLong("col2");
      users.put(u.id, u);
    }
    return users;
  }

  /* Fetch all rows from MySQL and Spanner and assert that they match exactly. */
  public void assertRows(
      SpannerResourceManager spannerResourceManager,
      CloudSqlResourceManager sourceDBResourceManager) {
    Map<Integer, User> sourceUsers;
    Map<Integer, User> spannerUsers;
    try {
      sourceUsers = fetchAllUsers(sourceDBResourceManager);
      spannerUsers = fetchAllUsers(spannerResourceManager);
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

  /** User POJO with random generation capability. */
  static class User {
    int id;
    String firstName;
    String lastName;
    int age;
    boolean status;
    long col1;
    long col2;

    static final List<String> UPDATABLE_COLUMNS =
        ImmutableList.of("first_name", "last_name", "age", "status", "col1", "col2");

    static User generateRandom(int id) {
      User u = new User();
      u.id = id;
      u.mutateRandomly();
      return u;
    }

    void mutateRandomly() {
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      this.firstName = "Fname_" + rnd.nextInt(10000);
      this.lastName = "Lname_" + rnd.nextInt(10000);
      this.age = rnd.nextInt(100);
      this.status = rnd.nextBoolean();
      this.col1 = rnd.nextLong();
      this.col2 = rnd.nextLong();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      User user = (User) o;
      return id == user.id
          && age == user.age
          && status == user.status
          && col1 == user.col1
          && col2 == user.col2
          && Objects.equals(firstName, user.firstName)
          && Objects.equals(lastName, user.lastName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, firstName, lastName, age, status, col1, col2);
    }

    @Override
    public String toString() {
      return String.format(
          "User{id=%d, fn='%s', age=%d, status=%s, col1=%d}", id, firstName, age, status, col1);
    }
  }
}
