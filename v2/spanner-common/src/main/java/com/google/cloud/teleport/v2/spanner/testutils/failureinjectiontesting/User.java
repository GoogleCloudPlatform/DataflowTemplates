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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;

public class User {
  int id;
  String firstName;
  String lastName;
  int age;
  boolean status;
  long col1;
  long col2;

  public static final String ID = "id";
  public static final String FIRST_NAME = "first_name";
  public static final String LAST_NAME = "last_name";
  public static final String AGE = "age";
  public static final String STATUS = "status";
  public static final String COL1 = "col1";
  public static final String COL2 = "col2";
  public static final List<String> UPDATABLE_COLUMNS =
      ImmutableList.of(FIRST_NAME, LAST_NAME, AGE, STATUS, COL1, COL2);

  private static final String INSERT_SQL =
      "INSERT INTO `Users` (id, first_name, last_name, age, status, col1, col2) VALUES (?, ?, ?, ?, ?, ?, ?)";
  private static final String DELETE_SQL = "DELETE FROM `Users` WHERE id=?";
  private static final String SELECT_ALL_SQL =
      "SELECT id, first_name, last_name, age, status, col1, col2 FROM Users";

  void insert(Connection conn) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
      ps.setInt(1, this.id);
      ps.setString(2, this.firstName);
      ps.setString(3, this.lastName);
      ps.setInt(4, this.age);
      ps.setInt(5, this.status ? 1 : 0); // tinyint mapping
      ps.setLong(6, this.col1);
      ps.setLong(7, this.col2);
      ps.executeUpdate();
    }
  }

  void update(Connection conn, Random random) throws SQLException {
    // Randomly select a column to update.
    String columnToUpdate =
        User.UPDATABLE_COLUMNS.get(random.nextInt(User.UPDATABLE_COLUMNS.size()));

    String sql = "UPDATE `Users` SET " + columnToUpdate + " = ? WHERE id = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      switch (columnToUpdate) {
        case User.FIRST_NAME:
          ps.setString(1, this.firstName);
          break;
        case User.LAST_NAME:
          ps.setString(1, this.lastName);
          break;
        case User.AGE:
          ps.setInt(1, this.age);
          break;
        case User.STATUS:
          ps.setInt(1, this.status ? 1 : 0);
          break;
        case User.COL1:
          ps.setLong(1, this.col1);
          break;
        case User.COL2:
          ps.setLong(1, this.col2);
          break;
      }
      ps.setInt(2, this.id);
      ps.executeUpdate();
    }
  }

  void delete(Connection conn) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
      ps.setInt(1, id);
      ps.executeUpdate();
    }
  }

  public static Map<Integer, User> fetchAll(AbstractJDBCResourceManager resourceManager)
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
        u.id = rs.getInt(User.ID);
        u.firstName = rs.getString(User.FIRST_NAME);
        u.lastName = rs.getString(User.LAST_NAME);
        u.age = rs.getInt(User.AGE);
        u.status = rs.getInt(User.STATUS) == 1; // tinyint mapping
        u.col1 = rs.getLong(User.COL1);
        u.col2 = rs.getLong(User.COL2);
        users.put(u.id, u);
      }
    }
    return users;
  }

  public static Map<Integer, User> fetchAll(SpannerResourceManager spannerResourceManager) {
    ImmutableList<com.google.cloud.spanner.Struct> result =
        spannerResourceManager.runQuery(SELECT_ALL_SQL);
    Map<Integer, User> users = new HashMap<>();

    for (com.google.cloud.spanner.Struct rs : result) {
      User u = new User();
      u.id = (int) rs.getLong(User.ID);
      u.firstName = rs.isNull(User.FIRST_NAME) ? null : rs.getString(User.FIRST_NAME);
      u.lastName = rs.isNull(User.LAST_NAME) ? null : rs.getString(User.LAST_NAME);
      u.age = rs.isNull(User.AGE) ? 0 : (int) rs.getLong(User.AGE);
      u.status = !rs.isNull(User.STATUS) && rs.getBoolean(User.STATUS);
      u.col1 = rs.isNull(User.COL1) ? 0 : rs.getLong(User.COL1);
      u.col2 = rs.isNull(User.COL2) ? 0 : rs.getLong(User.COL2);
      users.put(u.id, u);
    }
    return users;
  }

  static User generateRandom() {
    User u = new User();
    u.id = ThreadLocalRandom.current().nextInt(2_000_000_000);
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
