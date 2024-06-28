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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;

class TransformTestUtils {

  static final DataSourceConfiguration DATA_SOURCE_CONFIGURATION =
      DataSourceConfiguration.create(
          "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true");
  static final DataSource DATA_SOURCE = DATA_SOURCE_CONFIGURATION.buildDatasource();

  private TransformTestUtils() {}

  static void createDerbyTable(String tableName) throws SQLException {
    try (java.sql.Connection connection = getConnection()) {
      Statement stmtCreateTable = connection.createStatement();
      String createTableSQL =
          "CREATE TABLE "
              + tableName
              + " ("
              + "col1 INT,"
              + "col2 INT,"
              + "data VARCHAR(20),"
              + "PRIMARY KEY (col1, col2)"
              + ")";
      stmtCreateTable.executeUpdate(createTableSQL);

      // 2.2 Insert Data (Using PreparedStatement for Efficiency & Security)
      String insertSQL = "INSERT INTO " + tableName + " (col1, col2, data) VALUES (?, ?, ?)";
      PreparedStatement stmtInsert = connection.prepareStatement(insertSQL);

      // Batch the insert operations
      stmtInsert.setInt(1, 10);
      stmtInsert.setInt(2, 30);
      stmtInsert.setString(3, "Data A");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 15);
      stmtInsert.setInt(2, 35);
      stmtInsert.setString(3, "Data B");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 25);
      stmtInsert.setInt(2, 50);
      stmtInsert.setString(3, "Data C");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 30);
      stmtInsert.setInt(2, 60);
      stmtInsert.setString(3, "Data D");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 40);
      stmtInsert.setInt(2, 70);
      stmtInsert.setString(3, "Data E");
      stmtInsert.addBatch();

      stmtInsert.setInt(1, 40);
      stmtInsert.setInt(2, 80);
      stmtInsert.setString(3, "Data F");
      stmtInsert.addBatch();

      stmtInsert.executeBatch();
    }
  }

  static void dropDerbyTable(String tableName) throws SQLException {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.executeUpdate("drop table " + tableName);
    }
  }

  static Connection getConnection() throws SQLException {
    return DATA_SOURCE.getConnection();
  }
}
