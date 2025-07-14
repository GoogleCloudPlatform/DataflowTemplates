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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link SourceSchemaScanner}. */
public class MySqlInformationSchemaScanner implements SourceSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlInformationSchemaScanner.class);

  private final Connection connection;
  private final String databaseName;
  private final SourceDatabaseType sourceType = SourceDatabaseType.MYSQL;

  public MySqlInformationSchemaScanner(Connection connection, String databaseName) {
    this.connection = connection;
    this.databaseName = databaseName;
  }

  @Override
  public SourceSchema scan() {
    SourceSchema.Builder schemaBuilder =
        SourceSchema.builder(sourceType).databaseName(databaseName);

    try {
      Map<String, SourceTable> tables = scanTables();
      schemaBuilder.tables(com.google.common.collect.ImmutableMap.copyOf(tables));
    } catch (SQLException e) {
      throw new RuntimeException("Error scanning database schema", e);
    }

    return schemaBuilder.build();
  }

  private Map<String, SourceTable> scanTables() throws SQLException {
    Map<String, SourceTable> tables = new java.util.HashMap<>();
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(getTablesQuery())) {
      while (rs.next()) {
        String tableName = rs.getString(1);
        String schema = rs.getString(2);
        if (tableName == null) {
          continue;
        }
        SourceTable table = scanTable(tableName, schema);
        tables.put(tableName, table);
      }
    }
    return tables;
  }

  private SourceTable scanTable(String tableName, String schema) throws SQLException {
    SourceTable.Builder tableBuilder =
        SourceTable.builder(sourceType).name(tableName).schema(schema);

    // Scan columns
    List<SourceColumn> columns = scanColumns(tableName, schema);
    tableBuilder.columns(com.google.common.collect.ImmutableList.copyOf(columns));

    // Scan primary keys
    List<String> primaryKeys = scanPrimaryKeys(tableName, schema);
    tableBuilder.primaryKeyColumns(com.google.common.collect.ImmutableList.copyOf(primaryKeys));

    return tableBuilder.build();
  }

  private String getTablesQuery() {
    return String.format(
        "SELECT table_name, table_schema "
            + "FROM information_schema.tables "
            + "WHERE table_schema = '%s' "
            + "AND table_type = 'BASE TABLE'",
        databaseName);
  }

  private List<SourceColumn> scanColumns(String tableName, String schema) throws SQLException {
    List<SourceColumn> columns = new ArrayList<>();
    String query =
        String.format(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = '%s' AND table_name = '%s' "
                + "ORDER BY ordinal_position",
            schema, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        SourceColumn.Builder columnBuilder =
            SourceColumn.builder(sourceType)
                .name(rs.getString("column_name"))
                .type(rs.getString("data_type"))
                .isNullable("YES".equals(rs.getString("is_nullable")))
                .isPrimaryKey("PRI".equals(rs.getString("column_key")));

        // Handle size/precision/scale
        String maxLength = rs.getString("character_maximum_length");
        if (maxLength != null) {
          columnBuilder.size(Long.parseLong(maxLength));
        }

        String precision = rs.getString("numeric_precision");
        if (precision != null) {
          columnBuilder.precision(Integer.parseInt(precision));
        }

        String scale = rs.getString("numeric_scale");
        if (scale != null) {
          columnBuilder.scale(Integer.parseInt(scale));
        }

        columnBuilder.columnOptions(ImmutableList.of());
        columns.add(columnBuilder.build());
      }
    }
    return columns;
  }

  private List<String> scanPrimaryKeys(String tableName, String schema) throws SQLException {
    List<String> primaryKeys = new ArrayList<>();
    String query =
        String.format(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = '%s' AND table_name = '%s' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position",
            schema, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        primaryKeys.add(rs.getString("column_name"));
      }
    }
    return primaryKeys;
  }
}
