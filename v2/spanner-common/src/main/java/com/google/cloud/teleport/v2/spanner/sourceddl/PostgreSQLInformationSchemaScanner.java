/*
 * Copyright (C) 2026 Google LLC
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

/** PostgreSQL implementation of {@link SourceSchemaScanner}. */
public class PostgreSQLInformationSchemaScanner implements SourceSchemaScanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLInformationSchemaScanner.class);

  private final Connection connection;
  private final String databaseName;
  private final SourceDatabaseType sourceType = SourceDatabaseType.POSTGRESQL;

  // Schema name is 'public' by default for PostgreSQL, unless specified otherwise
  private final String schemaSearchPath;

  public PostgreSQLInformationSchemaScanner(Connection connection, String databaseName) {
    this.connection = connection;
    this.databaseName = databaseName;
    this.schemaSearchPath = "public";
  }

  public PostgreSQLInformationSchemaScanner(
      Connection connection, String databaseName, String schemaSearchPath) {
    this.connection = connection;
    this.databaseName = databaseName;
    this.schemaSearchPath =
        (schemaSearchPath != null && !schemaSearchPath.isBlank()) ? schemaSearchPath : "public";
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

    // Scan indexes
    List<SourceIndex> indexes = scanIndexes(tableName, schema);
    tableBuilder.indexes(com.google.common.collect.ImmutableList.copyOf(indexes));

    // Scan foreign keys
    List<SourceForeignKey> foreignKeys = scanForeignKeys(tableName, schema);
    tableBuilder.foreignKeys(com.google.common.collect.ImmutableList.copyOf(foreignKeys));

    return tableBuilder.build();
  }

  private String getTablesQuery() {
    return String.format(
        "SELECT table_name, table_schema "
            + "FROM information_schema.tables "
            + "WHERE table_schema = '%s' "
            + "AND table_type = 'BASE TABLE'",
        schemaSearchPath);
  }

  private List<SourceColumn> scanColumns(String tableName, String schema) throws SQLException {
    List<SourceColumn> columns = new ArrayList<>();
    // PostgreSQL uses numeric_precision, numeric_scale, character_maximum_length, etc.
    String query =
        String.format(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, is_generated "
                + "FROM information_schema.columns "
                + "WHERE table_schema = '%s' AND table_name = '%s' "
                + "ORDER BY ordinal_position",
            schemaSearchPath, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        SourceColumn.Builder columnBuilder =
            SourceColumn.builder(sourceType)
                .name(rs.getString("column_name"))
                .type(rs.getString("data_type"))
                .isNullable("YES".equals(rs.getString("is_nullable")));

        // We will set the primary key separately via the constraints.
        // PostgreSQL information_schema.columns does not have column_key enum.
        columnBuilder.isPrimaryKey(false);

        String isGenerated = rs.getString("is_generated"); // usually 'ALWAYS', 'NEVER'
        columnBuilder.isGenerated("ALWAYS".equals(isGenerated));

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
            "SELECT kcu.column_name "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "  ON tc.constraint_name = kcu.constraint_name "
                + "  AND tc.table_schema = kcu.table_schema "
                + "WHERE tc.table_schema = '%s' AND tc.table_name = '%s' "
                + "AND tc.constraint_type = 'PRIMARY KEY' "
                + "ORDER BY kcu.ordinal_position",
            schemaSearchPath, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        primaryKeys.add(rs.getString("column_name"));
      }
    }
    return primaryKeys;
  }

  private List<SourceIndex> scanIndexes(String tableName, String schema) throws SQLException {
    Map<String, SourceIndex.Builder> indexBuilders = new java.util.HashMap<>();
    Map<String, List<String>> indexColumns = new java.util.HashMap<>();

    // Postgres specific way to get indexes, since information_schema does not have 'statistics'
    String query =
        String.format(
            "SELECT i.relname AS index_name, "
                + "a.attname AS column_name, "
                + "ix.indisunique AS is_unique, "
                + "ix.indisprimary AS is_primary "
                + "FROM pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n "
                + "WHERE t.oid = ix.indrelid "
                + "  AND i.oid = ix.indexrelid "
                + "  AND a.attrelid = t.oid "
                + "  AND a.attnum = ANY(ix.indkey) "
                + "  AND t.relnamespace = n.oid "
                + "  AND n.nspname = '%s' "
                + "  AND t.relkind = 'r' "
                + "  AND t.relname = '%s' "
                + "  AND ix.indisprimary = false "
                + "ORDER BY t.relname, i.relname, a.attnum",
            schemaSearchPath, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        String indexName = rs.getString("index_name");
        String columnName = rs.getString("column_name");
        boolean isUnique = rs.getBoolean("is_unique");

        indexBuilders.computeIfAbsent(
            indexName,
            k ->
                SourceIndex.builder()
                    .name(k)
                    .tableName(tableName)
                    .isUnique(isUnique)
                    .isPrimary(false));

        indexColumns.computeIfAbsent(indexName, k -> new ArrayList<>()).add(columnName);
      }
    }

    List<SourceIndex> indexes = new ArrayList<>();
    for (Map.Entry<String, SourceIndex.Builder> entry : indexBuilders.entrySet()) {
      indexes.add(
          entry
              .getValue()
              .columns(
                  com.google.common.collect.ImmutableList.copyOf(indexColumns.get(entry.getKey())))
              .build());
    }
    return indexes;
  }

  private List<SourceForeignKey> scanForeignKeys(String tableName, String schema)
      throws SQLException {
    Map<String, SourceForeignKey.Builder> fkBuilders = new java.util.HashMap<>();
    Map<String, List<String>> keyColsMap = new java.util.HashMap<>();
    Map<String, List<String>> refColsMap = new java.util.HashMap<>();

    // PostgreSQL constraints table
    String query =
        String.format(
            "SELECT tc.constraint_name, tc.table_name, kcu.column_name, "
                + "ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name "
                + "FROM information_schema.table_constraints AS tc "
                + "JOIN information_schema.key_column_usage AS kcu "
                + "  ON tc.constraint_name = kcu.constraint_name "
                + "  AND tc.table_schema = kcu.table_schema "
                + "JOIN information_schema.constraint_column_usage AS ccu "
                + "  ON ccu.constraint_name = tc.constraint_name "
                + "  AND ccu.table_schema = tc.table_schema "
                + "WHERE tc.constraint_type = 'FOREIGN KEY' "
                + "  AND tc.table_schema = '%s' AND tc.table_name = '%s' "
                + "ORDER BY tc.constraint_name, kcu.ordinal_position",
            schemaSearchPath, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        String constraintName = rs.getString("constraint_name");
        String columnName = rs.getString("column_name");
        String referencedTable = rs.getString("referenced_table_name");
        String referencedColumn = rs.getString("referenced_column_name");

        if (!fkBuilders.containsKey(constraintName)) {
          fkBuilders.put(
              constraintName,
              SourceForeignKey.builder()
                  .name(constraintName)
                  .tableName(tableName)
                  .referencedTable(referencedTable));
          keyColsMap.put(constraintName, new ArrayList<>());
          refColsMap.put(constraintName, new ArrayList<>());
        }

        keyColsMap.get(constraintName).add(columnName);
        refColsMap.get(constraintName).add(referencedColumn);
      }
    }

    List<SourceForeignKey> foreignKeys = new ArrayList<>();
    for (Map.Entry<String, SourceForeignKey.Builder> entry : fkBuilders.entrySet()) {
      String fkName = entry.getKey();
      foreignKeys.add(
          entry
              .getValue()
              .keyColumns(com.google.common.collect.ImmutableList.copyOf(keyColsMap.get(fkName)))
              .referencedColumns(
                  com.google.common.collect.ImmutableList.copyOf(refColsMap.get(fkName)))
              .build());
    }
    return foreignKeys;
  }
}
