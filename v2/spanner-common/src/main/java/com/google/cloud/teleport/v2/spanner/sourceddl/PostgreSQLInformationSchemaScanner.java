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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

  private final String schema;

  public PostgreSQLInformationSchemaScanner(
      Connection connection, String databaseName, String schema) {
    this.connection = connection;
    this.databaseName = databaseName;
    // Schema name is 'public' by default for PostgreSQL, unless specified otherwise
    this.schema = (schema != null && !schema.isBlank()) ? schema : "public";
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
    String query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schema);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String tableName = rs.getString(1);
          if (tableName == null) {
            continue;
          }
          SourceTable table = scanTable(tableName, schema);
          tables.put(tableName, table);
        }
      }
    }
    return tables;
  }

  private SourceTable scanTable(String tableName, String schema) throws SQLException {
    SourceTable.Builder tableBuilder =
        SourceTable.builder(sourceType).name(tableName).schema(schema);

    List<SourceColumn> columns = scanColumns(tableName, schema);
    tableBuilder.columns(com.google.common.collect.ImmutableList.copyOf(columns));

    List<String> primaryKeys = scanPrimaryKeys(tableName, schema);
    tableBuilder.primaryKeyColumns(com.google.common.collect.ImmutableList.copyOf(primaryKeys));

    List<SourceIndex> indexes = scanIndexes(tableName, schema);
    tableBuilder.indexes(com.google.common.collect.ImmutableList.copyOf(indexes));

    List<SourceForeignKey> foreignKeys = scanForeignKeys(tableName, schema);
    tableBuilder.foreignKeys(com.google.common.collect.ImmutableList.copyOf(foreignKeys));

    return tableBuilder.build();
  }

  private List<SourceColumn> scanColumns(String tableName, String schema) throws SQLException {
    List<SourceColumn> columns = new ArrayList<>();
    String query =
        "SELECT c.column_name, c.data_type, e.data_type AS element_type, c.character_maximum_length, "
            + "c.numeric_precision, c.numeric_scale, c.is_nullable, c.is_generated "
            + "FROM information_schema.columns c "
            + "LEFT JOIN information_schema.element_types e "
            + "  ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) "
            + "      = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier)) "
            + "WHERE c.table_schema = ? AND c.table_name = ? "
            + "ORDER BY c.ordinal_position";

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schema);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String dataType = rs.getString("data_type");
          String elementType = rs.getString("element_type");

          SourceColumn.Builder columnBuilder =
              SourceColumn.builder(sourceType)
                  .name(rs.getString("column_name"))
                  .isNullable("YES".equals(rs.getString("is_nullable")));

          if ("ARRAY".equals(dataType) && elementType != null) {
            columnBuilder.type(elementType + "[]");
          } else {
            columnBuilder.type(dataType);
          }

          columnBuilder.isPrimaryKey(false);

          String isGenerated = rs.getString("is_generated");
          columnBuilder.isGenerated("ALWAYS".equals(isGenerated));

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
    }
    return columns;
  }

  private List<String> scanPrimaryKeys(String tableName, String schema) throws SQLException {
    List<String> primaryKeys = new ArrayList<>();
    String query =
        "SELECT kcu.column_name "
            + "FROM information_schema.table_constraints tc "
            + "JOIN information_schema.key_column_usage kcu "
            + "  ON tc.constraint_name = kcu.constraint_name "
            + "  AND tc.table_schema = kcu.table_schema "
            + "WHERE tc.table_schema = ? AND tc.table_name = ? "
            + "AND tc.constraint_type = 'PRIMARY KEY' "
            + "ORDER BY kcu.ordinal_position";

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schema);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          primaryKeys.add(rs.getString("column_name"));
        }
      }
    }
    return primaryKeys;
  }

  private List<SourceIndex> scanIndexes(String tableName, String schema) throws SQLException {
    Map<String, SourceIndex.Builder> indexBuilders = new java.util.HashMap<>();
    Map<String, List<String>> indexColsMap = new java.util.HashMap<>();

    String query =
        "SELECT irel.relname AS index_name, a.attname AS column_name, "
            + "i.indisunique AS is_unique "
            + "FROM pg_index AS i "
            + "JOIN pg_class AS trel ON trel.oid = i.indrelid "
            + "JOIN pg_namespace AS tnsp ON trel.relnamespace = tnsp.oid "
            + "JOIN pg_class AS irel ON irel.oid = i.indexrelid "
            + "CROSS JOIN LATERAL UNNEST (i.indkey) WITH ordinality AS c (colnum, ordinality) "
            + "JOIN pg_attribute AS a ON trel.oid = a.attrelid AND a.attnum = c.colnum "
            + "WHERE tnsp.nspname= ? AND trel.relname= ? AND i.indisprimary = false "
            + "GROUP BY tnsp.nspname, trel.relname, irel.relname, a.attname, array_position(i.indkey, a.attnum), i.indisunique "
            + "ORDER BY irel.relname, array_position(i.indkey, a.attnum);";

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schema);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String indexName = rs.getString("index_name");
          String columnName = rs.getString("column_name");
          boolean isUnique = rs.getBoolean("is_unique");

          if (!indexBuilders.containsKey(indexName)) {
            indexBuilders.put(
                indexName,
                SourceIndex.builder().name(indexName).tableName(tableName).isUnique(isUnique));
            indexColsMap.put(indexName, new ArrayList<>());
          }
          indexColsMap.get(indexName).add(columnName);
        }
      }
    }

    List<SourceIndex> indexes = new ArrayList<>();
    for (Map.Entry<String, SourceIndex.Builder> entry : indexBuilders.entrySet()) {
      indexes.add(
          entry
              .getValue()
              .columns(
                  com.google.common.collect.ImmutableList.copyOf(indexColsMap.get(entry.getKey())))
              .build());
    }
    return indexes;
  }

  private List<SourceForeignKey> scanForeignKeys(String tableName, String schema)
      throws SQLException {
    Map<String, SourceForeignKey.Builder> fkBuilders = new java.util.HashMap<>();
    Map<String, List<String>> keyColsMap = new java.util.HashMap<>();
    Map<String, List<String>> refColsMap = new java.util.HashMap<>();

    // Query to scan foreign keys using PostgreSQL system catalogs.
    // We use pg_constraint instead of INFORMATION_SCHEMA because INFORMATION_SCHEMA
    // does not provide a way to correctly map columns in multi-column foreign keys.
    String query =
        """
    SELECT
        c.conname AS "CONSTRAINT_NAME",      -- The name of the foreign key constraint
        a.attname AS "COLUMN_NAME",        -- The name of the column in the source table
        confrel.relname AS "REFERENCED_TABLE_NAME", -- The table being pointed to
        af.attname AS "REF_COLUMN_NAME"    -- The name of the column in the target table
    FROM
        pg_constraint c
    -- Get the schema (namespace) of the constraint
    JOIN pg_namespace n ON n.oid = c.connamespace
    -- Get the table the constraint belongs to
    JOIN pg_class r ON r.oid = c.conrelid
    -- Get the referenced (foreign) table
    JOIN pg_class confrel ON confrel.oid = c.confrelid
    -- Parallel unnest conkey (col IDs) and confkey (target col IDs)
    -- to maintain their 1-to-1 mapping and the original column order (ord)
    CROSS JOIN LATERAL UNNEST(c.conkey, c.confkey) WITH ORDINALITY AS u(concol, confcol, ord)
    -- Join with pg_attribute to get the column name using the ID from UNNEST
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.concol
    -- Join with pg_attribute to get the referenced column name using the ID from UNNEST
    JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = u.confcol
    WHERE
        c.contype = 'f'      -- 'f' stands for Foreign Key constraint
        AND n.nspname = ?    -- Filter by schema name
        AND r.relname = ?    -- Filter by table name
    -- Ensure composite keys are processed in the same order they were defined
    ORDER BY
        c.conname,
        u.ord;
    """;

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schema);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String constraintName = rs.getString("CONSTRAINT_NAME");
          String columnName = rs.getString("COLUMN_NAME");
          String referencedTable = rs.getString("REFERENCED_TABLE_NAME");
          String referencedColumn = rs.getString("REF_COLUMN_NAME");

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
