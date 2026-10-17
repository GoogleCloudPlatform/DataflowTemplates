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
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Scans a SQL Server database schema via JDBC {@link DatabaseMetaData}.
 *
 * <p>Besides tables, columns and primary keys, the scanner also collects foreign keys (via {@link
 * DatabaseMetaData#getImportedKeys}) and unique indexes (via {@link
 * DatabaseMetaData#getIndexInfo}). Column sizes, precisions and scales are populated from the
 * {@code COLUMN_SIZE} and {@code DECIMAL_DIGITS} metadata columns.
 */
public class SQLServerInformationSchemaScanner implements SourceSchemaScanner {

  private final Connection connection;
  private final String databaseName;

  public SQLServerInformationSchemaScanner(Connection connection, String databaseName) {
    this.connection = connection;
    this.databaseName = databaseName;
  }

  @Override
  public SourceSchema scan() {
    Map<String, SourceTable> tablesMap = new HashMap<>();
    SourceSchema.Builder builder =
        SourceSchema.builder(SourceDatabaseType.SQLSERVER).databaseName(databaseName);
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String schemaPattern = connection.getSchema();

      try (ResultSet rs = metaData.getTables(null, schemaPattern, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          SourceTable table = scanTable(metaData, schemaPattern, tableName);
          tablesMap.put(tableName, table);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to scan SQL Server information schema", e);
    }
    return builder.tables(ImmutableMap.copyOf(tablesMap)).build();
  }

  private SourceTable scanTable(DatabaseMetaData metaData, String schemaPattern, String tableName)
      throws SQLException {
    SourceTable.Builder tableBuilder =
        SourceTable.builder(SourceDatabaseType.SQLSERVER).name(tableName).schema(schemaPattern);

    ImmutableList.Builder<String> pksBuilder = ImmutableList.builder();
    Set<String> pkSet = new HashSet<>();
    String pkName = null;
    List<Map.Entry<Integer, String>> pkList = new ArrayList<>();
    try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, schemaPattern, tableName)) {
      while (pkResultSet.next()) {
        String pkCol = pkResultSet.getString("COLUMN_NAME");
        if (pkCol != null) {
          int keySeq = pkResultSet.getInt("KEY_SEQ");
          pkList.add(Map.entry(keySeq, pkCol));
          pkSet.add(pkCol);
          if (pkName == null) {
            pkName = pkResultSet.getString("PK_NAME");
          }
        }
      }
    }
    pkList.sort(Map.Entry.comparingByKey());
    for (Map.Entry<Integer, String> entry : pkList) {
      pksBuilder.add(entry.getValue());
    }

    ImmutableList.Builder<SourceColumn> columnsBuilder = ImmutableList.builder();
    try (ResultSet colsRs = metaData.getColumns(null, schemaPattern, tableName, "%")) {
      while (colsRs.next()) {
        String columnName = colsRs.getString("COLUMN_NAME");
        String dataType = colsRs.getString("TYPE_NAME");

        SourceColumn.Builder colBuilder =
            SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                .name(columnName)
                .type(dataType)
                .isNullable("YES".equalsIgnoreCase(colsRs.getString("IS_NULLABLE")))
                .isPrimaryKey(pkSet.contains(columnName));

        boolean isGenerated = "YES".equalsIgnoreCase(colsRs.getString("IS_GENERATEDCOLUMN"));
        colBuilder.isGenerated(isGenerated);

        // COLUMN_SIZE is the character length for string types, the byte length for binary
        // types and the precision for numeric types. DECIMAL_DIGITS is the scale for numeric
        // types and is NULL for types where scale is not applicable.
        int columnSize = colsRs.getInt("COLUMN_SIZE");
        if (!colsRs.wasNull()) {
          colBuilder.size((long) columnSize);
          int decimalDigits = colsRs.getInt("DECIMAL_DIGITS");
          if (!colsRs.wasNull()) {
            colBuilder.precision(columnSize);
            colBuilder.scale(decimalDigits);
          }
        }

        columnsBuilder.add(colBuilder.build());
      }
    }

    tableBuilder.columns(columnsBuilder.build());
    tableBuilder.primaryKeyColumns(pksBuilder.build());
    tableBuilder.foreignKeys(scanForeignKeys(metaData, schemaPattern, tableName));
    tableBuilder.indexes(scanUniqueIndexes(metaData, schemaPattern, tableName, pkName));
    return tableBuilder.build();
  }

  private ImmutableList<SourceForeignKey> scanForeignKeys(
      DatabaseMetaData metaData, String schemaPattern, String tableName) throws SQLException {
    // FK name -> (KEY_SEQ -> [fkColumn, pkColumn]) so that composite keys keep column order.
    Map<String, String> referencedTables = new LinkedHashMap<>();
    Map<String, TreeMap<Integer, String>> keyColumns = new HashMap<>();
    Map<String, TreeMap<Integer, String>> referencedColumns = new HashMap<>();
    try (ResultSet rs = metaData.getImportedKeys(null, schemaPattern, tableName)) {
      if (rs == null) {
        return ImmutableList.of();
      }
      while (rs.next()) {
        String fkName = rs.getString("FK_NAME");
        if (fkName == null) {
          continue;
        }
        int keySeq = rs.getInt("KEY_SEQ");
        referencedTables.putIfAbsent(fkName, rs.getString("PKTABLE_NAME"));
        keyColumns
            .computeIfAbsent(fkName, k -> new TreeMap<>())
            .put(keySeq, rs.getString("FKCOLUMN_NAME"));
        referencedColumns
            .computeIfAbsent(fkName, k -> new TreeMap<>())
            .put(keySeq, rs.getString("PKCOLUMN_NAME"));
      }
    }
    ImmutableList.Builder<SourceForeignKey> builder = ImmutableList.builder();
    for (Map.Entry<String, String> entry : referencedTables.entrySet()) {
      String fkName = entry.getKey();
      builder.add(
          SourceForeignKey.builder()
              .name(fkName)
              .tableName(tableName)
              .referencedTable(entry.getValue())
              .keyColumns(ImmutableList.copyOf(keyColumns.get(fkName).values()))
              .referencedColumns(ImmutableList.copyOf(referencedColumns.get(fkName).values()))
              .build());
    }
    return builder.build();
  }

  private ImmutableList<SourceIndex> scanUniqueIndexes(
      DatabaseMetaData metaData, String schemaPattern, String tableName, String pkName)
      throws SQLException {
    Map<String, TreeMap<Integer, String>> indexColumns = new LinkedHashMap<>();
    try (ResultSet rs = metaData.getIndexInfo(null, schemaPattern, tableName, true, false)) {
      if (rs == null) {
        return ImmutableList.of();
      }
      while (rs.next()) {
        // TYPE == tableIndexStatistic rows describe the table itself, not an index.
        if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
          continue;
        }
        String indexName = rs.getString("INDEX_NAME");
        String columnName = rs.getString("COLUMN_NAME");
        if (indexName == null || columnName == null) {
          continue;
        }
        indexColumns
            .computeIfAbsent(indexName, k -> new TreeMap<>())
            .put((int) rs.getShort("ORDINAL_POSITION"), columnName);
      }
    }
    ImmutableList.Builder<SourceIndex> builder = ImmutableList.builder();
    for (Map.Entry<String, TreeMap<Integer, String>> entry : indexColumns.entrySet()) {
      String indexName = entry.getKey();
      builder.add(
          SourceIndex.builder()
              .name(indexName)
              .tableName(tableName)
              .columns(ImmutableList.copyOf(entry.getValue().values()))
              .isUnique(true)
              .isPrimary(indexName.equals(pkName))
              .build());
    }
    return builder.build();
  }
}
