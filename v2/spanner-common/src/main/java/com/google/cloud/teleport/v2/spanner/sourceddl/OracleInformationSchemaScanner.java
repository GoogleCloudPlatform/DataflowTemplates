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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class OracleInformationSchemaScanner implements SourceSchemaScanner {
  private final Connection connection;
  private final String schema;

  public OracleInformationSchemaScanner(Connection connection) {
    this(connection, null);
  }

  public OracleInformationSchemaScanner(Connection connection, String schema) {
    this.connection = connection;
    this.schema = schema;
  }

  @Override
  public SourceSchema scan() {
    Map<String, SourceTable> tablesMap = new HashMap<>();
    SourceSchema.Builder builder =
        SourceSchema.builder(SourceDatabaseType.ORACLE).databaseName("ORACLE");
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String schemaPattern =
          (this.schema != null && !this.schema.isBlank())
              ? normalizeSchemaPattern(this.schema)
              : null;
      if (schemaPattern == null) {
        try {
          schemaPattern = connection.getSchema();
        } catch (Exception e) {
        }
      }
      if (schemaPattern == null) {
        try {
          schemaPattern = metaData.getUserName();
        } catch (Exception e) {
        }
      }

      try (ResultSet rs = metaData.getTables(null, schemaPattern, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (tableName == null
              || tableName.startsWith("BIN$")
              || tableName.startsWith("DR$")
              || tableName.startsWith("MLOG$_")
              || tableName.startsWith("RUPD$_")) {
            continue;
          }
          SourceTable table = scanTable(metaData, schemaPattern, tableName);
          tablesMap.put(tableName, table);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to scan Oracle information schema", e);
    }
    return builder.tables(ImmutableMap.copyOf(tablesMap)).build();
  }

  private static String normalizeSchemaPattern(String rawSchema) {
    String trimmed = rawSchema.trim();
    if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed.toUpperCase(Locale.ROOT);
  }

  private SourceTable scanTable(DatabaseMetaData metaData, String schemaPattern, String tableName)
      throws SQLException {
    SourceTable.Builder tableBuilder =
        SourceTable.builder(SourceDatabaseType.ORACLE).name(tableName).schema(schemaPattern);

    List<String> pks = new ArrayList<>();
    Set<String> pkSet = new HashSet<>();
    String pkIndexName = null;
    List<Map.Entry<Integer, String>> pkList = new ArrayList<>();
    try (ResultSet pkRs = metaData.getPrimaryKeys(null, schemaPattern, tableName)) {
      if (pkRs != null) {
        while (pkRs.next()) {
          if (pkIndexName == null) {
            try {
              pkIndexName = pkRs.getString("PK_NAME");
            } catch (Exception e) {
            }
          }
          String pkCol = pkRs.getString("COLUMN_NAME");
          if (pkCol != null) {
            int keySeq = 0;
            try {
              keySeq = pkRs.getInt("KEY_SEQ");
            } catch (Exception e) {
            }
            pkList.add(Map.entry(keySeq, pkCol));
            pkSet.add(pkCol);
          }
        }
      }
    }
    pkList.sort(Map.Entry.comparingByKey());
    for (Map.Entry<Integer, String> entry : pkList) {
      pks.add(entry.getValue());
    }

    List<SourceColumn> columns = new ArrayList<>();
    try (ResultSet colsRs = metaData.getColumns(null, schemaPattern, tableName, "%")) {
      if (colsRs != null) {
        while (colsRs.next()) {
          String colTable = null;
          try {
            colTable = colsRs.getString("TABLE_NAME");
          } catch (Exception e) {
          }
          if (colTable != null && !tableName.equals(colTable)) {
            continue;
          }
          String columnName = colsRs.getString("COLUMN_NAME");
          String dataType = colsRs.getString("TYPE_NAME");

          SourceColumn.Builder colBuilder =
              SourceColumn.builder(SourceDatabaseType.ORACLE)
                  .name(columnName)
                  .type(dataType)
                  .isNullable("YES".equalsIgnoreCase(colsRs.getString("IS_NULLABLE")))
                  .isPrimaryKey(pkSet.contains(columnName));

          String isGeneratedStr = "";
          try {
            isGeneratedStr = colsRs.getString("IS_GENERATEDCOLUMN");
          } catch (Exception e) {
          }
          String isAutoIncrementStr = "";
          try {
            isAutoIncrementStr = colsRs.getString("IS_AUTOINCREMENT");
          } catch (Exception e) {
          }
          boolean isGenerated =
              "YES".equalsIgnoreCase(isGeneratedStr)
                  || (!pkSet.contains(columnName) && "YES".equalsIgnoreCase(isAutoIncrementStr));
          colBuilder.isGenerated(isGenerated);

          populateColumnBounds(colsRs, dataType, colBuilder);
          columns.add(colBuilder.build());
        }
      }
    }

    List<SourceIndex> indexes = scanIndexes(metaData, schemaPattern, tableName, pkIndexName, pkSet);
    List<SourceForeignKey> foreignKeys = scanForeignKeys(metaData, schemaPattern, tableName);

    tableBuilder.columns(ImmutableList.copyOf(columns));
    tableBuilder.primaryKeyColumns(ImmutableList.copyOf(pks));
    tableBuilder.indexes(ImmutableList.copyOf(indexes));
    tableBuilder.foreignKeys(ImmutableList.copyOf(foreignKeys));
    return tableBuilder.build();
  }

  private void populateColumnBounds(
      ResultSet colsRs, String dataType, SourceColumn.Builder colBuilder) {
    if (dataType == null) {
      return;
    }
    String upperType =
        dataType
            .toUpperCase(Locale.ROOT)
            .replaceAll("\\s*\\([^)]*\\)", "")
            .replaceAll("\\s+", " ")
            .trim();
    try {
      int colSize = colsRs.getInt("COLUMN_SIZE");
      boolean sizeNull = colsRs.wasNull();
      int decimalDigits = colsRs.getInt("DECIMAL_DIGITS");
      boolean scaleNull = colsRs.wasNull();

      switch (upperType) {
        case "VARCHAR2":
        case "VARCHAR":
        case "CHAR":
        case "CHARACTER":
        case "NVARCHAR2":
        case "NCHAR":
        case "NCHAR VARYING":
        case "NATIONAL CHARACTER":
        case "NATIONAL CHAR":
        case "NATIONAL CHARACTER VARYING":
        case "NATIONAL CHAR VARYING":
        case "RAW":
        case "UROWID":
          if (!sizeNull && colSize > 0) {
            colBuilder.size((long) colSize);
          }
          break;
        case "NUMBER":
        case "NUMERIC":
        case "DECIMAL":
        case "DEC":
        case "INTEGER":
        case "INT":
        case "SMALLINT":
          if (!sizeNull && colSize > 0) {
            colBuilder.precision(colSize);
          }
          if (!scaleNull && decimalDigits != -127) {
            colBuilder.scale(decimalDigits);
          }
          break;
        default:
          break;
      }
    } catch (Exception e) {
      // Ignore if metadata columns are not available in mock ResultSets.
    }
  }

  private List<SourceIndex> scanIndexes(
      DatabaseMetaData metaData,
      String schemaPattern,
      String tableName,
      String pkIndexName,
      Set<String> pkSet) {
    List<SourceIndex> indexes = new ArrayList<>();
    try (ResultSet idxRs = metaData.getIndexInfo(null, schemaPattern, tableName, false, true)) {
      if (idxRs == null) {
        return indexes;
      }
      Map<String, SourceIndex.Builder> indexBuilders = new LinkedHashMap<>();
      Map<String, List<String>> indexColumns = new LinkedHashMap<>();
      while (idxRs.next()) {
        String indexName = idxRs.getString("INDEX_NAME");
        String columnName = idxRs.getString("COLUMN_NAME");
        if (indexName == null || columnName == null) {
          continue;
        }
        boolean nonUnique = idxRs.getBoolean("NON_UNIQUE");
        indexBuilders.computeIfAbsent(
            indexName,
            k ->
                SourceIndex.builder()
                    .name(k)
                    .tableName(tableName)
                    .isUnique(!nonUnique)
                    .isPrimary(false));
        indexColumns.computeIfAbsent(indexName, k -> new ArrayList<>()).add(columnName);
      }
      for (Map.Entry<String, SourceIndex.Builder> entry : indexBuilders.entrySet()) {
        String indexName = entry.getKey();
        List<String> cols = indexColumns.get(indexName);
        boolean isPkIndex =
            (pkIndexName != null && pkIndexName.equals(indexName))
                || (pkIndexName == null
                    && !pkSet.isEmpty()
                    && cols.size() == pkSet.size()
                    && pkSet.containsAll(cols));
        indexes.add(
            entry.getValue().isPrimary(isPkIndex).columns(ImmutableList.copyOf(cols)).build());
      }
    } catch (Exception e) {
      // Ignore index scan errors if unsupported by driver/mock.
    }
    return indexes;
  }

  private List<SourceForeignKey> scanForeignKeys(
      DatabaseMetaData metaData, String schemaPattern, String tableName) {
    List<SourceForeignKey> foreignKeys = new ArrayList<>();
    try (ResultSet fkRs = metaData.getImportedKeys(null, schemaPattern, tableName)) {
      if (fkRs == null) {
        return foreignKeys;
      }
      Map<String, SourceForeignKey.Builder> fkBuilders = new LinkedHashMap<>();
      Map<String, List<String>> keyColsMap = new LinkedHashMap<>();
      Map<String, List<String>> refColsMap = new LinkedHashMap<>();
      while (fkRs.next()) {
        String fkName = fkRs.getString("FK_NAME");
        String fkCol = fkRs.getString("FKCOLUMN_NAME");
        String pkTable = fkRs.getString("PKTABLE_NAME");
        String pkCol = fkRs.getString("PKCOLUMN_NAME");
        if (fkName == null || fkCol == null || pkTable == null || pkCol == null) {
          continue;
        }
        fkBuilders.computeIfAbsent(
            fkName,
            k -> SourceForeignKey.builder().name(k).tableName(tableName).referencedTable(pkTable));
        keyColsMap.computeIfAbsent(fkName, k -> new ArrayList<>()).add(fkCol);
        refColsMap.computeIfAbsent(fkName, k -> new ArrayList<>()).add(pkCol);
      }
      for (Map.Entry<String, SourceForeignKey.Builder> entry : fkBuilders.entrySet()) {
        String fkName = entry.getKey();
        foreignKeys.add(
            entry
                .getValue()
                .keyColumns(ImmutableList.copyOf(keyColsMap.get(fkName)))
                .referencedColumns(ImmutableList.copyOf(refColsMap.get(fkName)))
                .build());
      }
    } catch (Exception e) {
      // Ignore FK scan errors if unsupported by driver/mock.
    }
    return foreignKeys;
  }
}
