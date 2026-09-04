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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      String schemaPattern = "dbo";

      try (ResultSet rs = metaData.getTables(null, schemaPattern, "%", new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (tableName == null
              || tableName.startsWith("trace_xe_")
              || tableName.startsWith("spt_")) {
            continue;
          }
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
    List<SourceColumn> columns = new ArrayList<>();

    Set<String> computedColumns = new HashSet<>();
    String query =
        "SELECT c.name FROM sys.computed_columns c "
            + "JOIN sys.tables t ON c.object_id = t.object_id "
            + "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            + "WHERE s.name = ? AND t.name = ?";
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, schemaPattern);
      stmt.setString(2, tableName);
      try (ResultSet compRs = stmt.executeQuery()) {
        while (compRs.next()) {
          computedColumns.add(compRs.getString(1));
        }
      }
    } catch (Exception e) {
      // Ignore if sys.computed_columns is not accessible and fallback to metadata
    }

    try (ResultSet colsRs = metaData.getColumns(null, schemaPattern, tableName, "%")) {
      while (colsRs.next()) {
        String columnName = colsRs.getString("COLUMN_NAME");
        String dataType = colsRs.getString("TYPE_NAME");

        SourceColumn.Builder colBuilder =
            SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                .name(columnName)
                .type(dataType)
                .isNullable("YES".equalsIgnoreCase(colsRs.getString("IS_NULLABLE")));

        String isGeneratedStr = "";
        try {
          isGeneratedStr = colsRs.getString("IS_GENERATEDCOLUMN");
        } catch (Exception e) {
        }
        boolean isGenerated =
            computedColumns.contains(columnName) || "YES".equalsIgnoreCase(isGeneratedStr);
        colBuilder.isGenerated(isGenerated);
        columns.add(colBuilder.build());
      }
    }

    List<String> pks = new ArrayList<>();
    try (ResultSet pkRs = metaData.getPrimaryKeys(null, schemaPattern, tableName)) {
      while (pkRs.next()) {
        pks.add(pkRs.getString("COLUMN_NAME"));
      }
    }

    tableBuilder.columns(ImmutableList.copyOf(columns));
    tableBuilder.primaryKeyColumns(ImmutableList.copyOf(pks));
    return tableBuilder.build();
  }
}
