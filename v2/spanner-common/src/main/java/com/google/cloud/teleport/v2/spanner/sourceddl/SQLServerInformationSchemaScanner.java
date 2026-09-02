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
import java.util.HashMap;
import java.util.Map;

public class SQLServerInformationSchemaScanner implements SourceSchemaScanner {
  private final Connection connection;
  private final String databaseName;

  public SQLServerInformationSchemaScanner(Connection connection, String databaseName) {
    this.connection = connection;
    this.databaseName = databaseName;
  }

  /**
   * TODO: Analyze if it is needed to populate Indexes and Foreign Keys. Also, how to support other
   * schemas.
   *
   * @return SourceSchema
   */
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

    ImmutableList.Builder<SourceColumn> columnsBuilder = ImmutableList.builder();
    try (ResultSet colsRs = metaData.getColumns(null, schemaPattern, tableName, "%")) {
      while (colsRs.next()) {
        String columnName = colsRs.getString("COLUMN_NAME");
        String dataType = colsRs.getString("TYPE_NAME");

        SourceColumn.Builder colBuilder =
            SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                .name(columnName)
                .type(dataType)
                .isNullable("YES".equalsIgnoreCase(colsRs.getString("IS_NULLABLE")));

        boolean isGenerated = "YES".equalsIgnoreCase(colsRs.getString("IS_GENERATEDCOLUMN"));
        colBuilder.isGenerated(isGenerated);
        columnsBuilder.add(colBuilder.build());
      }
    }

    ImmutableList.Builder<String> pksBuilder = ImmutableList.builder();
    try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, schemaPattern, tableName)) {
      while (pkResultSet.next()) {
        pksBuilder.add(pkResultSet.getString("COLUMN_NAME"));
      }
    }

    tableBuilder.columns(columnsBuilder.build());
    tableBuilder.primaryKeyColumns(pksBuilder.build());
    return tableBuilder.build();
  }
}
