package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleInformationSchemaScanner implements SourceSchemaScanner {
  private final Connection connection;

  public OracleInformationSchemaScanner(Connection connection) {
    this.connection = connection;
  }

  @Override
  public SourceSchema scan() {
    Map<String, SourceTable> tablesMap = new HashMap<>();
    SourceSchema.Builder builder = SourceSchema.builder(SourceDatabaseType.ORACLE).databaseName("ORACLE");
    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String schemaPattern = null;
      try {
        schemaPattern = connection.getSchema();
      } catch (Exception e) {
      }
      
      try (ResultSet rs = metaData.getTables(null, schemaPattern, "%", new String[]{"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          SourceTable table = scanTable(metaData, schemaPattern, tableName);
          tablesMap.put(tableName, table);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to scan Oracle information schema", e);
    }
    return builder.tables(ImmutableMap.copyOf(tablesMap)).build();
  }

  private SourceTable scanTable(DatabaseMetaData metaData, String schemaPattern, String tableName) throws SQLException {
    SourceTable.Builder tableBuilder = SourceTable.builder(SourceDatabaseType.ORACLE).name(tableName).schema(schemaPattern);
    List<SourceColumn> columns = new ArrayList<>();
    
    try (ResultSet colsRs = metaData.getColumns(null, schemaPattern, tableName, "%")) {
      while (colsRs.next()) {
        String columnName = colsRs.getString("COLUMN_NAME");
        String dataType = colsRs.getString("TYPE_NAME");
        
        SourceColumn.Builder colBuilder = SourceColumn.builder(SourceDatabaseType.ORACLE)
            .name(columnName)
            .type(dataType)
            .isNullable("YES".equalsIgnoreCase(colsRs.getString("IS_NULLABLE")));
            
        String isGeneratedStr = "";
        try {
          isGeneratedStr = colsRs.getString("IS_GENERATEDCOLUMN");
        } catch (Exception e) {}
        colBuilder.isGenerated("YES".equalsIgnoreCase(isGeneratedStr));
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
