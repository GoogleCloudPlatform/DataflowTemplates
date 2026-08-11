package com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.dialectadapter.oracle;

import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
public class OracleDialectAdapter implements DialectAdapter {
  private String quote(String identifier) {
    if (identifier == null) { return null; }
    if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
      return identifier; // Already quoted
    }
    return "\"" + identifier + "\"";
  }
  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
       return query;
    }
    StringBuilder queryBuilder = new StringBuilder(query + " WHERE ");
    boolean firstDone = false;
    for (String partitionColumn : partitionColumns) {
      if (firstDone) { queryBuilder.append(" AND "); }
      queryBuilder.append("((TO_CHAR(?) IN ('0', 'false', 'FALSE')) OR "); 
      String quotedPart = quote(partitionColumn);
      queryBuilder.append(String.format("(%1$s >= ? AND (%1$s < ? OR (TO_CHAR(?) IN ('1', 'true', 'TRUE') AND %1$s = ?)))", quotedPart));
      queryBuilder.append(")");
      firstDone = true;
    }
    return queryBuilder.toString();
  }

  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    return addWhereClause("SELECT * FROM " + quote(tableName), partitionColumns);
  }

  @Override
  public String getCountQuery(String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause("SELECT COUNT(*) FROM " + quote(tableName), partitionColumns);
  }

  @Override
  public String getBoundaryQuery(String tableName, ImmutableList<String> partitionColumns, String colName) {
    return addWhereClause("SELECT MIN(" + quote(colName) + "), MAX(" + quote(colName) + ") FROM " + quote(tableName), partitionColumns);
  }


  @Override
  public boolean checkForTimeout(SQLException exception) {
    return false; // Simplified
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    return "SELECT 1 FROM DUAL WHERE 1=0";
  }

  @Override
  public ImmutableList<String> discoverTables(
      javax.sql.DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      String schemaPattern = sourceSchemaReference.namespace() != null ? sourceSchemaReference.namespace() : metaData.getUserName();
      if (schemaPattern != null) {
          schemaPattern = null;
      }
      try (ResultSet rs = metaData.getTables(null, schemaPattern, null, new String[] {"TABLE"})) {
        while (rs.next()) {
          tablesBuilder.add(rs.getString("TABLE_NAME"));
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    ImmutableList<String> tables = tablesBuilder.build();
    org.slf4j.LoggerFactory.getLogger(OracleDialectAdapter.class).info("Discovered Oracle Tables: {}", tables);
    return tables;
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference schemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Map<String, ImmutableMap.Builder<String, SourceColumnType>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableMap.builder()));
    
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      String schemaPattern = schemaReference.namespace() != null ? schemaReference.namespace() : metaData.getUserName();
      if (schemaPattern != null) {
          schemaPattern = null;
      }
      for (String table : tables) {
        try (ResultSet rs = metaData.getColumns(null, schemaPattern, table, null)) {
          while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            if (typeName != null) {
              typeName = typeName.replaceAll("\\([0-9]+\\)", "");
            }
            long colSize = rs.getLong("COLUMN_SIZE");
            long descinalDigits = rs.getLong("DECIMAL_DIGITS"); // May be 0 if null
            SourceColumnType record = new SourceColumnType(
                typeName,
                new Long[] {colSize},
                null);
            builders.get(table).put(colName, record);
          }
        }
          org.slf4j.LoggerFactory.getLogger(OracleDialectAdapter.class).info("Discovered Table Schema for {}: {}", table, builders.get(table).build());
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((k, v) -> result.put(k, v.build()));
    return result.build();
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      javax.sql.DataSource dataSource, JdbcSchemaReference sourceSchemaReference, ImmutableList<String> tables) {
    Map<String, ImmutableList.Builder<SourceColumnIndexInfo>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableList.builder()));
    
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      for (String table : tables) {
        try (ResultSet rs = metaData.getPrimaryKeys(null, null, table)) {
          while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String pkName = rs.getString("PK_NAME");
            long seq = rs.getShort("KEY_SEQ");
            
            SourceColumnIndexInfo.IndexType type = SourceColumnIndexInfo.IndexType.OTHER;
            try (ResultSet crs = metaData.getColumns(null, null, table, colName)) {
                if (crs.next()) {
                    String typeName = crs.getString("TYPE_NAME");
                    if (typeName != null) {
                        typeName = typeName.toUpperCase();
                        if (typeName.contains("CHAR") || typeName.contains("CLOB")) { type = SourceColumnIndexInfo.IndexType.STRING; }
                        else if (typeName.contains("INT") || typeName.contains("NUM")) { type = SourceColumnIndexInfo.IndexType.NUMERIC; }
                        else if (typeName.contains("DATE") || typeName.contains("TIME")) { type = SourceColumnIndexInfo.IndexType.TIME_STAMP; }
                    }
                }
            }

            SourceColumnIndexInfo.Builder infoBuilder = SourceColumnIndexInfo.builder()
                .setColumnName(colName)
                .setIsPrimary(true)
                .setIsUnique(true)
                .setOrdinalPosition(seq)
                .setIndexName(pkName != null ? pkName : "PRIMARY")
                .setIndexType(type)
                .setColumnTypeName("");
                
            if (type == SourceColumnIndexInfo.IndexType.STRING) {
                com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference emptyCollation = 
                    com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference.builder()
                    .setDbCharacterSet("UTF8")
                    .setDbCollation("UTF8_BIN")
                    .setPadSpace(false)
                    .build();
                infoBuilder.setCollationReference(emptyCollation);
                infoBuilder.setStringMaxLength(200);
            }

            builders.get(table).add(infoBuilder.build());
          }
        }
      }
    } catch (SQLException e) {
    }
    
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result = ImmutableMap.builder();
    builders.forEach((k, v) -> result.put(k, v.build()));
    return result.build();
  }
}
