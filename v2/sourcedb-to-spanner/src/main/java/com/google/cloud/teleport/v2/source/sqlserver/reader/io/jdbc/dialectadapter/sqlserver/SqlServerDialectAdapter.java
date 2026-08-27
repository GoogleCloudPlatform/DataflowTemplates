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
package com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.dialectadapter.sqlserver;

import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

public class SqlServerDialectAdapter implements DialectAdapter {

  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    String query =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = ?";
    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          tablesBuilder.add(rs.getString(1));
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }
    return tablesBuilder.build();
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }

    String query =
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = ? AND TABLE_NAME IN "
            + DialectAdapter.generateInClause(tables.size());
    Map<String, ImmutableMap.Builder<String, SourceColumnType>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableMap.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      for (int i = 0; i < tables.size(); i++) {
        stmt.setString(i + 2, tables.get(i));
      }
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String columnName = rs.getString("COLUMN_NAME");
          String dataType = rs.getString("DATA_TYPE");

          long charMaxLen = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
          boolean hasCharMaxLen = !rs.wasNull();

          long numPrecision = rs.getLong("NUMERIC_PRECISION");
          boolean hasNumPrecision = !rs.wasNull();

          long numScale = rs.getLong("NUMERIC_SCALE");
          boolean hasNumScale = !rs.wasNull();

          SourceColumnType sourceColumnType;
          if (hasCharMaxLen) {
            sourceColumnType = new SourceColumnType(dataType, new Long[] {charMaxLen}, null);
          } else if (hasNumPrecision && hasNumScale) {
            sourceColumnType =
                new SourceColumnType(dataType, new Long[] {numPrecision, numScale}, null);
          } else if (hasNumPrecision) {
            sourceColumnType = new SourceColumnType(dataType, new Long[] {numPrecision}, null);
          } else {
            sourceColumnType = new SourceColumnType(dataType, new Long[] {}, null);
          }
          if (builders.containsKey(tableName)) {
            builders.get(tableName).put(columnName, sourceColumnType);
          }
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((t, b) -> result.put(t, b.build()));
    return result.build();
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }
    // Simplified index discovery for SQL Server. Focus on primary keys.
    String query =
        "SELECT "
            + "    t.name AS table_name, "
            + "    ind.name AS index_name, "
            + "    col.name AS column_name, "
            + "    ic.key_ordinal AS ordinal_position, "
            + "    ind.is_unique, "
            + "    ind.is_primary_key AS is_primary, "
            + "    ty.name AS type_name "
            + "FROM sys.indexes ind "
            + "INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id and ind.index_id = ic.index_id "
            + "INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id "
            + "INNER JOIN sys.tables t ON ind.object_id = t.object_id "
            + "INNER JOIN sys.types ty ON col.system_type_id = ty.system_type_id AND col.user_type_id = ty.user_type_id "
            + "WHERE t.name IN "
            + DialectAdapter.generateInClause(tables.size());

    Map<String, ImmutableList.Builder<SourceColumnIndexInfo>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableList.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      for (int i = 0; i < tables.size(); i++) {
        stmt.setString(i + 1, tables.get(i));
      }
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          String typeName = rs.getString("type_name");
          // Just map string to STRING and everything else to NUMERIC to simplify.
          SourceColumnIndexInfo.IndexType indexType = SourceColumnIndexInfo.IndexType.OTHER;
          if (typeName.toUpperCase().contains("CHAR")) {
            indexType = SourceColumnIndexInfo.IndexType.STRING;
          } else if (typeName.toUpperCase().contains("INT")) {
            indexType = SourceColumnIndexInfo.IndexType.NUMERIC;
          }

          SourceColumnIndexInfo info =
              SourceColumnIndexInfo.builder()
                  .setColumnName(rs.getString("column_name"))
                  .setIndexName(rs.getString("index_name"))
                  .setIsUnique(rs.getBoolean("is_unique"))
                  .setIsPrimary(rs.getBoolean("is_primary"))
                  .setOrdinalPosition(rs.getLong("ordinal_position"))
                  .setCardinality(100L) // stub
                  .setColumnTypeName(typeName)
                  .setIndexType(indexType)
                  .build();

          if (builders.containsKey(tableName)) {
            builders.get(tableName).add(info);
          }
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
    }

    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();
    builders.forEach((t, b) -> result.put(t, b.build()));
    return result.build();
  }

  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    StringBuilder queryBuilder = new StringBuilder("SELECT * FROM " + tableName);
    if (!partitionColumns.isEmpty()) {
      queryBuilder.append(" WHERE ");
      queryBuilder.append(
          String.join(
              " AND ",
              partitionColumns.stream()
                  .map(
                      col ->
                          String.format(
                              "((? = 0) OR (%1$s >= ? AND (%1$s < ? OR (? = 1 AND %1$s = ?))))",
                              col))
                  .toArray(String[]::new)));
    }
    return queryBuilder.toString();
  }

  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    StringBuilder queryBuilder = new StringBuilder("SELECT COUNT(*) FROM " + tableName);
    if (!partitionColumns.isEmpty()) {
      queryBuilder.append(" WHERE ");
      queryBuilder.append(
          String.join(
              " AND ",
              partitionColumns.stream()
                  .map(
                      col ->
                          String.format(
                              "((? = 0) OR (%1$s >= ? AND (%1$s < ? OR (? = 1 AND %1$s = ?))))",
                              col))
                  .toArray(String[]::new)));
    }
    return queryBuilder.toString();
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    StringBuilder queryBuilder =
        new StringBuilder(
            String.format("SELECT MIN(%s), MAX(%s) FROM %s", colName, colName, tableName));
    if (!partitionColumns.isEmpty()) {
      queryBuilder.append(" WHERE ");
      queryBuilder.append(
          String.join(
              " AND ",
              partitionColumns.stream()
                  .map(
                      col ->
                          String.format(
                              "((? = 0) OR (%1$s >= ? AND (%1$s < ? OR (? = 1 AND %1$s = ?))))",
                              col))
                  .toArray(String[]::new)));
    }
    return queryBuilder.toString();
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    return false;
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    // Basic implementation to satisfy the interface.
    // Real implementation would need to return the ordered characters based on collation.
    return "SELECT 'a' AS char_col";
  }
}
