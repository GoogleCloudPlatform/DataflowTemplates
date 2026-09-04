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
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDialectAdapter implements DialectAdapter {

  private static final Logger logger = LoggerFactory.getLogger(SqlServerDialectAdapter.class);

  private static final ImmutableMap<String, SourceColumnIndexInfo.IndexType> INDEX_TYPE_MAPPING =
      ImmutableMap.<String, SourceColumnIndexInfo.IndexType>builder()
          .put("BIGINT", IndexType.NUMERIC)
          .put("INT", IndexType.NUMERIC)
          .put("SMALLINT", IndexType.NUMERIC)
          .put("TINYINT", IndexType.NUMERIC)
          .put("BIT", IndexType.NUMERIC)
          .put("DECIMAL", IndexType.DECIMAL)
          .put("NUMERIC", IndexType.DECIMAL)
          .put("MONEY", IndexType.DECIMAL)
          .put("SMALLMONEY", IndexType.DECIMAL)
          .put("FLOAT", IndexType.DOUBLE)
          .put("REAL", IndexType.FLOAT)
          .put("DATE", IndexType.DATE)
          .put("DATETIME", IndexType.TIME_STAMP)
          .put("DATETIME2", IndexType.TIME_STAMP)
          .put("SMALLDATETIME", IndexType.TIME_STAMP)
          .put("DATETIMEOFFSET", IndexType.TIME_STAMP)
          .put("CHAR", IndexType.STRING)
          .put("VARCHAR", IndexType.STRING)
          .put("TEXT", IndexType.STRING)
          .put("NCHAR", IndexType.STRING)
          .put("NVARCHAR", IndexType.STRING)
          .put("NTEXT", IndexType.STRING)
          .put("UNIQUEIDENTIFIER", IndexType.STRING)
          .put("XML", IndexType.STRING)
          .put("SYSNAME", IndexType.STRING)
          .put("TIME", IndexType.STRING)
          .put("BINARY", IndexType.BINARY)
          .put("VARBINARY", IndexType.BINARY)
          .put("IMAGE", IndexType.BINARY)
          .put("ROWVERSION", IndexType.BINARY)
          .put("TIMESTAMP", IndexType.BINARY)
          .build();

  private final Set<ColumnKey> customBoundaryQueryColumnKeys = ConcurrentHashMap.newKeySet();

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
    logger.info(
        String.format(
            "Discovering table schema for Datasource: %s, JdbcSchemaReference: %s, tables: %s",
            dataSource, sourceSchemaReference, tables));

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
      logger.error(
          String.format(
              "Sql exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      throw new SchemaDiscoveryException(e);
    }

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((t, b) -> result.put(t, b.build()));

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchema = result.build();
    logger.info(
        String.format(
            "Discovered table schema for Datasource: %s, JdbcSchemaReference: %s, tables: %s",
            dataSource, sourceSchemaReference, tables));
    return tableSchema;
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
    logger.info(
        String.format(
            "Discovering Indexes for DataSource: %s, JdbcSchemaReference: %s, Tables: %s",
            dataSource, sourceSchemaReference, tables));
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
          String upperType = typeName.toUpperCase();
          IndexType indexType = INDEX_TYPE_MAPPING.getOrDefault(upperType, IndexType.OTHER);

          SourceColumnIndexInfo.Builder infoBuilder =
              SourceColumnIndexInfo.builder()
                  .setColumnName(rs.getString("column_name"))
                  .setIndexName(rs.getString("index_name"))
                  .setIsUnique(rs.getBoolean("is_unique"))
                  .setIsPrimary(rs.getBoolean("is_primary"))
                  .setOrdinalPosition(rs.getLong("ordinal_position"))
                  .setCardinality(100L) // stub
                  .setColumnTypeName(typeName)
                  .setIndexType(indexType);

          if (indexType == SourceColumnIndexInfo.IndexType.STRING) {
            com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper
                    .CollationReference
                collation =
                    com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper
                        .CollationReference.builder()
                        .setDbCharacterSet("UTF8")
                        .setDbCollation("Latin1_General_BIN")
                        .setPadSpace(false)
                        .build();
            infoBuilder.setCollationReference(collation);
            infoBuilder.setStringMaxLength(255);
          } else if (indexType == SourceColumnIndexInfo.IndexType.DECIMAL) {
            infoBuilder.setNumericScale(4);
          }

          if (upperType.equals("BIT")) {
            customBoundaryQueryColumnKeys.add(
                new ColumnKey(tableName, rs.getString("column_name")));
          }

          if (builders.containsKey(tableName)) {
            builders.get(tableName).add(infoBuilder.build());
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
    return addWhereClause("SELECT * FROM " + tableName, partitionColumns);
  }

  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause("SELECT COUNT(*) FROM " + tableName, partitionColumns);
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    String colExpr =
        customBoundaryQueryColumnKeys.contains(new ColumnKey(tableName, colName))
            ? String.format("CAST(%s AS BIGINT)", colName)
            : colName;
    return addWhereClause(
        String.format("SELECT MIN(%s), MAX(%s) FROM %s", colExpr, colExpr, tableName),
        partitionColumns);
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    return false;
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    return "WITH Nums AS ("
        + " SELECT TOP 256 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n"
        + " FROM sys.all_objects a CROSS JOIN sys.all_objects b"
        + ") "
        + "SELECT "
        + "  NCHAR(n) AS charset_char,"
        + "  NCHAR(n) AS equivalent_charset_char,"
        + "  CAST(n AS BIGINT) AS codepoint_rank,"
        + "  CAST(0 AS BIT) AS is_empty,"
        + "  CAST(CASE WHEN n = 32 THEN 1 ELSE 0 END AS BIT) AS is_space,"
        + "  NCHAR(n) AS equivalent_charset_char_pad_space,"
        + "  CAST(CASE WHEN n < 32 THEN n WHEN n = 32 THEN 0 ELSE n - 1 END AS BIGINT) AS codepoint_rank_pad_space "
        + "FROM Nums "
        + "ORDER BY n";
  }

  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      return query;
    }
    StringBuilder queryBuilder = new StringBuilder(query);
    queryBuilder.append(" WHERE ");
    queryBuilder.append(
        String.join(
            " AND ",
            partitionColumns.stream()
                .map(
                    col ->
                        String.format(
                            "((? = 0) OR (%1$s >= ? AND (%1$s < ? OR (? = 1 AND %1$s = ?))))", col))
                .toArray(String[]::new)));
    return queryBuilder.toString();
  }

  private static final class ColumnKey implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String tableName;
    private final String columnName;

    public ColumnKey(String tableName, String columnName) {
      this.tableName = clean(tableName);
      this.columnName = clean(columnName);
    }

    private static String clean(String identifier) {
      if (identifier == null) {
        return "";
      }
      return identifier
          .replace("`", "")
          .replace("\"", "")
          .replace("[", "")
          .replace("]", "")
          .toLowerCase();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ColumnKey)) {
        return false;
      }
      ColumnKey that = (ColumnKey) o;
      return tableName.equals(that.tableName) && columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, columnName);
    }
  }
}
