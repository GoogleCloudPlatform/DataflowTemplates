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
import java.sql.SQLTimeoutException;
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

  private String getNamespace(JdbcSchemaReference sourceSchemaReference) {
    return (sourceSchemaReference.namespace() == null
            || sourceSchemaReference.namespace().isEmpty())
        ? "dbo"
        : sourceSchemaReference.namespace();
  }

  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(String.format("Discovering tables for DataSource: %s", dataSource));

    String query =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = ? AND TABLE_SCHEMA = ?";
    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      stmt.setString(2, getNamespace(sourceSchemaReference));
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          tablesBuilder.add(rs.getString(1));
        }
      }
    } catch (SQLException e) {
      logger.error(
          String.format(
              "Sql exception while discovering table list for datasource=%s cause=%s",
              dataSource, e));
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
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
            + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME IN "
            + DialectAdapter.generateInClause(tables.size());
    Map<String, ImmutableMap.Builder<String, SourceColumnType>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableMap.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      stmt.setString(2, getNamespace(sourceSchemaReference));
      for (int i = 0; i < tables.size(); i++) {
        stmt.setString(i + 3, tables.get(i));
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
          if ("BIT".equalsIgnoreCase(dataType)) {
            // In SQL Server, MIN() and MAX() aggregate functions are invalid on BIT columns.
            // Running SELECT MIN(id),
            // MAX(id) FROM bit_pk_table directly fails. Hence, special handling will be required
            // for BIT.
            customBoundaryQueryColumnKeys.add(new ColumnKey(tableName, columnName));
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
            + "    ty.name AS type_name, "
            + "    col.max_length AS max_length, "
            + "    col.scale AS scale, "
            + "    col.collation_name AS collation_name, "
            + "    ISNULL(p.rows, 0) AS cardinality "
            + "FROM sys.indexes ind "
            + "INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id and ind.index_id = ic.index_id "
            + "INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id "
            + "INNER JOIN sys.tables t ON ind.object_id = t.object_id "
            + "INNER JOIN sys.types ty ON col.system_type_id = ty.system_type_id AND col.user_type_id = ty.user_type_id "
            + "LEFT JOIN (SELECT object_id, index_id, SUM(rows) AS rows FROM sys.partitions GROUP BY object_id, index_id) p ON ind.object_id = p.object_id AND ind.index_id = p.index_id "
            + "WHERE SCHEMA_NAME(t.schema_id) = ? "
            + "AND ic.is_included_column = 0 "
            + "AND ind.is_disabled = 0 "
            + "AND ind.is_hypothetical = 0 "
            + "AND t.name IN "
            + DialectAdapter.generateInClause(tables.size());

    Map<String, ImmutableList.Builder<SourceColumnIndexInfo>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableList.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, getNamespace(sourceSchemaReference));
      for (int i = 0; i < tables.size(); i++) {
        stmt.setString(i + 2, tables.get(i));
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
                  .setCardinality(rs.getLong("cardinality"))
                  .setColumnTypeName(typeName)
                  .setIndexType(indexType);

          if (indexType == SourceColumnIndexInfo.IndexType.STRING) {
            boolean padSpace = upperType.equals("CHAR") || upperType.equals("NCHAR");
            String collationName = rs.getString("collation_name");
            if (collationName == null || collationName.isEmpty()) {
              collationName = "Latin1_General_BIN";
            }
            com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper
                    .CollationReference
                collation =
                    com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper
                        .CollationReference.builder()
                        .setDbCharacterSet("UTF8")
                        .setDbCollation(collationName)
                        .setPadSpace(padSpace)
                        .build();
            infoBuilder.setCollationReference(collation);
            int maxLength = rs.getInt("max_length");
            if (maxLength <= 0) {
              maxLength = 255;
            } else if (upperType.startsWith("N")) {
              maxLength = maxLength / 2;
            }
            infoBuilder.setStringMaxLength(maxLength);
          } else if (indexType == SourceColumnIndexInfo.IndexType.DECIMAL) {
            infoBuilder.setNumericScale(rs.getInt("scale"));
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
    if (exception instanceof SQLTimeoutException) {
      return true;
    }
    if (exception.getSQLState() != null && "HY008".equalsIgnoreCase(exception.getSQLState())) {
      return true;
    }
    if (exception.getErrorCode() == 1222) {
      return true;
    }
    return false;
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    String sanitizedCollation =
        (dbCollation == null || dbCollation.isEmpty() || !dbCollation.matches("^[a-zA-Z0-9_]+$"))
            ? "Latin1_General_BIN"
            : dbCollation;
    return "WITH Nums AS ("
        + " SELECT TOP 256 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n"
        + " FROM sys.all_objects a CROSS JOIN sys.all_objects b"
        + "), "
        + "CharsWithFlags AS ("
        + " SELECT"
        + "   n,"
        + "   NCHAR(n) AS charset_char,"
        + "   CAST(CASE WHEN ('a' + NCHAR(n) + 'a') COLLATE "
        + sanitizedCollation
        + "     = 'aa' COLLATE "
        + sanitizedCollation
        + " THEN 1 ELSE 0 END AS BIT) AS is_empty,"
        + "   CAST(CASE WHEN ('a' + NCHAR(n) + 'a') COLLATE "
        + sanitizedCollation
        + "     = 'a a' COLLATE "
        + sanitizedCollation
        + " THEN 1 ELSE 0 END AS BIT) AS is_space"
        + " FROM Nums"
        + "), "
        + "Equivalents AS ("
        + " SELECT"
        + "   n,"
        + "   charset_char,"
        + "   is_empty,"
        + "   is_space,"
        + "   FIRST_VALUE(charset_char) OVER ("
        + "     PARTITION BY charset_char COLLATE "
        + sanitizedCollation
        + ", is_empty"
        + "     ORDER BY charset_char COLLATE "
        + sanitizedCollation
        + ", n"
        + "   ) AS equivalent_charset_char,"
        + "   FIRST_VALUE(charset_char) OVER ("
        + "     PARTITION BY charset_char COLLATE "
        + sanitizedCollation
        + ", is_empty, is_space"
        + "     ORDER BY charset_char COLLATE "
        + sanitizedCollation
        + ", n"
        + "   ) AS equivalent_charset_char_pad_space"
        + " FROM CharsWithFlags"
        + ") "
        + "SELECT "
        + "  charset_char,"
        + "  equivalent_charset_char,"
        + "  CAST(DENSE_RANK() OVER ("
        + "    PARTITION BY is_empty"
        + "    ORDER BY equivalent_charset_char COLLATE "
        + sanitizedCollation
        + "  ) - 1 AS BIGINT) AS codepoint_rank,"
        + "  is_empty,"
        + "  is_space,"
        + "  equivalent_charset_char_pad_space,"
        + "  CAST(DENSE_RANK() OVER ("
        + "    PARTITION BY is_empty, is_space"
        + "    ORDER BY equivalent_charset_char_pad_space COLLATE "
        + sanitizedCollation
        + "  ) - 1 AS BIGINT) AS codepoint_rank_pad_space "
        + "FROM Equivalents "
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
