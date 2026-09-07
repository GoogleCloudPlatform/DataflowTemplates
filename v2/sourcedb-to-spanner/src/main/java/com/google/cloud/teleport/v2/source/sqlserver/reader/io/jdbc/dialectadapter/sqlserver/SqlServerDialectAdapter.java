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

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
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
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDialectAdapter implements DialectAdapter {

  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDialectAdapter.class);

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
    LOGGER.info("Discovering tables.");

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
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering tables for db=%s, cause=%s",
              sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering tables for db=%s, cause=%s",
              sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering tables for db=%s, cause=%s",
              sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering tables for db=%s, cause=%s",
              sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw e;
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
    LOGGER.info(
        String.format(
            "Discovering table schema for JdbcSchemaReference: %s, tables: %s",
            sourceSchemaReference, tables));

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
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering table schema for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering table schema for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering table schema for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering table schema for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw e;
    }

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((t, b) -> result.put(t, b.build()));

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchema = result.build();
    LOGGER.info(
        String.format(
            "Discovered table schema for JdbcSchemaReference: %s, tables: %s",
            sourceSchemaReference, tables));
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
    LOGGER.info(
        String.format(
            "Discovering Indexes for JdbcSchemaReference: %s, Tables: %s",
            sourceSchemaReference, tables));
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
            CollationReference collation =
                CollationReference.builder()
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
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering table indexes for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering table indexes for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering table indexes for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering table indexes for db=%s tables=%s, cause=%s",
              sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw e;
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

  /**
   * Generates a SQL query to extract character collation sort orders and equivalence classes for
   * SQL Server.
   *
   * <p>The query operates as follows:
   *
   * <ul>
   *   <li><b>BaseChars CTE</b>: Generates code points (0 to 65535) using {@code GENERATE_SERIES},
   *       excluding UTF-16 surrogate code points (0xD800 - 0xDFFF / 55296 - 57343).
   *       <ul>
   *         <li>{@code KeyAllPos}: Character enclosed with non-space sentinels (e.g. 'X' + char +
   *             'X') to isolate comparison from ANSI SQL-92 PAD SPACE behavior across all
   *             positions.
   *         <li>{@code KeyPadSpace}: Character at trailing position (e.g. 'X' + char), subject to
   *             ANSI SQL-92 PAD SPACE equality rules.
   *       </ul>
   *   <li><b>CharsWithFlags CTE</b>: Identifies special character properties under the collation.
   *       <ul>
   *         <li>{@code is_empty}: True if equivalent to '\0' or zero-width (empty string) at all
   *             positions.
   *         <li>{@code is_space}: True if equivalent to ' ' at all positions.
   *       </ul>
   *   <li><b>EquivalenceRanks CTE</b>:
   *       <ul>
   *         <li>{@code codepointRank}: 0-offset rank partitioned by {@code is_empty} based on
   *             collation sort order across all positions, guaranteeing contiguous indexing for
   *             non-empty characters in {@link CollationIndex}.
   *         <li>{@code codepointRankPadSpace}: 0-offset rank partitioned by {@code is_empty} and
   *             {@code is_space} based on collation sort order at trailing position.
   *         <li>{@code MinCodePointAllPos}: Lowest code point in the {@code (KeyAllPos, is_empty)}
   *             equivalence group (canonical representative across all positions).
   *         <li>{@code MinCodePointPadSpace}: Lowest code point in the {@code (KeyPadSpace,
   *             is_empty, is_space)} equivalence group (canonical representative at trailing
   *             position).
   *       </ul>
   *   <li><b>Main SELECT</b>:
   *       <ul>
   *         <li>Projects {@code charset_char}, {@code equivalent_charset_char}, {@code
   *             codepoint_rank}, {@code equivalent_charset_char_pad_space}, {@code
   *             codepoint_rank_pad_space}, {@code is_empty}, and {@code is_space}.
   *         <li>Orders by {@code is_empty}, {@code codepointRank}, and {@code CodePoint}.
   *       </ul>
   * </ul>
   */
  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    // TODO: currently this is only suppose to work for Unicode 2 byte charset and
    // Latin1_General_100_CI_AS_SC
    // collation. It need to be generalised for all charset and collations.
    String sanitizedCollation =
        (dbCollation == null || dbCollation.isEmpty() || !dbCollation.matches("^[a-zA-Z0-9_]+$"))
            ? "Latin1_General_100_CI_AS_SC"
            : dbCollation;
    return "WITH BaseChars AS (\n"
        + "    SELECT \n"
        + "        s.value AS CodePoint,\n"
        + "        NCHAR(s.value) AS charsetChar,\n"
        + "        CAST(N'X' + NCHAR(s.value) + N'X' AS NVARCHAR(10)) \n"
        + "            COLLATE "
        + sanitizedCollation
        + " AS KeyAllPos,\n"
        + "        CAST(N'X' + NCHAR(s.value) AS NVARCHAR(10)) \n"
        + "            COLLATE "
        + sanitizedCollation
        + " AS KeyPadSpace\n"
        + "    FROM GENERATE_SERIES(0, 65535) s\n"
        + "    WHERE s.value NOT BETWEEN 55296 AND 57343\n"
        + "),\n"
        + "CharsWithFlags AS (\n"
        + "    SELECT \n"
        + "        CodePoint,\n"
        + "        charsetChar,\n"
        + "        KeyAllPos,\n"
        + "        KeyPadSpace,\n"
        + "        CAST(CASE \n"
        + "            WHEN KeyAllPos = CAST(N'X' + NCHAR(0) + N'X' AS NVARCHAR(10)) COLLATE "
        + sanitizedCollation
        + " \n"
        + "                OR KeyAllPos = CAST(N'XX' AS NVARCHAR(10)) COLLATE "
        + sanitizedCollation
        + " \n"
        + "            THEN 1 ELSE 0 \n"
        + "        END AS BIT) AS is_empty,\n"
        + "        CAST(CASE \n"
        + "            WHEN KeyAllPos = CAST(N'X' + N' ' + N'X' AS NVARCHAR(10)) COLLATE "
        + sanitizedCollation
        + " \n"
        + "            THEN 1 ELSE 0 \n"
        + "        END AS BIT) AS is_space\n"
        + "    FROM BaseChars\n"
        + "),\n"
        + "EquivalenceRanks AS (\n"
        + "    SELECT \n"
        + "        CodePoint,\n"
        + "        charsetChar,\n"
        + "        is_empty,\n"
        + "        is_space,\n"
        + "        DENSE_RANK() OVER (PARTITION BY is_empty ORDER BY KeyAllPos) - 1 AS codepointRank,\n"
        + "        DENSE_RANK() OVER (PARTITION BY is_empty, is_space ORDER BY KeyPadSpace) - 1 AS codepointRankPadSpace,\n"
        + "        MIN(CodePoint) OVER (PARTITION BY KeyAllPos, is_empty) AS MinCodePointAllPos,\n"
        + "        MIN(CodePoint) OVER (PARTITION BY KeyPadSpace, is_empty, is_space) AS MinCodePointPadSpace\n"
        + "    FROM CharsWithFlags\n"
        + ")\n"
        + "SELECT \n"
        + "    charsetChar as charset_char,\n"
        + "    NCHAR(MinCodePointAllPos) AS equivalent_charset_char,\n"
        + "    codepointRank as codepoint_rank,\n"
        + "    NCHAR(MinCodePointPadSpace) AS equivalent_charset_char_pad_space,\n"
        + "    codepointRankPadSpace AS codepoint_rank_pad_space,\n"
        + "    is_empty,\n"
        + "    is_space\n"
        + "FROM EquivalenceRanks\n"
        + "ORDER BY \n"
        + "    is_empty,\n"
        + "    codepointRank,\n"
        + "    CodePoint;";
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
