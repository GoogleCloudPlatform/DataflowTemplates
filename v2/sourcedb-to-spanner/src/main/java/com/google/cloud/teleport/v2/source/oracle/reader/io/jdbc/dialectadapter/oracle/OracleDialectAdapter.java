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
package com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.dialectadapter.oracle;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDialectAdapter implements DialectAdapter {

  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);
  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDialectAdapter.class);

  private String quote(String identifier) {
    if (identifier == null) {
      return null;
    }
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
      if (firstDone) {
        queryBuilder.append(" AND ");
      }
      queryBuilder.append("((TO_CHAR(?) IN ('0', 'false', 'FALSE')) OR ");
      String quotedPart = quote(partitionColumn);
      queryBuilder.append(
          String.format(
              "(%1$s >= ? AND (%1$s < ? OR (TO_CHAR(?) IN ('1', 'true', 'TRUE') AND %1$s = ?)))",
              quotedPart));
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
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause("SELECT COUNT(*) FROM " + quote(tableName), partitionColumns);
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    return addWhereClause(
        "SELECT MIN(" + quote(colName) + "), MAX(" + quote(colName) + ") FROM " + quote(tableName),
        partitionColumns);
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    if (exception instanceof java.sql.SQLTimeoutException) {
      return true;
    }
    int errorCode = exception.getErrorCode();
    if (errorCode == 1013 || errorCode == 3156) {
      // ORA-01013: user requested cancel (query timeout)
      // ORA-03156: OCI call timed out
      return true;
    }
    return false;
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
      String schemaPattern =
          sourceSchemaReference.namespace() != null
              ? sourceSchemaReference.namespace()
              : metaData.getUserName();
      try (ResultSet rs = metaData.getTables(null, schemaPattern, null, new String[] {"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          // Safely bypass Oracle Recycle Bin objects natively
          if (tableName != null && tableName.startsWith("BIN$")) {
            continue;
          }
          tablesBuilder.add(tableName);
        }
      }
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering tables for datasource=%s db=%s, cause=%s",
              dataSource, sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering tables for datasource=%s db=%s, cause=%s",
              dataSource, sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering tables for datasource=%s db=%s, cause=%s",
              dataSource, sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering tables for datasource=%s db=%s, cause=%s",
              dataSource, sourceSchemaReference, e));
      schemaDiscoveryErrors.inc();
      throw e;
    }
    ImmutableList<String> tables = tablesBuilder.build();
    LOGGER.info("Discovered Oracle Tables: {}", tables);
    return tables;
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference schemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Map<String, ImmutableMap.Builder<String, SourceColumnType>> builders =
        tables.stream().collect(Collectors.toMap(t -> t, t -> ImmutableMap.builder()));

    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      String schemaPattern =
          schemaReference.namespace() != null
              ? schemaReference.namespace()
              : metaData.getUserName();
      for (String table : tables) {
        try (ResultSet rs = metaData.getColumns(null, schemaPattern, table, null)) {
          while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            if (typeName != null) {
              typeName = typeName.replaceAll("\\([0-9]+\\)", "");
            }
            long colSize = rs.getLong("COLUMN_SIZE");
            boolean hasColSize = !rs.wasNull();
            long decimalDigits = rs.getLong("DECIMAL_DIGITS");
            boolean hasDecimalDigits = !rs.wasNull();

            Long[] mods;
            if (hasColSize && hasDecimalDigits) {
              mods = new Long[] {colSize, decimalDigits};
            } else if (hasColSize) {
              mods = new Long[] {colSize};
            } else if (hasDecimalDigits) {
              mods = new Long[] {decimalDigits};
            } else {
              mods = new Long[] {};
            }

            SourceColumnType record = new SourceColumnType(typeName, mods, null);
            builders.get(table).put(colName, record);
          }
        }
        LOGGER.info("Discovered Table Schema for {}: {}", table, builders.get(table).build());
      }
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, schemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, schemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, schemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, schemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw e;
    }
    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((k, v) -> result.put(k, v.build()));
    return result.build();
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      javax.sql.DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    Map<String, ImmutableList.Builder<SourceColumnIndexInfo>> builders =
        tables.stream().collect(Collectors.toMap(t -> t, t -> ImmutableList.builder()));

    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      String schemaPattern =
          sourceSchemaReference.namespace() != null
              ? sourceSchemaReference.namespace()
              : metaData.getUserName();
      for (String table : tables) {
        try (ResultSet rs = metaData.getPrimaryKeys(null, schemaPattern, table)) {
          while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String pkName = rs.getString("PK_NAME");
            long seq = rs.getShort("KEY_SEQ");

            SourceColumnIndexInfo.IndexType type = SourceColumnIndexInfo.IndexType.OTHER;
            String columnTypeName = "";
            int stringMaxLength = 0;
            try (ResultSet crs = metaData.getColumns(null, schemaPattern, table, colName)) {
              if (crs.next()) {
                String typeName = crs.getString("TYPE_NAME");
                if (typeName != null) {
                  columnTypeName = typeName.replaceAll("\\([0-9]+\\)", "");
                  String typeNameUpper = typeName.toUpperCase();
                  if (typeNameUpper.contains("CHAR") || typeNameUpper.contains("CLOB")) {
                    type = SourceColumnIndexInfo.IndexType.STRING;
                    stringMaxLength = crs.getInt("COLUMN_SIZE");
                  } else if (typeNameUpper.contains("INT") || typeNameUpper.contains("NUM")) {
                    type = SourceColumnIndexInfo.IndexType.NUMERIC;
                  } else if (typeNameUpper.contains("DATE") || typeNameUpper.contains("TIME")) {
                    type = SourceColumnIndexInfo.IndexType.TIME_STAMP;
                  }
                }
              }
            }

            SourceColumnIndexInfo.Builder infoBuilder =
                SourceColumnIndexInfo.builder()
                    .setColumnName(colName)
                    .setIsPrimary(true)
                    .setIsUnique(true)
                    .setOrdinalPosition(seq)
                    .setIndexName(pkName != null ? pkName : "PRIMARY")
                    .setIndexType(type)
                    .setColumnTypeName(columnTypeName);

            if (type == SourceColumnIndexInfo.IndexType.STRING) {
              CollationReference emptyCollation =
                  CollationReference.builder()
                      .setDbCharacterSet("UTF8")
                      .setDbCollation("UTF8_BIN")
                      .setPadSpace(false)
                      .build();
              infoBuilder.setCollationReference(emptyCollation);
              infoBuilder.setStringMaxLength(stringMaxLength);
            }

            builders.get(table).add(infoBuilder.build());
          }
        }
      }
    } catch (SQLTransientConnectionException e) {
      LOGGER.warn(
          String.format(
              "Transient connection error while discovering table indexes for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      LOGGER.error(
          String.format(
              "Non Transient connection error while discovering table indexes for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      LOGGER.error(
          String.format(
              "Sql exception while discovering table indexes for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      LOGGER.error(
          String.format(
              "Schema discovery exception while discovering table indexes for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw e;
    }

    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();
    builders.forEach((k, v) -> result.put(k, v.build()));
    return result.build();
  }
}
