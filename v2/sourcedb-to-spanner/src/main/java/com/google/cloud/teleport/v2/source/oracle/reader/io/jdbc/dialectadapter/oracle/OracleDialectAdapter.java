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

import com.google.cloud.teleport.v2.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDialectAdapter implements DialectAdapter {
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
          tablesBuilder.add(rs.getString("TABLE_NAME"));
        }
      }
    } catch (SQLException e) {
      throw new SchemaDiscoveryException(e);
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
            long decimalDigits = rs.getLong("DECIMAL_DIGITS"); // May be 0 if null
            SourceColumnType record = new SourceColumnType(typeName, new Long[] {colSize}, null);
            builders.get(table).put(colName, record);
          }
        }
        LOGGER.info("Discovered Table Schema for {}: {}", table, builders.get(table).build());
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
      javax.sql.DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables) {
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
            try (ResultSet crs = metaData.getColumns(null, schemaPattern, table, colName)) {
              if (crs.next()) {
                String typeName = crs.getString("TYPE_NAME");
                if (typeName != null) {
                  typeName = typeName.toUpperCase();
                  if (typeName.contains("CHAR") || typeName.contains("CLOB")) {
                    type = SourceColumnIndexInfo.IndexType.STRING;
                  } else if (typeName.contains("INT") || typeName.contains("NUM")) {
                    type = SourceColumnIndexInfo.IndexType.NUMERIC;
                  } else if (typeName.contains("DATE") || typeName.contains("TIME")) {
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
                    .setColumnTypeName("");

            if (type == SourceColumnIndexInfo.IndexType.STRING) {
              CollationReference emptyCollation =
                  CollationReference.builder()
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
      LOGGER.error("Error discovering table indexes", e);
    }

    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();
    builders.forEach((k, v) -> result.put(k, v.build()));
    return result.build();
  }
}
