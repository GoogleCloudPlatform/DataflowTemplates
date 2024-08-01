/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.postgresql;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLDialectAdapter implements DialectAdapter {

  public enum PostgreSQLVersion {
    DEFAULT,
  }

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLDialectAdapter.class);

  // SQLState / Error codes
  // Ref: <a href="https://www.postgresql.org/docs/current/errcodes-appendix.html"></a>
  private static final String SQL_STATE_ER_QUERY_CANCELLED = "57014";
  private static final String SQL_STATE_ER_LOCK_TIMEOUT = "55P03";
  private static final Set<String> TIMEOUT_SQL_STATES =
      Sets.newHashSet(SQL_STATE_ER_QUERY_CANCELLED, SQL_STATE_ER_LOCK_TIMEOUT);

  // Information schema / System tables constants
  private static final ImmutableList<String> EXCLUDED_SCHEMAS =
      ImmutableList.of("information_schema");
  private static final String EXCLUDED_SCHEMAS_STR = String.join(",", EXCLUDED_SCHEMAS);
  private static final char SCHEMA_TABLE_SEPARATOR = '.';

  // Errors
  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);

  private PostgreSQLVersion version;

  public PostgreSQLDialectAdapter(PostgreSQLVersion version) {
    this.version = version;
  }

  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info("Discovering tables for DataSource: {}", dataSource);

    final String query =
        String.format(
            "SELECT table_schema,"
                + "  table_name"
                + " FROM information_schema.tables"
                + " WHERE table_type = 'BASE TABLE'"
                + "  AND table_catalog = ?"
                + "  AND table_schema NOT LIKE 'pg_%%'"
                + "  AND table_schema NOT IN (%s)",
            EXCLUDED_SCHEMAS_STR);

    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (PreparedStatement stmt = dataSource.getConnection().prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      logger.info("Executing query " + query + ": " + stmt);
      try (ResultSet rs = stmt.executeQuery()) {
        StringBuilder tableName = new StringBuilder();
        while (rs.next()) {
          tableName.append(rs.getString("table_schema"));
          tableName.append(SCHEMA_TABLE_SEPARATOR);
          tableName.append(rs.getString("table_name"));
          tablesBuilder.add(tableName.toString());
          tableName.setLength(0);
        }
        ImmutableList<String> tables = tablesBuilder.build();
        logger.info("Discovered tables for DataSource: {}, tables: {}", dataSource, tables);
        return tables;
      }
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          "Transient connection error while discovering table list for datasource={}",
          dataSource,
          e);
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          "Non Transient connection error while discovering table list for datasource={}",
          dataSource,
          e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error("Sql exception while discovering table list for datasource={}", dataSource, e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    }
  }

  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        "Discovering table schema for Datasource: {}, SourceSchemaReference: {}, tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    final String query =
        String.format(
            "SELECT column_name,"
                + "  data_type,"
                + "  character_maximum_length,"
                + "  numeric_precision,"
                + "  numeric_scale"
                + " FROM information_schema.columns"
                + " WHERE table_catalog = ?"
                + "  AND (table_schema || '.' || table_name) = ?"
                + "  AND table_schema NOT LIKE 'pg_%%'"
                + "  AND table_schema NOT IN (%s)",
            EXCLUDED_SCHEMAS_STR);

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> tableSchemaBuilder =
        ImmutableMap.builder();
    try (PreparedStatement statement = dataSource.getConnection().prepareStatement(query)) {
      for (String table : tables) {
        statement.setString(1, sourceSchemaReference.dbName());
        statement.setString(2, table);
        logger.info("Executing query " + query + ": " + statement);
        try (ResultSet resultSet = statement.executeQuery()) {
          ImmutableMap.Builder<String, SourceColumnType> schema = ImmutableMap.builder();
          while (resultSet.next()) {
            SourceColumnType sourceColumnType;
            final String columnName = resultSet.getString("column_name");
            final String columnType = resultSet.getString("data_type");
            final long characterMaximumLength = resultSet.getLong("character_maximum_length");
            boolean typeHasMaximumCharacterLength = !resultSet.wasNull();
            final long numericPrecision = resultSet.getLong("numeric_precision");
            boolean typeHasPrecision = !resultSet.wasNull();
            final long numericScale = resultSet.getLong("numeric_scale");
            boolean typeHasScale = !resultSet.wasNull();
            if (typeHasMaximumCharacterLength) {
              sourceColumnType =
                  new SourceColumnType(columnType, new Long[] {characterMaximumLength}, null);
            } else if (typeHasPrecision && typeHasScale) {
              sourceColumnType =
                  new SourceColumnType(
                      columnType, new Long[] {numericPrecision, numericScale}, null);
            } else if (typeHasPrecision) {
              sourceColumnType =
                  new SourceColumnType(columnType, new Long[] {numericPrecision}, null);
            } else {
              sourceColumnType = new SourceColumnType(columnType, new Long[] {}, null);
            }
            schema.put(columnName, sourceColumnType);
          }
          tableSchemaBuilder.put(table, schema.build());
        }
      }
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          "Transient connection error while discovering table schema for datasource={} db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          "Non Transient connection error while discovering table schema for datasource={}, db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          "Sql exception while discovering table schema for datasource={} db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      // Already logged.
      schemaDiscoveryErrors.inc();
      throw e;
    }
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchema =
        tableSchemaBuilder.build();
    logger.info(
        "Discovered tale schema for Datasource: {}, SourceSchemaReference: {}, tables: {}, schema: {}",
        dataSource,
        sourceSchemaReference,
        tables,
        tableSchema);

    return tableSchema;
  }

  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        "Discovering Indexes for DataSource: {}, SourceSchemaReference: {}, Tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    final String query =
        String.format(
            "SELECT a.attname as column_name,"
                + "  ixs.indexname as index_name,"
                + "  ix.indisunique as is_unique,"
                + "  ix.indisprimary as is_primary,"
                + "  ix.indnkeyatts as cardinality,"
                + "  a.attnum as ordinal_position,"
                + "  t.typcategory as type_category"
                + " FROM pg_catalog.pg_indexes ixs"
                + "  JOIN pg_catalog.pg_class c on c.relname = ixs.indexname"
                + "  JOIN pg_catalog.pg_index ix on c.oid = ix.indexrelid"
                + "  JOIN pg_catalog.pg_attribute a on c.oid = a.attrelid"
                + "  JOIN pg_catalog.pg_type t on t.oid = a.atttypid"
                + " WHERE (ixs.schemaname || '.' || ixs.tablename) = ?"
                + "  AND ixs.schemaname NOT LIKE 'pg_%%'"
                + "  AND ixs.schemaname NOT IN (%s)"
                + " ORDER BY ix.indexrelid, ordinal_position ASC;",
            EXCLUDED_SCHEMAS_STR);
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> tableIndexesBuilder =
        ImmutableMap.builder();
    try (PreparedStatement statement = dataSource.getConnection().prepareStatement(query)) {
      for (String table : tables) {
        statement.setString(1, table);
        ImmutableList.Builder<SourceColumnIndexInfo> indexInfosBuilder = ImmutableList.builder();
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            indexInfosBuilder.add(
                SourceColumnIndexInfo.builder()
                    .setColumnName(resultSet.getString("column_name"))
                    .setIndexName(resultSet.getString("index_name"))
                    .setIsUnique(resultSet.getBoolean("is_unique"))
                    .setIsPrimary(resultSet.getBoolean("is_primary"))
                    .setCardinality(resultSet.getLong("cardinality"))
                    .setOrdinalPosition(resultSet.getLong("ordinal_position"))
                    .setIndexType(indexTypeFrom(resultSet.getString("type_category")))
                    .build());
          }
        }
        tableIndexesBuilder.put(table, indexInfosBuilder.build());
      }
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          "Transient connection error while discovering table indexes for datasource={} db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          "Non Transient connection error while discovering table indexes for datasource={}, db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          "Sql exception while discovering table schema for datasource={} db={} tables={}",
          dataSource,
          sourceSchemaReference,
          tables,
          e);
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      // Already logged.
      schemaDiscoveryErrors.inc();
      throw e;
    }
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> tableIndexes =
        tableIndexesBuilder.build();
    logger.info(
        "Discovered Indexes for DataSource: {}, SourceSchemaReference: {}, Tables: {}.\nIndexes: {}",
        dataSource,
        sourceSchemaReference,
        tables,
        tableIndexes);
    return tableIndexes;
  }

  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    return addWhereClause("SELECT * FROM " + tableName, partitionColumns);
  }

  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause(
        String.format(
            "SET statement_timeout = %s; SELECT COUNT(*) FROM %s", timeoutMillis, tableName),
        partitionColumns);
  }

  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    return addWhereClause(
        String.format("SELECT MIN(%s), MAX(%s) FROM %s", colName, colName, tableName),
        partitionColumns);
  }

  @Override
  public boolean checkForTimeout(SQLException exception) {
    return exception.getSQLState() != null
        && TIMEOUT_SQL_STATES.contains(exception.getSQLState().toUpperCase());
  }

  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation) {
    // TODO(thiagotnunes)
    return "";
  }

  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(query);
    if (!partitionColumns.isEmpty()) {
      queryBuilder.append(" WHERE ");
      queryBuilder.append(
          partitionColumns.stream()
              // Include the column / range to define the where clause.
              // `(exclude col = FALSE) OR (col >= range.start() AND (col < range.end() OR
              // (range.isLast() = TRUE AND col = range.end()))`
              .map(
                  partitionColumn ->
                      String.format(
                          "((? = FALSE) OR (%1$s >= ? AND (%1$s < ? OR (? = TRUE AND %1$s = ?))))",
                          partitionColumn))
              .collect(Collectors.joining(" AND ")));
    }
    return queryBuilder.toString();
  }

  // Ref <a
  // href="https://www.postgresql.org/docs/16/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE"></a>
  private SourceColumnIndexInfo.IndexType indexTypeFrom(String typeCategory) {
    switch (typeCategory) {
      case "N":
        return SourceColumnIndexInfo.IndexType.NUMERIC;
      case "D":
        return SourceColumnIndexInfo.IndexType.DATE_TIME;
      default:
        return SourceColumnIndexInfo.IndexType.OTHER;
    }
  }
}
