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

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.CHARSET_REPLACEMENT_TAG;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.COLLATION_REPLACEMENT_TAG;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.RETURN_TYPE_REPLACEMENT_TAG;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.replaceTagsAndSanitize;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.resourceAsString;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter for PostgreSQL dialect of JDBC databases. */
public class PostgreSQLDialectAdapter implements DialectAdapter {

  public enum PostgreSQLVersion {
    DEFAULT,
  }

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLDialectAdapter.class);

  private static final int VARCHAR_MAX_LENGTH = 65535;

  // SQLState / Error codes
  // Ref: <a href="https://www.postgresql.org/docs/current/errcodes-appendix.html"></a>
  private static final String SQL_STATE_ER_QUERY_CANCELLED = "57014";
  private static final String SQL_STATE_ER_LOCK_TIMEOUT = "55P03";
  private static final Set<String> TIMEOUT_SQL_STATES =
      Sets.newHashSet(SQL_STATE_ER_QUERY_CANCELLED, SQL_STATE_ER_LOCK_TIMEOUT);

  // Errors
  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);

  // Collations
  private static final String COLLATIONS_QUERY_RESOURCE_PATH =
      "sql/postgresql_collation_order_query.sql";

  private static final String PAD_SPACE_RETURN_TYPE = "CHAR(5)";
  private static final String NO_PAD_SPACE_RETURN_TYPE = "TEXT";

  private final PostgreSQLVersion version;

  public PostgreSQLDialectAdapter(PostgreSQLVersion version) {
    this.version = version;
  }

  /**
   * Discover Tables to migrate. This method could be used to auto infer tables to migrate if not
   * passed via options. You could try this in <a
   * href="https://www.db-fiddle.com/f/kWijwWoQLf1obsovToWiV2/0">db-fiddle</a>.
   *
   * @param dataSource Provider for JDBC connection.
   * @return The list of table names for the given database.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   *     <p><b>Note:</b>
   *     <p>This Implementation logs every exception and generate metrics as appropriate.
   */
  @Override
  public ImmutableList<String> discoverTables(
      DataSource dataSource, JdbcSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info("Discovering tables for DataSource: {}", dataSource);

    final String query =
        "SELECT table_name"
            + " FROM information_schema.tables"
            + " WHERE table_type = 'BASE TABLE'"
            + "  AND table_catalog = ?"
            + "  AND table_schema = ?";

    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (PreparedStatement stmt = dataSource.getConnection().prepareStatement(query)) {
      stmt.setString(1, sourceSchemaReference.dbName());
      stmt.setString(2, sourceSchemaReference.namespace());
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          tablesBuilder.add(rs.getString("table_name"));
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

  /**
   * Discover the schema of tables to migrate. You could try this in <a
   * href="https://www.db-fiddle.com/f/vYbnwUtfoXaKHkV7PMKxa/1">db-fiddle</a>.
   *
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return source table schema.
   * @throws SchemaDiscoveryException - Fatal exception during Schema discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema discovery.
   */
  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        "Discovering table schema for Datasource: {}, JdbcSchemaReference: {}, tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    final String query =
        "SELECT column_name,"
            + "  data_type,"
            + "  character_maximum_length,"
            + "  numeric_precision,"
            + "  numeric_scale"
            + " FROM information_schema.columns"
            + " WHERE table_catalog = ?"
            + "  AND table_schema = ?"
            + "  AND table_name = ?";

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> tableSchemaBuilder =
        ImmutableMap.builder();
    try (PreparedStatement statement = dataSource.getConnection().prepareStatement(query)) {
      for (String table : tables) {
        statement.setString(1, sourceSchemaReference.dbName());
        statement.setString(2, sourceSchemaReference.namespace());
        statement.setString(3, table);
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
        "Discovered table schema for Datasource: {}, JdbcSchemaReference: {}, tables: {}, schema: {}",
        dataSource,
        sourceSchemaReference,
        tables,
        tableSchema);

    return tableSchema;
  }

  /**
   * Discover the indexes of tables to migrate. You can try this in <a
   * href="https://www.db-fiddle.com/f/kTanXYXoM2VgCjSf6NZHjD/6">db-fiddle</a>.
   *
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return The discovered indexes.
   * @throws SchemaDiscoveryException - Fatal exception during Schema Discovery.
   * @throws RetriableSchemaDiscoveryException - Retriable exception during Schema Discovery.
   */
  @Override
  public ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoverTableIndexes(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        "Discovering Indexes for DataSource: {}, JdbcSchemaReference: {}, Tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    final String query =
        "SELECT a.attname AS column_name,"
            + "  ixs.indexname AS index_name,"
            + "  ix.indisunique AS is_unique,"
            + "  ix.indisprimary AS is_primary,"
            + "  c.reltuples AS cardinality,"
            + "  a.attnum AS ordinal_position,"
            + "  t.typname AS type_name,"
            + "  information_schema._pg_char_max_length(a.atttypid, a.atttypmod) AS type_length,"
            + "  t.typcategory AS type_category,"
            + "  ico.collation_name AS collation,"
            + "  ico.pad_attribute AS pad,"
            + "  pg_encoding_to_char(d.encoding) AS charset"
            + " FROM pg_catalog.pg_indexes ixs"
            + "  JOIN pg_catalog.pg_class c ON c.relname = ixs.indexname"
            + "  JOIN pg_catalog.pg_index ix ON c.oid = ix.indexrelid"
            + "  JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid"
            + "  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid"
            + "  LEFT OUTER JOIN pg_catalog.pg_collation co ON co.oid = ix.indcollation[a.attnum - 1]"
            + "  LEFT OUTER JOIN information_schema.collations ico ON ico.collation_name = co.collname"
            + "  LEFT OUTER JOIN pg_catalog.pg_database d ON d.datname = current_database()"
            + " WHERE ixs.schemaname = ?"
            + "  AND ixs.tablename = ?"
            + " ORDER BY ix.indexrelid, ordinal_position ASC;";
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> tableIndexesBuilder =
        ImmutableMap.builder();
    try (PreparedStatement statement = dataSource.getConnection().prepareStatement(query)) {
      for (String table : tables) {
        statement.setString(1, sourceSchemaReference.namespace());
        statement.setString(2, table);
        ImmutableList.Builder<SourceColumnIndexInfo> indexInfosBuilder = ImmutableList.builder();
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            SourceColumnIndexInfo.Builder indexBuilder =
                SourceColumnIndexInfo.builder()
                    .setColumnName(resultSet.getString("column_name"))
                    .setIndexName(resultSet.getString("index_name"))
                    .setIsUnique(resultSet.getBoolean("is_unique"))
                    .setIsPrimary(resultSet.getBoolean("is_primary"))
                    .setCardinality(resultSet.getLong("cardinality"))
                    .setOrdinalPosition(resultSet.getLong("ordinal_position"))
                    .setIndexType(indexTypeFrom(resultSet.getString("type_category")));

            String collation = resultSet.getString("collation");
            if (collation != null) {
              String charset = resultSet.getString("charset");
              String typeName = resultSet.getString("type_name");
              Integer typeLength = resultSet.getInt("type_length");
              if (resultSet.wasNull()) {
                typeLength = null;
              }
              // Collation PAD SPACE is not supported in Postgresql
              // (https://www.postgresql.org/docs/current/infoschema-collations.html)
              // The only way to have blank space padding is for specific types with fixed length
              // (https://www.postgresql.org/docs/current/datatype-character.html)
              boolean shouldPadSpace = isBlankPaddedType(typeName, typeLength);
              indexBuilder.setCollationReference(
                  CollationReference.builder()
                      .setDbCharacterSet(charset)
                      .setDbCollation(collation)
                      .setPadSpace(shouldPadSpace)
                      .build());
              indexBuilder.setStringMaxLength(typeLength == null ? VARCHAR_MAX_LENGTH : typeLength);
            }
            indexInfosBuilder.add(indexBuilder.build());
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
        "Discovered Indexes for DataSource: {}, JdbcSchemaReference: {}, Tables: {}.\nIndexes: {}",
        dataSource,
        sourceSchemaReference,
        tables,
        tableIndexes);
    return tableIndexes;
  }

  /**
   * Get query for the prepared statement to read columns within a range.
   *
   * @param tableName name of the table to read
   * @param partitionColumns partition columns.
   * @return Query Statement.
   */
  @Override
  public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
    return addWhereClause("SELECT * FROM " + tableName, partitionColumns);
  }

  /**
   * Get query for the prepared statement to count a given range.
   *
   * @param tableName name of the table to read.
   * @param partitionColumns partition columns.
   * @param timeoutMillis timeout of the count query in milliseconds. Set to 0 to disable timeout.
   * @return Query Statement.
   */
  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause(
        String.format(
            "SET statement_timeout = %s; SELECT COUNT(*) FROM %s", timeoutMillis, tableName),
        partitionColumns);
  }

  /**
   * Get query for the prepared statement to get min and max of a given column, optionally in the
   * context of a parent range.
   *
   * @param tableName name of the table to read.
   * @param partitionColumns if not-empty, partition columns. Set empty for first column of
   *     partitioning.
   */
  @Override
  public String getBoundaryQuery(
      String tableName, ImmutableList<String> partitionColumns, String colName) {
    return addWhereClause(
        String.format("SELECT MIN(%s), MAX(%s) FROM %s", colName, colName, tableName),
        partitionColumns);
  }

  /**
   * Check if a given {@link SQLException} is a timeout. The implementation needs to check for
   * dialect specific {@link SQLException#getSQLState() SqlState} to check if the exception
   * indicates a server side timeout. The client side timeout would be already checked for by
   * handling {@link SQLTimeoutException}, so the implementation does not need to check for the
   * same.
   */
  @Override
  public boolean checkForTimeout(SQLException exception) {
    return exception.getSQLState() != null
        && TIMEOUT_SQL_STATES.contains(exception.getSQLState().toUpperCase());
  }

  /**
   * Ref <a href="https://www.db-fiddle.com/f/sJyGyFpqfnoxYFpEXPxR1/0"></a> Get Query that returns
   * order of collation. The query must return all the characters in the character set with the
   * columns listed in {@link CollationOrderRow.CollationsOrderQueryColumns}.
   *
   * @param dbCharset character set used by the database for which collation ordering has to be
   *     found.
   * @param dbCollation collation set used by the database for which collation ordering has to be
   *     found.
   * @param padSpace pad space used by the database for which collation ordering has to be found. If
   *     this is set to true, we use a fixed length character type to construct the query, which
   *     ignores trailing space. If this is set to false, we use a variable length character type
   *     instead.
   */
  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    String query = resourceAsString(COLLATIONS_QUERY_RESOURCE_PATH);
    Map<String, String> tags = new HashMap<>();
    tags.put(CHARSET_REPLACEMENT_TAG, dbCharset);
    tags.put(COLLATION_REPLACEMENT_TAG, dbCollation);
    tags.put(
        RETURN_TYPE_REPLACEMENT_TAG, padSpace ? PAD_SPACE_RETURN_TYPE : NO_PAD_SPACE_RETURN_TYPE);
    return replaceTagsAndSanitize(query, tags);
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

  /**
   * Ref <a
   * href="https://www.postgresql.org/docs/16/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE"></a>.
   */
  private SourceColumnIndexInfo.IndexType indexTypeFrom(String typeCategory) {
    switch (typeCategory) {
      case "N":
        return SourceColumnIndexInfo.IndexType.NUMERIC;
      case "D":
        return SourceColumnIndexInfo.IndexType.DATE_TIME;
      case "S":
        return SourceColumnIndexInfo.IndexType.STRING;
      default:
        return SourceColumnIndexInfo.IndexType.OTHER;
    }
  }

  /** Ref <a href="https://www.postgresql.org/docs/current/datatype-character.html"></a>. */
  private boolean isBlankPaddedType(String typeName, @Nullable Integer typeLength) {
    String upperTypeName = typeName.toUpperCase();
    return typeLength != null
        && (upperTypeName.equals("CHARACTER")
            || upperTypeName.equals("CHAR")
            || upperTypeName.equals("BPCHAR"));
  }
}
