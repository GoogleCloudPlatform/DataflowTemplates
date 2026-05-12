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
  private final Map<ColumnKey, String> columnCastWrappers =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final Map<ColumnKey, String> columnParameterCastWrappers =
      new java.util.concurrent.ConcurrentHashMap<>();

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
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
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
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }
    logger.info(
        "Discovering table schema for Datasource: {}, JdbcSchemaReference: {}, tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    final String query =
        "SELECT table_name,"
            + "  column_name,"
            + "  data_type,"
            + "  character_maximum_length,"
            + "  numeric_precision,"
            + "  numeric_scale"
            + " FROM information_schema.columns"
            + " WHERE table_catalog = ?"
            + "  AND table_schema = ?"
            + "  AND table_name IN "
            + DialectAdapter.generateInClause(tables.size());

    Map<String, ImmutableMap.Builder<String, SourceColumnType>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableMap.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(query)) {
      statement.setFetchSize(1000);
      statement.setString(1, sourceSchemaReference.dbName());
      statement.setString(2, sourceSchemaReference.namespace());
      for (int i = 0; i < tables.size(); i++) {
        statement.setString(i + 3, tables.get(i));
      }
      logger.info("Executing query " + query + ": " + statement);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          final String tableName = resultSet.getString("table_name");
          final String columnName = resultSet.getString("column_name");
          final String columnType = resultSet.getString("data_type");
          final long characterMaximumLength = resultSet.getLong("character_maximum_length");
          boolean typeHasMaximumCharacterLength = !resultSet.wasNull();
          final long numericPrecision = resultSet.getLong("numeric_precision");
          boolean typeHasPrecision = !resultSet.wasNull();
          final long numericScale = resultSet.getLong("numeric_scale");
          boolean typeHasScale = !resultSet.wasNull();
          SourceColumnType sourceColumnType;
          if (typeHasMaximumCharacterLength) {
            sourceColumnType =
                new SourceColumnType(columnType, new Long[] {characterMaximumLength}, null);
          } else if (typeHasPrecision && typeHasScale) {
            sourceColumnType =
                new SourceColumnType(columnType, new Long[] {numericPrecision, numericScale}, null);
          } else if (typeHasPrecision) {
            sourceColumnType =
                new SourceColumnType(columnType, new Long[] {numericPrecision}, null);
          } else {
            sourceColumnType = new SourceColumnType(columnType, new Long[] {}, null);
          }
          if (builders.containsKey(tableName)) {
            builders.get(tableName).put(columnName, sourceColumnType);
          }
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
    }

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> result =
        ImmutableMap.builder();
    builders.forEach((table, builder) -> result.put(table, builder.build()));

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> tableSchema = result.build();
    logger.info(
        "Discovered table schema for Datasource: {}, JdbcSchemaReference: {}, tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

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
    if (tables.isEmpty()) {
      return ImmutableMap.of();
    }
    logger.info(
        "Discovering Indexes for DataSource: {}, JdbcSchemaReference: {}, Tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);

    // https://www.db-fiddle.com/f/eUSGErdEWNQL8FhMj99Kb7/0
    final String query =
        "SELECT ixs.tablename AS table_name,"
            + "  a.attname AS column_name,"
            + "  ixs.indexname AS index_name,"
            + "  ix.indisunique AS is_unique,"
            + "  ix.indisprimary AS is_primary,"
            + "("
            + "SELECT SUM(pc.reltuples)"
            + " FROM pg_catalog.pg_class pc"
            + "  WHERE pc.oid IN ("
            + "  SELECT inhrelid FROM pg_catalog.pg_inherits WHERE inhparent = c.oid"
            + "  UNION"
            + "  SELECT c.oid"
            + " )"
            + ") AS cardinality,"
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
            + "  AND ixs.tablename IN "
            + DialectAdapter.generateInClause(tables.size())
            + " ORDER BY ix.indexrelid, ordinal_position ASC;";

    Map<String, ImmutableList.Builder<SourceColumnIndexInfo>> builders = new HashMap<>();
    tables.forEach(table -> builders.put(table, ImmutableList.builder()));

    try (Connection conn = dataSource.getConnection();
        PreparedStatement statement = conn.prepareStatement(query)) {
      statement.setFetchSize(1000);
      statement.setString(1, sourceSchemaReference.namespace());
      for (int i = 0; i < tables.size(); i++) {
        statement.setString(i + 2, tables.get(i));
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          final String tableName = resultSet.getString("table_name");
          final String typeName = resultSet.getString("type_name");
          final SourceColumnIndexInfo.IndexType indexType =
              indexTypeFrom(resultSet.getString("type_category"), typeName);
          SourceColumnIndexInfo.Builder indexBuilder =
              SourceColumnIndexInfo.builder()
                  .setColumnName(resultSet.getString("column_name"))
                  .setIndexName(resultSet.getString("index_name"))
                  .setIsUnique(resultSet.getBoolean("is_unique"))
                  .setIsPrimary(resultSet.getBoolean("is_primary"))
                  .setCardinality(resultSet.getLong("cardinality"))
                  .setOrdinalPosition(resultSet.getLong("ordinal_position"))
                  .setIndexType(indexType);

          String collation = resultSet.getString("collation");
          if ("uuid".equalsIgnoreCase(typeName)) {
            // PostgreSQL UUID type lacks a physical collation, but sorts lexicographically/binary.
            // We map it to a virtual "UUID" collation to trigger the static UUID mapper in
            // CollationMapper.
            collation = "UUID";
          }
          if (collation != null) {
            String charset = resultSet.getString("charset");
            if (charset == null) {
              charset = "UTF8";
            }
            Integer typeLength = resultSet.getInt("type_length");
            if (resultSet.wasNull()) {
              typeLength = null;
            }
            if (typeLength == null && "uuid".equalsIgnoreCase(typeName)) {
              // A standard UUID has 36 characters, but since hyphens are stripped inside
              // CollationMapper,
              // the serialized representation length is exactly 32 characters.
              typeLength = 32;
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
          if ("uuid".equalsIgnoreCase(typeName)) {
            ColumnKey key = new ColumnKey(tableName, resultSet.getString("column_name"));
            // PostgreSQL requires explicit casting to TEXT for MIN/MAX boundary queries on UUIDs,
            // and requires explicit casting back to UUID when binding parameter placeholders.
            columnCastWrappers.put(key, "CAST(%s AS TEXT)");
            columnParameterCastWrappers.put(key, "CAST(? AS uuid)");
          }
          if (builders.containsKey(tableName)) {
            builders.get(tableName).add(indexBuilder.build());
          }
        }
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
    }

    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> result =
        ImmutableMap.builder();
    builders.forEach((table, builder) -> result.put(table, builder.build()));

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> tableIndexes = result.build();
    logger.info(
        "Discovered Indexes for DataSource: {}, JdbcSchemaReference: {}, Tables: {}",
        dataSource,
        sourceSchemaReference,
        tables);
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
    return addWhereClause("SELECT * FROM " + tableName, tableName, partitionColumns);
  }

  /**
   * Get query for the prepared statement to count a given range.
   *
   * @param tableName name of the table to read.
   * @param partitionColumns partition columns.
   * @param timeoutMillis timeout of the count query in milliseconds. Set to 0 to disable timeout.
   *     Note that PG does not have an easy way of adding a server level timeout hint in the single
   *     statement. The client side prepared statement timeout which is set by {@link
   *     com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.ReadWithUniformPartitions
   *     ReadWithUniformPartitions} will help in capping the time query spends in counting the rows.
   * @return Query Statement.
   */
  @Override
  public String getCountQuery(
      String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
    return addWhereClause(
        String.format("SELECT COUNT(*) FROM %s", tableName), tableName, partitionColumns);
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
    String selectCol = colName;
    String wrapper = columnCastWrappers.get(new ColumnKey(tableName, colName));
    if (wrapper != null) {
      selectCol = String.format(wrapper, colName);
    }
    return addWhereClause(
        String.format("SELECT MIN(%s), MAX(%s) FROM %s", selectCol, selectCol, tableName),
        tableName,
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

  private String addWhereClause(
      String query, String tableName, ImmutableList<String> partitionColumns) {
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
                  partitionColumn -> {
                    String paramPlaceholder =
                        columnParameterCastWrappers.getOrDefault(
                            new ColumnKey(tableName, partitionColumn), "?");
                    return String.format(
                        "((? = FALSE) OR (%1$s >= %2$s AND (%1$s < %2$s OR (? = TRUE AND %1$s = %2$s))))",
                        partitionColumn, paramPlaceholder);
                  })
              .collect(Collectors.joining(" AND ")));
    }
    return queryBuilder.toString();
  }

  /**
   * Ref <a
   * href="https://www.postgresql.org/docs/16/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE"></a>.
   */
  private SourceColumnIndexInfo.IndexType indexTypeFrom(String typeCategory, String typeName) {
    switch (typeCategory) {
      case "N":
        return SourceColumnIndexInfo.IndexType.NUMERIC;
      case "D":
        return SourceColumnIndexInfo.IndexType.TIME_STAMP;
      case "S":
        return SourceColumnIndexInfo.IndexType.STRING;
      case "U":
        return indexTypeForUserDefinedType(typeName);
      default:
        return SourceColumnIndexInfo.IndexType.OTHER;
    }
  }

  private SourceColumnIndexInfo.IndexType indexTypeForUserDefinedType(String typeName) {
    if (typeName == null) {
      return SourceColumnIndexInfo.IndexType.OTHER;
    }
    switch (typeName.toUpperCase()) {
      case "UUID":
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

  /**
   * A composite key representing a specific table and column combination. Used to track
   * type-specific state across different lifecycle phases (e.g., mapping unorderable types
   * discovered during index inspection to custom SQL casting wrappers used during query
   * generation). Normalizes identifiers to ensure case-insensitive, quote-stripped matching logic
   * is consistent.
   */
  private static final class ColumnKey implements Serializable {
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
      return identifier.replace("\"", "").toLowerCase();
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
      return java.util.Objects.hash(tableName, columnName);
    }
  }
}
