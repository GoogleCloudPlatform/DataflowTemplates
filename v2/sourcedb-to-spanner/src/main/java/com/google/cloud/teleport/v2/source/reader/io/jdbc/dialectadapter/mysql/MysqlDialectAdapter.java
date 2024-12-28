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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.CHARSET_REPLACEMENT_TAG;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.COLLATION_REPLACEMENT_TAG;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.replaceTagsAndSanitize;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.ResourceUtils.resourceAsString;
import static org.apache.curator.shaded.com.google.common.collect.Sets.newHashSet;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Pattern;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter for MySql dialect of JDBC databases. */
public final class MysqlDialectAdapter implements DialectAdapter {

  public static final String PAD_SPACE = "PAD SPACE";
  private final MySqlVersion mySqlVersion;

  private static final Logger logger = LoggerFactory.getLogger(MysqlDialectAdapter.class);

  /**
   * Ref: <a
   * href=https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html#error_er_query_interrupted>error_er_query_interrupted</a>.
   */
  private static final String SQL_STATE_ER_QUERY_INTERRUPTED = "70100";

  private static final HashSet<String> TIMEOUT_SQL_STATES =
      newHashSet(SQL_STATE_ER_QUERY_INTERRUPTED);

  /**
   * Ref: <a
   * href=https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html#error_er_query_interrupted>error_er_query_interrupted</a>.
   */
  private static final Integer ER_QUERY_INTERRUPTED = 1317;

  /** Ref <a href=>https://bugs.mysql.com/bug.php?id=96537>bug/96537</a>. */
  private static final Integer ER_FILSORT_ABORT = 1028;

  /** Ref <a href=>https://bugs.mysql.com/bug.php?id=96537>bug/96537</a>. */
  private static final Integer ER_FILSORT_TERMINATED = 10930;

  /**
   * Ref: <a
   * href=https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html#error_er_query_timeout>error_er_query_timeout</a>.
   */
  private static final Integer ER_QUERY_TIMEOUT = 3024;

  private static final HashSet<Integer> TIMEOUT_SQL_ERROR_CODES =
      newHashSet(ER_QUERY_INTERRUPTED, ER_FILSORT_ABORT, ER_FILSORT_TERMINATED, ER_QUERY_TIMEOUT);

  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);

  private static final String COLLATIONS_QUERY_RESOURCE_PATH =
      "sql/mysql_collation_order_query.sql";

  public MysqlDialectAdapter(MySqlVersion mySqlVersion) {
    this.mySqlVersion = mySqlVersion;
  }

  /**
   * Discover Tables to migrate. This method could be used to auto infer tables to migrate if not
   * passed via options.
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

    logger.info(String.format("Discovering tables for DataSource: %s", dataSource));
    final String tableDiscoveryQuery =
        String.format(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE "
                + "TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '%s' ",
            sourceSchemaReference.dbName());
    ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
    try (Statement stmt = dataSource.getConnection().createStatement()) {
      ResultSet rs = stmt.executeQuery(tableDiscoveryQuery);
      while (rs.next()) {
        tablesBuilder.add(rs.getString(1));
      }
      ImmutableList<String> tables = tablesBuilder.build();
      logger.info(
          String.format("Discovered tables for DataSource: %s, tables: %s", dataSource, tables));
      return tables;
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          String.format(
              "Transient connection error while discovering table list for datasource=%s",
              dataSource, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          String.format(
              "Non Transient connection error while discovering table list for datasource=%s",
              dataSource, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          String.format(
              "Sql exception while discovering table list for datasource=%s cause=%s",
              dataSource, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    }
  }

  /**
   * @param dataSource Provider for JDBC connection.
   * @param sourceSchemaReference Source database name and (optionally namespace)
   * @param tables Tables to migrate.
   * @return source table schema.
   * @throws SchemaDiscoveryException
   * @throws RetriableSchemaDiscoveryException
   *     <p><b>Note:</b>
   *     <p><b>Implementation Detail:</b>There is a choice to read from information schema table via
   *     a prepared statement, or read the same information through jdbc metaadata calls. The major
   *     part of parasing and mapping complexity is same in either route. We choose the route of
   *     reading information schema table directly, since:
   *     <ol>
   *       <li>With all other things equal, using a prepared statement to read information schema
   *           would be faster than making individual calls.
   *       <li>Various drivers have different properties that can affect the search scope of the
   *           calls.
   *     </ol>
   */
  @Override
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DataSource dataSource,
      JdbcSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        String.format(
            "Discovering table schema for Datasource: %s, JdbcSchemaReference: %s, tables: %s",
            dataSource, sourceSchemaReference, tables));

    String discoveryQuery = getSchemaDiscoveryQuery(sourceSchemaReference);

    ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> tableSchemaBuilder =
        ImmutableMap.<String, ImmutableMap<String, SourceColumnType>>builder();
    try (PreparedStatement statement =
        dataSource.getConnection().prepareStatement(discoveryQuery)) {
      tables.forEach(table -> tableSchemaBuilder.put(table, getTableCols(table, statement)));
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          String.format(
              "Transient connection error while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          String.format(
              "Non Transient connection error while discovering table schema for datasource=%s, db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          String.format(
              "Sql exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
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
        String.format(
            "Discovered table schema for Datasource: %s, JdbcSchemaReference: %s, tables: %s, schema: %s",
            dataSource, sourceSchemaReference, tables, tableSchema));

    return tableSchema;
  }

  /**
   * Discover the indexes of tables to migrate.
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
        String.format(
            "Discovering Indexes for DataSource: %s, JdbcSchemaReference: %s, Tables: %s",
            dataSource, sourceSchemaReference, tables));
    String discoveryQuery = getIndexDiscoveryQuery(sourceSchemaReference);
    ImmutableMap.Builder<String, ImmutableList<SourceColumnIndexInfo>> tableIndexesBuilder =
        ImmutableMap.<String, ImmutableList<SourceColumnIndexInfo>>builder();

    try (PreparedStatement statement =
        dataSource.getConnection().prepareStatement(discoveryQuery)) {
      tables.forEach(table -> tableIndexesBuilder.put(table, getTableIndexes(table, statement)));
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          String.format(
              "Transient connection error while discovering table indexes for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          String.format(
              "Non Transient connection error while discovering table indexes for datasource=%s, db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      schemaDiscoveryErrors.inc();
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          String.format(
              "Sql exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
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
        String.format(
            "Discovered Indexes for DataSource: %s, JdbcSchemaReference: %s, Tables: %s.\nIndexes: %s",
            dataSource, sourceSchemaReference, tables, tableIndexes));
    return tableIndexes;
  }

  protected static String getSchemaDiscoveryQuery(JdbcSchemaReference sourceSchemaReference) {
    return "SELECT "
        + String.join(",", InformationSchemaCols.colList())
        + " FROM INFORMATION_SCHEMA.Columns WHERE TABLE_SCHEMA = "
        + "'"
        + sourceSchemaReference.dbName()
        + "'"
        + " AND"
        + " TABLE_NAME = ?";
  }

  /**
   * Discover Indexed columns and their Collations(if applicable). You could try this on <a href =
   * https://www.db-fiddle.com/f/kRVPA5jDwZYNj2rsdtif4K/3>db-fiddle</a>
   *
   * @param sourceSchemaReference
   * @return
   */
  protected static String getIndexDiscoveryQuery(JdbcSchemaReference sourceSchemaReference) {
    return "SELECT *"
        + " FROM INFORMATION_SCHEMA.STATISTICS stats"
        + " JOIN "
        + "INFORMATION_SCHEMA.COLUMNS cols"
        + " ON "
        + "stats.table_schema = cols.table_schema"
        + " AND stats.table_name = cols.table_name"
        + " AND stats.column_name = cols.column_name"
        + " LEFT JOIN "
        + "INFORMATION_SCHEMA.COLLATIONS collations"
        + " ON "
        + "cols.COLLATION_NAME = collations.COLLATION_NAME"
        + " WHERE stats.TABLE_SCHEMA = "
        + "'"
        + sourceSchemaReference.dbName()
        + "'"
        + " AND"
        + " stats.TABLE_NAME = ?";
  }

  private ImmutableMap<String, SourceColumnType> getTableCols(
      String table, PreparedStatement statement) throws SchemaDiscoveryException {
    var colsBuilder = ImmutableMap.<String, SourceColumnType>builder();
    try {
      statement.setString(1, table);
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        String colName = rs.getString(InformationSchemaCols.NAME_COL);
        SourceColumnType colType = resultSetToSourceColumnType(rs);
        colsBuilder.put(colName, colType);
      }
    } catch (java.sql.SQLException e) {
      logger.error(
          String.format(
              "Sql error while discovering table schema with statement=%s table=%s, cause=%s",
              statement, table, e));
      throw new SchemaDiscoveryException(e);
    }
    return colsBuilder.build();
  }

  private static final ImmutableMap<String, SourceColumnIndexInfo.IndexType> INDEX_TYPE_MAPPING =
      ImmutableMap.<String, SourceColumnIndexInfo.IndexType>builder()
          .put("BIGINT UNSIGNED", IndexType.BIG_INT_UNSIGNED)
          .put("BIGINT", IndexType.NUMERIC)
          .put("DATETIME", IndexType.DATE_TIME)
          .put("INTEGER", IndexType.NUMERIC)
          .put("INTEGER UNSIGNED", IndexType.NUMERIC)
          .put("MEDIUMINT", IndexType.NUMERIC)
          .put("SMALLINT", IndexType.NUMERIC)
          .put("TINYINT", IndexType.NUMERIC)
          // String types: Ref https://dev.mysql.com/doc/refman/8.4/en/string-type-syntax.html
          .put("CHAR", IndexType.STRING)
          .put("VARCHAR", IndexType.STRING)
          // Mapping BINARY, VARBINARY and TINYBLOB to Java bigInteger
          // Ref https://dev.mysql.com/doc/refman/8.4/en/charset-binary-collations.html
          .put("BINARY", IndexType.BINARY)
          .put("VARBINARY", IndexType.BINARY)
          .put("TINYBLOB", IndexType.BINARY)
          .put("TINYTEXT", IndexType.STRING)
          .build();

  /**
   * Get the PadSpace attribute from {@link ResultSet} for index discovery query {@link
   * #getIndexDiscoveryQuery(JdbcSchemaReference)}. This method takes care of the fact that older
   * versions of MySQL notably Mysql5.7 don't have a {@link
   * InformationSchemaStatsCols#PAD_SPACE_COL} column and default to PAD SPACE comparisons.
   */
  @VisibleForTesting
  @Nullable
  protected String getPadSpaceString(ResultSet resultSet) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      if (metaData.getColumnName(i + 1).equals(InformationSchemaStatsCols.PAD_SPACE_COL)) {
        return resultSet.getString(InformationSchemaStatsCols.PAD_SPACE_COL);
      }
    }
    // For MySql5.7 there is no pad-space column
    logger.info(
        "Did not find {} column in INFORMATION_SCHEMA.COLLATIONS table. Assuming PAD-SPACE collation for non-binary strings as per MySQL5.7 spec",
        InformationSchemaStatsCols.PAD_SPACE_COL);
    return PAD_SPACE;
  }

  private ImmutableList<SourceColumnIndexInfo> getTableIndexes(
      String table, PreparedStatement statement) throws SchemaDiscoveryException {

    ImmutableList.Builder<SourceColumnIndexInfo> indexesBuilder =
        ImmutableList.<SourceColumnIndexInfo>builder();
    try {
      statement.setString(1, table);
      ResultSet rs = statement.executeQuery();
      while (rs.next()) {
        String colName = rs.getString(InformationSchemaStatsCols.COL_NAME_COL);
        String indexName = rs.getString(InformationSchemaStatsCols.INDEX_NAME_COL);
        boolean isUnique = !rs.getBoolean(InformationSchemaStatsCols.NON_UNIQ_COL);
        boolean isPrimary = indexName.trim().toUpperCase().equals("PRIMARY");
        long cardinality = rs.getLong(InformationSchemaStatsCols.CARDINALITY_COL);
        long ordinalPosition = rs.getLong(InformationSchemaStatsCols.ORDINAL_POS_COL);
        @Nullable
        Integer stringMaxLength = rs.getInt(InformationSchemaStatsCols.CHAR_MAX_LENGTH_COL);
        if (rs.wasNull()) {
          stringMaxLength = null;
        }
        @Nullable String characterSet = rs.getString(InformationSchemaStatsCols.CHARACTER_SET_COL);
        @Nullable String collation = rs.getString(InformationSchemaStatsCols.COLLATION_COL);
        @Nullable String padSpace = getPadSpaceString(rs);
        logger.debug(
            "Discovered column {} from index {}, isUnique {}, isPrimary {}, cardinality {}, ordinalPosition {}, character-set {}, collation {}, pad-space {}",
            colName,
            indexName,
            isUnique,
            isPrimary,
            cardinality,
            ordinalPosition,
            characterSet,
            collation,
            padSpace);
        // TODO(vardhanvthigle): MySql 5.7 is always PAD space and does not have PAD_ATTRIBUTE
        // Column.
        String columType = normalizeColumnType(rs.getString(InformationSchemaStatsCols.TYPE_COL));
        IndexType indexType = INDEX_TYPE_MAPPING.getOrDefault(columType, IndexType.OTHER);
        CollationReference collationReference = null;
        if (indexType.equals(IndexType.STRING)) {
          collationReference =
              CollationReference.builder()
                  .setDbCharacterSet(escapeMySql(characterSet))
                  .setDbCollation(escapeMySql(collation))
                  .setPadSpace(
                      (padSpace == null) ? false : padSpace.trim().toUpperCase().equals(PAD_SPACE))
                  .build();
        } else {
          stringMaxLength = null;
        }

        indexesBuilder.add(
            SourceColumnIndexInfo.builder()
                .setColumnName(colName)
                .setIndexName(indexName)
                .setIsUnique(isUnique)
                .setIsPrimary(isPrimary)
                .setCardinality(cardinality)
                .setOrdinalPosition(ordinalPosition)
                .setIndexType(indexType)
                .setCollationReference(collationReference)
                .setStringMaxLength(stringMaxLength)
                .build());
      }
    } catch (java.sql.SQLException e) {
      logger.error(
          String.format(
              "Sql error while discovering table schema with statement=%s table=%s, cause=%s",
              statement, table, e));
      throw new SchemaDiscoveryException(e);
    }
    return indexesBuilder.build();
  }

  @VisibleForTesting
  protected static String escapeMySql(String input) {
    if (input.startsWith("`")) {
      return input;
    } else {
      return "`" + input + "`";
    }
  }

  private SourceColumnType resultSetToSourceColumnType(ResultSet rs) throws SQLException {
    String colType = normalizeColumnType(rs.getString(InformationSchemaCols.TYPE_COL));
    long charMaxLength = rs.getLong(InformationSchemaCols.CHAR_MAX_LENGTH_COL);
    boolean isStringTypeColum = !rs.wasNull();
    if (isStringTypeColum) {
      return new SourceColumnType(colType, new Long[] {charMaxLength}, null);
    }
    long numericPrecision = rs.getLong(InformationSchemaCols.NUMERIC_PRECISION_COL);
    boolean typeHasPrecision = !rs.wasNull();
    long numericScale = rs.getLong(InformationSchemaCols.NUMERIC_SCALE_COL);
    boolean typeHasScale = !rs.wasNull();
    if (typeHasPrecision && typeHasScale) {
      return new SourceColumnType(colType, new Long[] {numericPrecision, numericScale}, null);
    } else if (typeHasPrecision) {
      return new SourceColumnType(colType, new Long[] {numericPrecision}, null);
    } else {
      return new SourceColumnType(colType, new Long[] {}, null);
    }
  }

  private static final Pattern normalizeColumnTypeDisplayWidths = Pattern.compile("\\([^()]*\\)");
  private static final Pattern normalizedColumnTypeMultiSpaces = Pattern.compile("( )+");
  private static final ImmutableMap<String, String> mySQlTypeAliases =
      ImmutableMap.of(
          "DOUBLE PRECISION",
          "DOUBLE",
          "DEC",
          "DECIMAL",
          "INT",
          "INTEGER",
          "INT UNSIGNED",
          "INTEGER UNSIGNED",
          "BOOLEAN",
          "BOOL");

  private String normalizeColumnType(String columnType) {

    // Remove Display Widths, for example FLOAT(5) Becomes FLOAT.
    // Note that for numeric types, Display widths have nothing to do with data precision.
    // Precision and scale are conveyed by the respective columns in the system tables where ever
    // relevant.
    // For string types, the width is also present in `CHARACTER_MAX_LENGTH` column, which is what
    // we use for mapping.
    String columnTypeWithoutDisplayWidth =
        normalizeColumnTypeDisplayWidths.matcher(columnType).replaceAll("").toUpperCase();

    // Remove Unsigned as the unified type mapping (except for BIGINT) does not care about unsigned
    // type.
    // TODO(vardhanvthigle): CHECK HOW does SIGNED INTEGER MAPPING work,
    //  if it does not, we might need to deviate a bit from the unified mapping.
    //  Mapping signed/unsigned small and medium integers to integer will always work.
    String normalizedType = columnTypeWithoutDisplayWidth;
    if (!columnTypeWithoutDisplayWidth.contains("BIGINT")
        && !columnTypeWithoutDisplayWidth.startsWith("INT")) {
      normalizedType = normalizedType.replaceAll("UNSIGNED", "");
    }
    // Removing Display widths or `Unsigned` can potentially leave with either multiple spaces or
    // tailing white space.
    normalizedType = normalizedColumnTypeMultiSpaces.matcher(normalizedType).replaceAll(" ").trim();

    // Some types are aliased, for example `int` and `integer` are aliased.
    return mySQlTypeAliases.getOrDefault(normalizedType, normalizedType);
  }

  private String addWhereClause(String query, ImmutableList<String> partitionColumns) {

    // Implementation detail, using StringBuilder since we are generating the query in a loop.
    StringBuilder queryBuilder = new StringBuilder(query);

    boolean firstDone = false;
    for (String partitionColumn : partitionColumns) {

      if (firstDone) {
        // Add AND for iteration after first.
        queryBuilder.append(" AND ");
      } else {
        // add `where` only for first iteration.
        queryBuilder.append(" WHERE ");
      }

      // Include the column?
      queryBuilder.append("((? = FALSE) OR ");
      // range to define the where clause. `col >= range.start() AND (col < range.end() OR
      // (range.isLast() = TRUE AND col = range.end()))`
      queryBuilder.append(
          String.format("(%1$s >= ? AND (%1$s < ? OR (? = TRUE AND %1$s = ?)))", partitionColumn));
      queryBuilder.append(")");
      firstDone = true;
    }
    return queryBuilder.toString();
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
    return addWhereClause("select * from " + tableName, partitionColumns);
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
            "select /*+ MAX_EXECUTION_TIME(%s) */ COUNT(*) from %s", timeoutMillis, tableName),
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
        String.format("select MIN(%s),MAX(%s) from %s", colName, colName, tableName),
        partitionColumns);
  }

  /**
   * Check if a given {@link SQLException} is a timeout. The implementation needs to check for
   * dialect specific {@link SQLException#getSQLState() SqlState} and {@link
   * SQLException#getErrorCode() ErrorCode} to check if the exception indicates a server side
   * timeout. The client side timeout would be already checked for by handling {@link
   * SQLTimeoutException}, so the implementation does not need to check for the same.
   */
  @Override
  public boolean checkForTimeout(SQLException exception) {
    if (exception.getSQLState() != null) {
      if (TIMEOUT_SQL_STATES.contains(exception.getSQLState().toLowerCase())) {
        return true;
      }
    }
    if (TIMEOUT_SQL_ERROR_CODES.contains(exception.getErrorCode())) {
      return true;
    }
    return false;
  }

  /**
   * Get Query that returns order of collation. The query must return all the characters in the
   * character set with the columns listed in {@link CollationsOrderQueryColumns}.
   *
   * @param dbCharset character set used by the database for which collation ordering has to be
   *     found.
   * @param dbCollation collation set used by the database for which collation ordering has to be
   *     found.
   * @param padSpace pad space used by the database for which collation ordering has to be found.
   */
  @Override
  public String getCollationsOrderQuery(String dbCharset, String dbCollation, boolean padSpace) {
    String query = resourceAsString(COLLATIONS_QUERY_RESOURCE_PATH);
    Map<String, String> tags = new HashMap<>();
    tags.put("'" + CHARSET_REPLACEMENT_TAG + "'", "'" + dbCharset + "'");
    tags.put("'" + COLLATION_REPLACEMENT_TAG + "'", "'" + dbCollation + "'");
    // Queries with size > max_allowed_packet get rejected by
    // the db. max_allowed_packet is generally around 16Mb which is a lot for our use case.
    return replaceTagsAndSanitize(query, tags);
  }

  /**
   * Version of MySql. As of now the code does not need to distinguish between versions of Mysql.
   * Having the type allows the implementation do finer distinctions if needed in the future.
   */
  public enum MySqlVersion {
    DEFAULT,
  }

  protected static final class InformationSchemaCols {
    public static final String NAME_COL = "COLUMN_NAME";
    public static final String TYPE_COL = "COLUMN_TYPE";
    public static final String CHAR_MAX_LENGTH_COL = "CHARACTER_MAXIMUM_LENGTH";
    public static final String NUMERIC_PRECISION_COL = "NUMERIC_PRECISION";
    public static final String NUMERIC_SCALE_COL = "NUMERIC_SCALE";

    public static ImmutableList<String> colList() {
      return ImmutableList.of(
          NAME_COL, TYPE_COL, CHAR_MAX_LENGTH_COL, NUMERIC_PRECISION_COL, NUMERIC_SCALE_COL);
    }

    private InformationSchemaCols() {}
  }

  protected static final class InformationSchemaStatsCols {
    public static final String COL_NAME_COL = "stats.COLUMN_NAME";
    public static final String INDEX_NAME_COL = "stats.INDEX_NAME";
    public static final String ORDINAL_POS_COL = "stats.SEQ_IN_INDEX";
    public static final String NON_UNIQ_COL = "stats.NON_UNIQUE";
    public static final String CARDINALITY_COL = "stats.CARDINALITY";

    public static final String TYPE_COL = "cols.COLUMN_TYPE";
    public static final String CHAR_MAX_LENGTH_COL = "cols.CHARACTER_MAXIMUM_LENGTH";
    public static final String CHARACTER_SET_COL = "cols.CHARACTER_SET_NAME";
    public static final String COLLATION_COL = "cols.COLLATION_NAME";

    // TODO(vardhanvthigle): MySql 5.7 is always PAD space and does not have PAD_ATTRIBUTE Column.
    public static final String PAD_SPACE_COL = "collations.PAD_ATTRIBUTE";

    public static ImmutableList<String> colList() {
      return ImmutableList.of(
          COL_NAME_COL,
          INDEX_NAME_COL,
          ORDINAL_POS_COL,
          NON_UNIQ_COL,
          CARDINALITY_COL,
          TYPE_COL,
          CHAR_MAX_LENGTH_COL,
          CHARACTER_SET_COL,
          COLLATION_COL,
          PAD_SPACE_COL);
    }

    private InformationSchemaStatsCols() {}
  }
}
