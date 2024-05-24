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

import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcSourceRowMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Pattern;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter for MySql dialect of JDBC databases. */
public final class MysqlDialectAdapter implements DialectAdapter {
  private final MySqlVersion mySqlVersion;

  private static final Logger logger = LoggerFactory.getLogger(MysqlDialectAdapter.class);

  private final Counter schemaDiscoveryErrors =
      Metrics.counter(JdbcSourceRowMapper.class, MetricCounters.READER_SCHEMA_DISCOVERY_ERRORS);

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
      DataSource dataSource, SourceSchemaReference sourceSchemaReference)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {

    logger.info(String.format("Discovering tables for DataSource: %s", dataSource));
    final String tableDiscoveryQuery =
        String.format("SHOW TABLES in %s", sourceSchemaReference.dbName());
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
              "Sql exception while discovering table list for datasource=%s", dataSource, e));
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
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        String.format(
            "Discovering tale schema for Datasource: %s, SourceSchemaReference: %s, tables: %s",
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
            "Discovered tale schema for Datasource: %s, SourceSchemaReference: %s, tables: %s, schema: %s",
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
      SourceSchemaReference sourceSchemaReference,
      ImmutableList<String> tables)
      throws SchemaDiscoveryException, RetriableSchemaDiscoveryException {
    logger.info(
        String.format(
            "Discovering Indexes for DataSource: %s, SourceSchemaReference: %s, Tables: %s",
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
            "Discovered Indexes for DataSource: %s, SourceSchemaReference: %s, Tables: %s.\nIndexes: %s",
            dataSource, sourceSchemaReference, tables, tableIndexes));
    return tableIndexes;
  }

  protected static String getSchemaDiscoveryQuery(SourceSchemaReference sourceSchemaReference) {
    return "SELECT "
        + String.join(",", InformationSchemaCols.colList())
        + " FROM INFORMATION_SCHEMA.Columns WHERE TABLE_SCHEMA = "
        + "'"
        + sourceSchemaReference.dbName()
        + "'"
        + " AND"
        + " TABLE_NAME = ?";
  }

  protected static String getIndexDiscoveryQuery(SourceSchemaReference sourceSchemaReference) {
    return "SELECT "
        + String.join(",", InformationSchemaStatsCols.colList())
        + " FROM INFORMATION_SCHEMA.STATISTICS stats"
        + " JOIN "
        + "INFORMATION_SCHEMA.COLUMNS cols"
        + " ON "
        + "stats.table_schema = cols.table_schema"
        + " AND stats.table_name = cols.table_name"
        + " AND stats.column_name = cols.column_name"
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
          .put("BIGINT", IndexType.NUMERIC)
          .put("DATETIME", IndexType.DATE_TIME)
          .put("INTEGER", IndexType.NUMERIC)
          .put("INTEGER UNSIGNED", IndexType.NUMERIC)
          .put("MEDIUMINT", IndexType.NUMERIC)
          .put("SMALLINT", IndexType.NUMERIC)
          .put("TINYINT", IndexType.NUMERIC)
          .build();

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
        String columType = normalizeColumnType(rs.getString(InformationSchemaStatsCols.TYPE_COL));
        IndexType indexType = INDEX_TYPE_MAPPING.getOrDefault(columType, IndexType.OTHER);

        indexesBuilder.add(
            SourceColumnIndexInfo.builder()
                .setColumnName(colName)
                .setIndexName(indexName)
                .setIsUnique(isUnique)
                .setIsPrimary(isPrimary)
                .setCardinality(cardinality)
                .setOrdinalPosition(ordinalPosition)
                .setIndexType(indexType)
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

  /**
   * Version of MySql. As of now the code does not need to distinguish between versions of Mysql.
   * Having the type allows the implementation do finer distinctions if needed in the future.
   */
  public enum MySqlVersion {
    DEFAULT,
  }

  protected static final class InformationSchemaCols {
    public static final String NAME_COL = "COLUMN_NAME";
    public static final String TYPE_COL = "DATA_TYPE";
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

    public static final String TYPE_COL = "cols.DATA_TYPE";

    public static ImmutableList<String> colList() {
      return ImmutableList.of(
          COL_NAME_COL, INDEX_NAME_COL, ORDINAL_POS_COL, NON_UNIQ_COL, CARDINALITY_COL, TYPE_COL);
    }

    private InformationSchemaStatsCols() {}
  }
}
