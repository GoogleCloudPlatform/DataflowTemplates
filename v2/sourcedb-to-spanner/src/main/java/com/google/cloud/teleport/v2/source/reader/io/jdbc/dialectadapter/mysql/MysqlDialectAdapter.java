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

import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
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
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter for MySql dialect of JDBC databases. */
public final class MysqlDialectAdapter implements DialectAdapter {
  private final MySqlVersion mySqlVersion;

  private static final Logger logger = LoggerFactory.getLogger(MysqlDialectAdapter.class);

  public MysqlDialectAdapter(MySqlVersion mySqlVersion) {
    this.mySqlVersion = mySqlVersion;
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

    String discoveryQuery = getSchemaDiscoveryQuery(sourceSchemaReference);

    var tablesBuilder = ImmutableMap.<String, ImmutableMap<String, SourceColumnType>>builder();
    try (PreparedStatement statement =
        dataSource.getConnection().prepareStatement(discoveryQuery)) {
      tables.forEach(table -> tablesBuilder.put(table, getTableCols(table, statement)));
    } catch (SQLTransientConnectionException e) {
      logger.warn(
          String.format(
              "Transient connection error while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      // TODO: Add metrics for transient connection errors.
      throw new RetriableSchemaDiscoveryException(e);
    } catch (SQLNonTransientConnectionException e) {
      logger.error(
          String.format(
              "Non Transient connection error while discovering table schema for datasource=%s, db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      // TODO: Add metrics for non-transient connection errors.
      throw new SchemaDiscoveryException(e);
    } catch (SQLException e) {
      logger.error(
          String.format(
              "Sql exception while discovering table schema for datasource=%s db=%s tables=%s, cause=%s",
              dataSource, sourceSchemaReference, tables, e));
      // TODO: Add metrics for SQL exceptions.
      throw new SchemaDiscoveryException(e);
    } catch (SchemaDiscoveryException e) {
      // Already logged.
      throw e;
    }
    return tablesBuilder.build();
  }

  protected static String getSchemaDiscoveryQuery(SourceSchemaReference sourceSchemaReference) {
    return new StringBuffer()
        .append("SELECT ")
        .append(String.join(",", InformationSchemaCols.colList()))
        .append(
            String.format(
                " FROM INFORMATION_SCHEMA.Columns WHERE TABLE_SCHEMA = %s AND",
                sourceSchemaReference.dbName()))
        .append(" TABLE_NAME = ?")
        .toString();
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
          "DOUBLE PRECISION", "DOUBLE", "DEC", "DECIMAL", "INT", "INTEGER", "BOOLEAN", "BOOL");

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
    if (!columnTypeWithoutDisplayWidth.contains("BIGINT")) {
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
}
