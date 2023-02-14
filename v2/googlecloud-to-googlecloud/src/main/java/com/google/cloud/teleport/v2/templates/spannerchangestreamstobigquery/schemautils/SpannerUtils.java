/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link SpannerUtils} provides methods that retrieve schema information from Spanner. Note
 * all the models returned in the methods of these class are tracked by the change stream.
 */
public class SpannerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerUtils.class);

  private static final String INFORMATION_SCHEMA_TABLE_NAME = "TABLE_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_TABLE_NAME = "table_name";
  private static final String INFORMATION_SCHEMA_COLUMN_NAME = "COLUMN_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_COLUMN_NAME = "column_name";
  private static final String INFORMATION_SCHEMA_SPANNER_TYPE = "SPANNER_TYPE";
  private static final String INFORMATION_SCHEMA_POSTGRES_SPANNER_TYPE = "spanner_type";
  private static final String INFORMATION_SCHEMA_ORDINAL_POSITION = "ORDINAL_POSITION";
  private static final String INFORMATION_SCHEMA_POSTGRES_ORDINAL_POSITION = "ordinal_position";
  private static final String INFORMATION_SCHEMA_CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String INFORMATION_SCHEMA_POSTGRES_CONSTRAINT_NAME = "constraint_name";
  private static final String INFORMATION_SCHEMA_ALL = "ALL";
  private static final String INFORMATION_SCHEMA_POSTGRES_ALL = "all";

  private DatabaseClient databaseClient;
  private String changeStreamName;
  private Dialect dialect;

  public SpannerUtils(DatabaseClient databaseClient, String changeStreamName, Dialect dialect) {
    this.databaseClient = databaseClient;
    this.changeStreamName = changeStreamName;
    this.dialect = dialect;
  }

  /**
   * @return a map where the key is the table name tracked by the change stream and the value is the
   *     {@link SpannerTable} object of the table name. This function should be called once in the
   *     initialization of the DoFn.
   */
  public Map<String, TrackedSpannerTable> getSpannerTableByName() {
    Set<String> spannerTableNames = getSpannerTableNamesTrackedByChangeStreams();

    Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName =
        getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName();

    return getSpannerTableByName(
        spannerTableNames, spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);
  }

  /**
   * @return a map where the key is the table name tracked by the change stream and the value is the
   *     {@link SpannerTable} object of the table name.
   */
  private Map<String, TrackedSpannerTable> getSpannerTableByName(
      Set<String> spannerTableNames,
      Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    Map<String, Map<String, Integer>> keyColumnNameToOrdinalPositionByTableName =
        getKeyColumnNameToOrdinalPositionByTableName(spannerTableNames);
    Map<String, List<TrackedSpannerColumn>> spannerColumnsByTableName =
        getSpannerColumnsByTableName(
            spannerTableNames,
            keyColumnNameToOrdinalPositionByTableName,
            spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);

    Map<String, TrackedSpannerTable> result = new HashMap<>();
    for (String tableName : spannerColumnsByTableName.keySet()) {
      List<TrackedSpannerColumn> pkColumns = new ArrayList<>();
      List<TrackedSpannerColumn> nonPkColumns = new ArrayList<>();
      Map<String, Integer> keyColumnNameToOrdinalPosition =
          keyColumnNameToOrdinalPositionByTableName.get(tableName);
      for (TrackedSpannerColumn spannerColumn : spannerColumnsByTableName.get(tableName)) {
        if (keyColumnNameToOrdinalPosition.containsKey(spannerColumn.getName())) {
          pkColumns.add(spannerColumn);
        } else {
          nonPkColumns.add(spannerColumn);
        }
      }
      result.put(tableName, new TrackedSpannerTable(tableName, pkColumns, nonPkColumns));
    }

    return result;
  }

  /**
   * Query INFORMATION_SCHEMA.COLUMNS to construct {@link SpannerColumn} for each Spanner column
   * tracked by change stream.
   */
  private Map<String, List<TrackedSpannerColumn>> getSpannerColumnsByTableName(
      Set<String> spannerTableNames,
      Map<String, Map<String, Integer>> keyColumnNameToOrdinalPositionByTableName,
      Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    Map<String, List<TrackedSpannerColumn>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
                + "FROM INFORMATION_SCHEMA.COLUMNS");
    if (this.isPostgres()) {
      // Skip the columns of the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
        sqlStringBuilder.append(
            spannerTableNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
        sqlStringBuilder.append("])");
      }
      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    } else {
      // Skip the columns of the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
      if (!spannerTableNames.isEmpty()) {
        statementBuilder
            .bind("tableNames")
            .to(Value.stringArray(new ArrayList<>(spannerTableNames)));
      }
    }

    try (ResultSet columnsResultSet =
        databaseClient.singleUse().executeQuery(statementBuilder.build())) {
      while (columnsResultSet.next()) {
        String tableName = columnsResultSet.getString(informationSchemaTableName());
        String columnName = columnsResultSet.getString(informationSchemaColumnName());
        // Skip if the columns of the table is tracked explicitly, and the specified column is not
        // tracked. Primary key columns are always tracked.
        if (spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName.containsKey(tableName)
            && !spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName
                .get(tableName)
                .contains(columnName)
            && (!keyColumnNameToOrdinalPositionByTableName.containsKey(tableName)
                || !keyColumnNameToOrdinalPositionByTableName
                    .get(tableName)
                    .containsKey(columnName))) {
          continue;
        }

        int ordinalPosition = (int) columnsResultSet.getLong(informationSchemaOrdinalPosition());
        String spannerType = columnsResultSet.getString(informationSchemaSpannerType());
        result.putIfAbsent(tableName, new ArrayList<>());
        // Set primary key ordinal position for primary key column.
        int pkOrdinalPosition = -1;
        if (!keyColumnNameToOrdinalPositionByTableName.containsKey(tableName)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot find key column for change stream %s and table %s",
                  changeStreamName, tableName));
        }
        if (keyColumnNameToOrdinalPositionByTableName.get(tableName).containsKey(columnName)) {
          pkOrdinalPosition =
              keyColumnNameToOrdinalPositionByTableName.get(tableName).get(columnName);
        }
        TrackedSpannerColumn spannerColumn =
            TrackedSpannerColumn.create(
                columnName,
                informationSchemaTypeToSpannerType(spannerType),
                ordinalPosition,
                pkOrdinalPosition);
        result.get(tableName).add(spannerColumn);
      }
    }

    return result;
  }

  /**
   * Query INFORMATION_SCHEMA.KEY_COLUMN_USAGE to get the names of the primary key columns that are
   * tracked by change stream. We need to know the primary keys information to be able to set {@link
   * TableRow} or do Spanner snapshot read, the alternative way is to extract the primary key
   * information from {@link Mod} whenever we process it, but it's less efficient, since that will
   * require to parse the types and sort them based on the ordinal positions for each {@link Mod}.
   */
  private Map<String, Map<String, Integer>> getKeyColumnNameToOrdinalPositionByTableName(
      Set<String> spannerTableNames) {
    Map<String, Map<String, Integer>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      StringBuilder sqlStringBuilder =
          new StringBuilder(
              "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                  + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

      // Skip the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
        sqlStringBuilder.append(
            spannerTableNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
        sqlStringBuilder.append("])");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    } else {
      StringBuilder sqlStringBuilder =
          new StringBuilder(
              "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                  + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

      // Skip the tables that are not tracked by change stream.
      if (!spannerTableNames.isEmpty()) {
        sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
      }

      statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
      if (!spannerTableNames.isEmpty()) {
        statementBuilder
            .bind("tableNames")
            .to(Value.stringArray(new ArrayList<>(spannerTableNames)));
      }
    }

    try (ResultSet keyColumnsResultSet =
        databaseClient.singleUse().executeQuery(statementBuilder.build())) {
      while (keyColumnsResultSet.next()) {
        String tableName = keyColumnsResultSet.getString(informationSchemaTableName());
        String columnName = keyColumnsResultSet.getString(informationSchemaColumnName());
        // Note The ordinal position of primary key in INFORMATION_SCHEMA.KEY_COLUMN_USAGE table
        // is different from the ordinal position from INFORMATION_SCHEMA.COLUMNS table.
        int ordinalPosition = (int) keyColumnsResultSet.getLong(informationSchemaOrdinalPosition());
        String constraintName = keyColumnsResultSet.getString(informationSchemaConstraintName());
        // We are only interested in primary key constraint.
        if (isPrimaryKey(constraintName)) {
          result.putIfAbsent(tableName, new HashMap<>());
          result.get(tableName).put(columnName, ordinalPosition);
        }
      }
    }

    return result;
  }

  private static boolean isPrimaryKey(String constraintName) {
    return constraintName.startsWith("PK");
  }

  /** @return the Spanner table names that are tracked by the change stream. */
  private Set<String> getSpannerTableNamesTrackedByChangeStreams() {
    boolean isChangeStreamForAll = isChangeStreamForAll();

    Statement.Builder statementBuilder;
    String sql;
    if (this.isPostgres()) {
      sql =
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      sql =
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }

    if (isChangeStreamForAll) {
      // If the change stream is tracking all tables, we have to look up the table names in
      // INFORMATION_SCHEMA.TABLES.
      if (this.isPostgres()) {
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'";
      } else {
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";
      }
      statementBuilder = Statement.newBuilder(sql);
    }

    Set<String> result = new HashSet<>();
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statementBuilder.build())) {

      while (resultSet.next()) {
        result.add(resultSet.getString(informationSchemaTableName()));
      }
    }

    return result;
  }

  /** @return if the change stream tracks all the tables in the database. */
  private boolean isChangeStreamForAll() {
    Boolean result = null;

    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      String sql =
          "SELECT CHANGE_STREAMS.all FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      String sql =
          "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statementBuilder.build())) {
      while (resultSet.next()) {
        if (this.isPostgres()) {
          String resultString = resultSet.getString(informationSchemaAll());
          if (resultString != null) {
            result = resultSet.getString(informationSchemaAll()).equalsIgnoreCase("YES");
          }
        } else {
          result = resultSet.getBoolean(informationSchemaAll());
        }
      }
    }

    if (result == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find change stream %s in INFORMATION_SCHEMA", changeStreamName));
    }

    return result;
  }

  /**
   * @return the Spanner column names that are tracked explicitly by change stream by table name.
   *     e.g. Given Singers table with SingerId, FirstName and LastName, an empty map will be
   *     returned if we have change stream "CREATE CHANGE STREAM AllStream FOR ALL" or "CREATE
   *     CHANGE STREAM AllStream FOR Singers", {"Singers" -> {"SingerId", "FirstName"}} will be
   *     returned if we have change stream "CREATE CHANGE STREAM SingerStream FOR Singers(SingerId,
   *     FirstName)"
   */
  private Map<String, Set<String>>
      getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName() {
    Map<String, Set<String>> result = new HashMap<>();
    Statement.Builder statementBuilder;
    if (this.isPostgres()) {
      String sql =
          "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
              + "WHERE CHANGE_STREAM_NAME = $1";
      statementBuilder = Statement.newBuilder(sql).bind("p1").to(changeStreamName);
    } else {
      String sql =
          "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
              + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
      statementBuilder = Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);
    }

    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statementBuilder.build())) {

      while (resultSet.next()) {
        String tableName = resultSet.getString(informationSchemaTableName());
        String columnName = resultSet.getString(informationSchemaColumnName());
        result.putIfAbsent(tableName, new HashSet<>());
        result.get(tableName).add(columnName);
      }
    }

    return result;
  }

  private Type informationSchemaTypeToSpannerType(String type) {
    if (this.isPostgres()) {
      return informationSchemaPostgreSQLTypeToSpannerType(type);
    }
    return informationSchemaGoogleSQLTypeToSpannerType(type);
  }

  private Type informationSchemaGoogleSQLTypeToSpannerType(String type) {
    type = cleanInformationSchemaType(type);
    switch (type) {
      case "BOOL":
        return Type.bool();
      case "BYTES":
        return Type.bytes();
      case "DATE":
        return Type.date();
      case "FLOAT64":
        return Type.float64();
      case "INT64":
        return Type.int64();
      case "JSON":
        return Type.json();
      case "NUMERIC":
        return Type.numeric();
      case "STRING":
        return Type.string();
      case "TIMESTAMP":
        return Type.timestamp();
      default:
        if (type.startsWith("ARRAY")) {
          // Get array type, e.g. "ARRAY<STRING>" -> "STRING".
          String spannerArrayType = type.substring(6, type.length() - 1);
          Type itemType = informationSchemaGoogleSQLTypeToSpannerType(spannerArrayType);
          return Type.array(itemType);
        }

        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", type));
    }
  }

  private Type informationSchemaPostgreSQLTypeToSpannerType(String type) {
    boolean isPostgresArray = isPostgresArray(type);
    String cleanedType = "";
    if (isPostgresArray) {
      cleanedType = type.substring(0, type.length() - 2);
      Type itemType = informationSchemaPostgreSQLTypeToSpannerType(cleanedType);
      return Type.array(itemType);
    } else {
      cleanedType = cleanInformationSchemaType(type);
    }

    switch (cleanedType) {
      case "BOOLEAN":
        return Type.bool();
      case "BYTEA":
        return Type.bytes();
      case "DOUBLE PRECISION":
        return Type.float64();
      case "BIGINT":
        return Type.int64();
      case "DATE":
        return Type.date();
      case "JSONB":
        return Type.pgJsonb();
      case "NUMERIC":
        return Type.pgNumeric();
      case "CHARACTER VARYING":
        return Type.string();
      case "TIMESTAMP WITH TIME ZONE":
        return Type.timestamp();
      case "SPANNER.COMMIT_TIMESTAMP":
        return Type.timestamp();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported Spanner PostgreSQL type: %s", type));
    }
  }

  private boolean isPostgresArray(String type) {
    return type.endsWith("[]");
  }

  /**
   * Remove the Spanner type length limit, since Spanner doesn't document clearly on the
   * parameterized types like BigQuery does, i.e. BigQuery's docmentation on <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_data_types">Parameterized
   * data types</a>, but Spanner doesn't have a similar one. We might have problem if we transfer
   * the length limit into BigQuery. By removing the length limit, we essentially loose the
   * constraint of data written to BigQuery, and it won't cause errors.
   */
  private String cleanInformationSchemaType(String type) {
    // Remove type size, e.g. STRING(1024) -> STRING.
    int leftParenthesisIdx = type.indexOf('(');
    if (leftParenthesisIdx != -1) {
      type = type.substring(0, leftParenthesisIdx) + type.substring(type.indexOf(')') + 1);
    }

    // Convert it to upper case.
    return type.toUpperCase();
  }

  public static void appendToSpannerKey(
      TrackedSpannerColumn column, JSONObject keysJsonObject, Key.Builder keyBuilder) {
    Type.Code code = column.getType().getCode();
    String name = column.getName();
    switch (code) {
      case BOOL:
        keyBuilder.append(keysJsonObject.getBoolean(name));
        break;
      case FLOAT64:
        keyBuilder.append(keysJsonObject.getDouble(name));
        break;
      case INT64:
        keyBuilder.append(keysJsonObject.getLong(name));
        break;
      case NUMERIC:
        keyBuilder.append(keysJsonObject.getBigDecimal(name));
        break;
      case BYTES:
      case DATE:
      case STRING:
      case TIMESTAMP:
        keyBuilder.append(keysJsonObject.getString(name));
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", code));
    }
  }

  private boolean isPostgres() {
    return this.dialect == Dialect.POSTGRESQL;
  }

  private String informationSchemaTableName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_TABLE_NAME;
    }
    return INFORMATION_SCHEMA_TABLE_NAME;
  }

  private String informationSchemaColumnName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_COLUMN_NAME;
    }
    return INFORMATION_SCHEMA_COLUMN_NAME;
  }

  private String informationSchemaSpannerType() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_SPANNER_TYPE;
    }
    return INFORMATION_SCHEMA_SPANNER_TYPE;
  }

  private String informationSchemaOrdinalPosition() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_ORDINAL_POSITION;
    }
    return INFORMATION_SCHEMA_ORDINAL_POSITION;
  }

  private String informationSchemaConstraintName() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_CONSTRAINT_NAME;
    }
    return INFORMATION_SCHEMA_CONSTRAINT_NAME;
  }

  private String informationSchemaAll() {
    if (this.isPostgres()) {
      return INFORMATION_SCHEMA_POSTGRES_ALL;
    }
    return INFORMATION_SCHEMA_ALL;
  }
}
