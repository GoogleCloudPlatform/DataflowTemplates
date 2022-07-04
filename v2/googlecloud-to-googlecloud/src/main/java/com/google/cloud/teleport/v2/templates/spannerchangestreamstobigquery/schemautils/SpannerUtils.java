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
import org.json.JSONObject;

/**
 * Class {@link SpannerUtils} provides methods that retrieve schema information from Spanner. Note
 * all the models returned in the methods of these class are tracked by the change stream.
 */
public class SpannerUtils {

  private static final String INFORMATION_SCHEMA_TABLE_NAME = "TABLE_NAME";
  private static final String INFORMATION_SCHEMA_COLUMN_NAME = "COLUMN_NAME";
  private static final String INFORMATION_SCHEMA_SPANNER_TYPE = "SPANNER_TYPE";
  private static final String INFORMATION_SCHEMA_ORDINAL_POSITION = "ORDINAL_POSITION";
  private static final String INFORMATION_SCHEMA_CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String INFORMATION_SCHEMA_ALL = "ALL";

  private DatabaseClient databaseClient;
  private String changeStreamName;

  public SpannerUtils(DatabaseClient databaseClient, String changeStreamName) {
    this.databaseClient = databaseClient;
    this.changeStreamName = changeStreamName;
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
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
                + "FROM INFORMATION_SCHEMA.COLUMNS");

    // Skip the columns of the tables that are not tracked by change stream.
    if (!spannerTableNames.isEmpty()) {
      sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
    }

    Statement.Builder statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    if (!spannerTableNames.isEmpty()) {
      statementBuilder.bind("tableNames").to(Value.stringArray(new ArrayList<>(spannerTableNames)));
    }

    try (ResultSet columnsResultSet =
        databaseClient.singleUse().executeQuery(statementBuilder.build())) {
      while (columnsResultSet.next()) {
        String tableName = columnsResultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
        String columnName = columnsResultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
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

        int ordinalPosition = (int) columnsResultSet.getLong(INFORMATION_SCHEMA_ORDINAL_POSITION);
        String spannerType = columnsResultSet.getString(INFORMATION_SCHEMA_SPANNER_TYPE);
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
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

    // Skip the tables that are not tracked by change stream.
    if (!spannerTableNames.isEmpty()) {
      sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
    }

    Statement.Builder statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    if (!spannerTableNames.isEmpty()) {
      statementBuilder.bind("tableNames").to(Value.stringArray(new ArrayList<>(spannerTableNames)));
    }

    try (ResultSet keyColumnsResultSet =
        databaseClient.singleUse().executeQuery(statementBuilder.build())) {
      while (keyColumnsResultSet.next()) {
        String tableName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
        String columnName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
        // Note The ordinal position of primary key in INFORMATION_SCHEMA.KEY_COLUMN_USAGE table
        // is different from the ordinal position from INFORMATION_SCHEMA.COLUMNS table.
        int ordinalPosition =
            (int) keyColumnsResultSet.getLong(INFORMATION_SCHEMA_ORDINAL_POSITION);
        String constraintName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_CONSTRAINT_NAME);
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

    String sql =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    Statement.Builder statementBuilder =
        Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName);

    if (isChangeStreamForAll) {
      // If the change stream is tracking all tables, we have to look up the table names in
      // INFORMATION_SCHEMA.TABLES.
      sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";
      statementBuilder = Statement.newBuilder(sql);
    }

    Set<String> result = new HashSet<>();
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statementBuilder.build())) {

      while (resultSet.next()) {
        result.add(resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME));
      }
    }

    return result;
  }

  /** @return if the change stream tracks all the tables in the database. */
  private boolean isChangeStreamForAll() {
    String sql =
        "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    Boolean result = null;
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build())) {

      while (resultSet.next()) {
        result = resultSet.getBoolean(INFORMATION_SCHEMA_ALL);
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
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    Map<String, Set<String>> result = new HashMap<>();
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build())) {

      while (resultSet.next()) {
        String tableName = resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
        String columnName = resultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
        result.putIfAbsent(tableName, new HashSet<>());
        result.get(tableName).add(columnName);
      }
    }

    return result;
  }

  private Type informationSchemaTypeToSpannerType(String type) {
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
          Type itemType = informationSchemaTypeToSpannerType(spannerArrayType);
          return Type.array(itemType);
        }

        throw new IllegalArgumentException(String.format("Unsupported Spanner type: %s", type));
    }
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
}
