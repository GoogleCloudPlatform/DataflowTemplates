/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generator for creating Data Manipulation Language (DML) statements for Cassandra. Implements
 * the {@link IDMLGenerator} interface to handle various types of DML operations, such as insert,
 * update, delete, and upsert.
 *
 * <p>This class is designed to construct Cassandra-specific DML statements by mapping input data
 * and schema information to query formats that align with Cassandra's syntax and structure. It also
 * validates primary keys, handles data type conversions, and manages timestamps in queries.
 *
 * <p>Key Responsibilities:
 *
 * <ul>
 *   <li>Generating upsert statements for inserting or updating records.
 *   <li>Creating delete statements for rows identified by primary key values.
 *   <li>Mapping input data to Cassandra-compatible column values.
 *   <li>Handling specific data types and ensuring query compatibility with Cassandra.
 * </ul>
 *
 * <p>Usage Example:
 *
 * <pre>{@code
 * IDMLGenerator generator = new CassandraDMLGenerator();
 * DMLGeneratorResponse response = generator.getDMLStatement(dmlGeneratorRequest);
 * }</pre>
 *
 * @see IDMLGenerator
 */
public class CassandraDMLGenerator implements IDMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDMLGenerator.class);

  /**
   * @param dmlGeneratorRequest the request containing necessary information to construct the DML
   *     statement, including modification type, table schema, new values, and key values.
   * @return DMLGeneratorResponse
   */
  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
    if (dmlGeneratorRequest == null) {
      LOG.warn("DMLGeneratorRequest is null. Cannot process the request.");
      return new DMLGeneratorResponse("");
    }

    String spannerTableName = dmlGeneratorRequest.getSpannerTableName();
    Schema schema = dmlGeneratorRequest.getSchema();

    if (schema == null
        || schema.getSpannerToID() == null
        || schema.getSpSchema() == null
        || schema.getSrcSchema() == null) {
      LOG.warn("Schema is invalid or incomplete for table: {}", spannerTableName);
      return new DMLGeneratorResponse("");
    }

    NameAndCols tableMapping = schema.getSpannerToID().get(spannerTableName);
    if (tableMapping == null) {
      LOG.warn(
          "Spanner table {} not found in session file. Dropping the record.", spannerTableName);
      return new DMLGeneratorResponse("");
    }

    String spannerTableId = tableMapping.getName();
    SpannerTable spannerTable = schema.getSpSchema().get(spannerTableId);
    if (spannerTable == null) {
      LOG.warn(
          "Spanner table {} not found in session file. Dropping the record.", spannerTableName);
      return new DMLGeneratorResponse("");
    }

    SourceTable sourceTable = schema.getSrcSchema().get(spannerTableId);
    if (sourceTable == null) {
      LOG.warn(
          "Source table {} not found for Spanner table ID: {}", spannerTableName, spannerTableId);
      return new DMLGeneratorResponse("");
    }

    if (sourceTable.getPrimaryKeys() == null || sourceTable.getPrimaryKeys().length == 0) {
      LOG.warn(
          "Cannot reverse replicate table {} without primary key. Skipping the record.",
          sourceTable.getName());
      return new DMLGeneratorResponse("");
    }

    Map<String, PreparedStatementValueObject<?>> pkColumnNameValues =
        getPkColumnValues(
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset());
    if (pkColumnNameValues == null) {
      LOG.warn(
          "Failed to generate primary key values for table {}. Skipping the record.",
          sourceTable.getName());
      return new DMLGeneratorResponse("");
    }

    String modType = dmlGeneratorRequest.getModType();
    switch (modType) {
      case "INSERT":
      case "UPDATE":
        return generateUpsertStatement(
            spannerTable, sourceTable, dmlGeneratorRequest, pkColumnNameValues);
      case "DELETE":
        long timestamp = Instant.now().toEpochMilli() * 1000;
        return getDeleteStatementCQL(sourceTable.getName(), pkColumnNameValues, timestamp);
      default:
        LOG.error("Unsupported modType: {} for table {}", modType, spannerTableName);
        return new DMLGeneratorResponse("");
    }
  }

  /**
   * Generates an upsert (insert or update) DML statement for a given Spanner table based on the
   * provided source table, request parameters, and primary key column values.
   *
   * @param spannerTable the Spanner table metadata containing column definitions and constraints.
   * @param sourceTable the source table metadata containing the table name and structure.
   * @param dmlGeneratorRequest the request containing new values, key values, and timezone offset
   *     for generating the DML.
   * @param pkColumnNameValues a map of primary key column names and their corresponding prepared
   *     statement value objects.
   * @return a {@link DMLGeneratorResponse} containing the generated upsert statement and associated
   *     data.
   *     <p>This method: 1. Extracts column values from the provided request using the
   *     `getColumnValues` method. 2. Combines the column values with the primary key column values.
   *     3. Constructs the upsert statement using the `getUpsertStatementCQL` method.
   *     <p>The upsert statement ensures that the record is inserted or updated in the Spanner table
   *     based on the primary key.
   */
  private static DMLGeneratorResponse generateUpsertStatement(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues) {
    Map<String, PreparedStatementValueObject<?>> columnNameValues =
        getColumnValues(
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset());
    return getUpsertStatementCQL(
        sourceTable.getName(),
        Instant.now().toEpochMilli() * 1000,
        columnNameValues,
        pkColumnNameValues);
  }

  /**
   * Constructs an upsert (insert or update) CQL statement for a Cassandra or similar database using
   * the provided table name, timestamp, column values, and primary key values.
   *
   * @param tableName the name of the table to which the upsert statement applies.
   * @param timestamp the timestamp (in microseconds) to use for the operation.
   * @param columnNameValues a map of column names and their corresponding prepared statement value
   *     objects for non-primary key columns.
   * @param pkColumnNameValues a map of primary key column names and their corresponding prepared
   *     statement value objects.
   * @return a {@link DMLGeneratorResponse} containing the generated CQL statement and a list of
   *     values to be used with the prepared statement.
   *     <p>This method: 1. Iterates through the primary key and column values, appending column
   *     names and placeholders to the generated CQL statement. 2. Constructs the `INSERT INTO` CQL
   *     statement with the provided table name, columns, and placeholders. 3. Appends a `USING
   *     TIMESTAMP` clause to include the provided timestamp in the statement. 4. Creates a list of
   *     values to bind to the placeholders in the prepared statement.
   *     <p>The returned response contains the complete prepared CQL statement and the values
   *     required to execute it.
   */
  private static DMLGeneratorResponse getUpsertStatementCQL(
      String tableName,
      long timestamp,
      Map<String, PreparedStatementValueObject<?>> columnNameValues,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues) {

    StringBuilder allColumns = new StringBuilder();
    StringBuilder placeholders = new StringBuilder();
    List<PreparedStatementValueObject<?>> values = new ArrayList<>();

    for (Map.Entry<String, PreparedStatementValueObject<?>> entry : pkColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      PreparedStatementValueObject<?> colValue = entry.getValue();
      if (colValue.value() != null) {
        allColumns.append(colName).append(", ");
        placeholders.append("?, ");
        values.add(colValue);
      }
    }

    for (Map.Entry<String, PreparedStatementValueObject<?>> entry : columnNameValues.entrySet()) {
      String colName = entry.getKey();
      PreparedStatementValueObject<?> colValue = entry.getValue();
      if (colValue.value() != CassandraTypeHandler.NullClass.INSTANCE) {
        allColumns.append(colName).append(", ");
        placeholders.append("?, ");
        values.add(colValue);
      }
    }

    if (allColumns.length() > 0) {
      allColumns.setLength(allColumns.length() - 2);
    }
    if (placeholders.length() > 0) {
      placeholders.setLength(placeholders.length() - 2);
    }

    String preparedStatement =
        "INSERT INTO "
            + tableName
            + " ("
            + allColumns
            + ") VALUES ("
            + placeholders
            + ") USING TIMESTAMP ?;";

    PreparedStatementValueObject<Long> timestampObj =
        PreparedStatementValueObject.create("USING_TIMESTAMP", timestamp);
    values.add(timestampObj);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  /**
   * Constructs a delete statement in CQL (Cassandra Query Language) using the provided table name,
   * primary key values, and timestamp.
   *
   * @param tableName the name of the table from which records will be deleted.
   * @param pkColumnNameValues a map containing the primary key column names and their corresponding
   *     prepared statement value objects.
   * @param timestamp the timestamp (in microseconds) to use for the delete operation.
   * @return a {@link DMLGeneratorResponse} containing the generated CQL delete statement and a list
   *     of values to bind to the prepared statement.
   *     <p>This method: 1. Iterates through the provided primary key column values, appending
   *     conditions to the WHERE clause of the CQL delete statement. 2. Constructs the `DELETE FROM`
   *     CQL statement with the specified table name, primary key conditions, and a `USING
   *     TIMESTAMP` clause. 3. Creates a list of values to be used with the prepared statement,
   *     including the timestamp.
   *     <p>If no primary key column values are provided, an empty WHERE clause is generated. An
   *     exception may be thrown if any value type does not match the expected type.
   */
  private static DMLGeneratorResponse getDeleteStatementCQL(
      String tableName,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues,
      long timestamp) {

    StringBuilder deleteConditions = new StringBuilder();
    List<PreparedStatementValueObject<?>> values = new ArrayList<>();

    for (Map.Entry<String, PreparedStatementValueObject<?>> entry : pkColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      PreparedStatementValueObject<?> colValue = entry.getValue();
      if (colValue.value() != CassandraTypeHandler.NullClass.INSTANCE) {
        deleteConditions.append(colName).append(" = ? AND ");
        values.add(entry.getValue());
      }
    }

    if (deleteConditions.length() > 0) {
      deleteConditions.setLength(deleteConditions.length() - 5);
    }

    String preparedStatement =
        "DELETE FROM " + tableName + " WHERE " + deleteConditions + " USING TIMESTAMP ?;";

    PreparedStatementValueObject<Long> timestampObj =
        PreparedStatementValueObject.create("USING_TIMESTAMP", timestamp);
    values.add(timestampObj);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  /**
   * Extracts the column values from the source table based on the provided Spanner schema, new
   * values, and key values JSON objects.
   *
   * @param spannerTable the Spanner table schema.
   * @param sourceTable the source table schema.
   * @param newValuesJson the JSON object containing new values for columns.
   * @param keyValuesJson the JSON object containing key values for columns.
   * @param sourceDbTimezoneOffset the timezone offset of the source database.
   * @return a map of column names to their corresponding prepared statement value objects.
   *     <p>This method: 1. Iterates over the non-primary key column definitions in the source table
   *     schema. 2. Maps each column in the source table schema to its corresponding column in the
   *     Spanner schema. 3. Checks if the column values exist in the `keyValuesJson` or
   *     `newValuesJson` and retrieves the appropriate value. 4. Skips columns that do not exist in
   *     any of the JSON objects or are marked as null.
   */
  private static Map<String, PreparedStatementValueObject<?>> getColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    Set<String> sourcePKs = sourceTable.getPrimaryKeySet();
    for (Map.Entry<String, SourceColumnDefinition> entry : sourceTable.getColDefs().entrySet()) {
      SourceColumnDefinition sourceColDef = entry.getValue();

      String colName = sourceColDef.getName();
      if (sourcePKs.contains(colName)) {
        continue; // we only need non-primary keys
      }

      String colId = entry.getKey();
      SpannerColumnDefinition spannerColDef = spannerTable.getColDefs().get(colId);
      if (spannerColDef == null) {
        continue;
      }
      String spannerColumnName = spannerColDef.getName();
      PreparedStatementValueObject<?> columnValue;
      if (keyValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColumnName)) {
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        continue;
      }

      response.put(sourceColDef.getName(), columnValue);
    }

    return response;
  }

  /**
   * Extracts the primary key column values from the source table based on the provided Spanner
   * schema, new values, and key values JSON objects.
   *
   * @param spannerTable the Spanner table schema.
   * @param sourceTable the source table schema.
   * @param newValuesJson the JSON object containing new values for columns.
   * @param keyValuesJson the JSON object containing key values for columns.
   * @param sourceDbTimezoneOffset the timezone offset of the source database.
   * @return a map of primary key column names to their corresponding prepared statement value
   *     objects, or null if a required column is missing.
   *     <p>This method: 1. Iterates over the primary key definitions in the source table schema. 2.
   *     Maps each primary key column in the source table schema to its corresponding column in the
   *     Spanner schema. 3. Checks if the primary key column values exist in the `keyValuesJson` or
   *     `newValuesJson` and retrieves the appropriate value. 4. Returns null if any required
   *     primary key column is missing in the JSON objects.
   */
  private static Map<String, PreparedStatementValueObject<?>> getPkColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    ColumnPK[] sourcePKs = sourceTable.getPrimaryKeys();

    for (ColumnPK currentSourcePK : sourcePKs) {
      String colId = currentSourcePK.getColId();
      SourceColumnDefinition sourceColDef = sourceTable.getColDefs().get(colId);
      SpannerColumnDefinition spannerColDef = spannerTable.getColDefs().get(colId);
      if (spannerColDef == null) {
        LOG.warn(
            "The corresponding primary key column {} was not found in Spanner",
            sourceColDef.getName());
        return null;
      }
      String spannerColumnName = spannerColDef.getName();
      PreparedStatementValueObject<?> columnValue;
      if (keyValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColumnName)) {
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn("The column {} was not found in input record", spannerColumnName);
        return null;
      }

      response.put(sourceColDef.getName(), columnValue);
    }

    return response;
  }

  /**
   * Maps a column value from the source table to its corresponding Spanner column value based on
   * their respective definitions.
   *
   * @param spannerColDef the Spanner column definition.
   * @param sourceColDef the source column definition.
   * @param valuesJson the JSON object containing column values.
   * @param sourceDbTimezoneOffset the timezone offset of the source database.
   * @return a {@link PreparedStatementValueObject} containing the mapped value for the column.
   *     <p>This method: 1. Retrieves the value of the column from the JSON object. 2. Converts the
   *     value to the appropriate type based on the Spanner and source column definitions. 3. Uses a
   *     type handler to map the value if necessary.
   */
  private static PreparedStatementValueObject<?> getMappedColumnValue(
      SpannerColumnDefinition spannerColDef,
      SourceColumnDefinition sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {
    return CassandraTypeHandler.getColumnValueByType(
        spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);
  }
}
