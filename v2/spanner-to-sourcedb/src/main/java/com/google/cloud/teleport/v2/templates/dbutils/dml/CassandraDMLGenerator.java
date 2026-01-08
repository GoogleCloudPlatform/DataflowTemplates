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

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
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
      throw new InvalidDMLGenerationException(
          "DMLGeneratorRequest is null. Cannot process the request.");
    }
    ISchemaMapper schemaMapper = dmlGeneratorRequest.getSchemaMapper();
    String spannerTableName = dmlGeneratorRequest.getSpannerTableName();
    Ddl spannerDdl = dmlGeneratorRequest.getSpannerDdl();
    SourceSchema sourceSchema = dmlGeneratorRequest.getSourceSchema();
    if (schemaMapper == null || spannerDdl == null || sourceSchema == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Schema Mapper, Ddl and SourceSchema must be not null, respectively found %s,%s,%s.",
              schemaMapper, spannerDdl, sourceSchema));
    }
    String sourceTableName = "";
    try {
      sourceTableName = schemaMapper.getSourceTableName("", spannerTableName);
    } catch (Exception e) {
      throw new InvalidDMLGenerationException(
          "Could not find source table name for spanner table: " + spannerTableName, e);
    }

    Table spannerTable = spannerDdl.table(spannerTableName);
    if (spannerTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "The spanner table %s was not found in ddl found on spanner. Ddl: %s",
              spannerTableName, spannerDdl));
    }
    SourceTable sourceTable = sourceSchema.table(sourceTableName);
    if (sourceTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "The source table %s was not found in source schema. Source Schema: %s",
              sourceTableName, sourceSchema));
    }

    if (sourceTable.primaryKeyColumns() == null || sourceTable.primaryKeyColumns().size() == 0) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for source table %s without primary key, skipping the record",
              sourceTableName));
    }

    Map<String, PreparedStatementValueObject<?>> pkColumnNameValues =
        getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
    if (pkColumnNameValues == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for table %s without primary key, skipping the record",
              sourceTableName));
    }
    java.sql.Timestamp timestamp = dmlGeneratorRequest.getCommitTimestamp().toSqlTimestamp();
    String modType = dmlGeneratorRequest.getModType();
    return generateDMLResponse(
        spannerTable,
        sourceTable,
        schemaMapper,
        dmlGeneratorRequest,
        pkColumnNameValues,
        timestamp,
        modType);
  }

  /**
   * Generates a DML response based on the given modification type (INSERT, UPDATE, or DELETE).
   *
   * <p>This method processes the data from SpannerTable, SourceTable, and DMLGeneratorRequest to
   * construct a corresponding CQL statement (INSERT, UPDATE, or DELETE) for Cassandra. The
   * statement is generated based on the modification type and includes the appropriate primary key
   * and column values, along with an optional timestamp.
   *
   * @param spannerTable the SpannerTable object containing schema information of the Spanner table
   * @param sourceTable the SourceTable object containing details of the source table (e.g., name)
   * @param dmlGeneratorRequest the request object containing new and key value data in JSON format
   * @param pkColumnNameValues a map of primary key column names and their corresponding value
   *     objects
   * @param timestamp the optional timestamp to be included in the Cassandra statement (can be null)
   * @param modType the type of modification to perform, either "INSERT", "UPDATE", or "DELETE"
   * @return DMLGeneratorResponse the response containing the generated CQL statement and bound
   *     values
   * @throws IllegalArgumentException if the modType is unsupported or if any required data is
   *     invalid
   * @implNote The method uses the following logic: - Combines primary key values and column values
   *     into a single list of entries. - Depending on the modType: - For "INSERT" or "UPDATE",
   *     calls {@link #getUpsertStatementCQL}. - For "DELETE", calls {@link #getDeleteStatementCQL}.
   *     - For unsupported modType values, logs an error and returns an empty response.
   */
  private static DMLGeneratorResponse generateDMLResponse(
      Table spannerTable,
      SourceTable sourceTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues,
      java.sql.Timestamp timestamp,
      String modType) {
    Map<String, PreparedStatementValueObject<?>> columnNameValues =
        getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
    Map<String, PreparedStatementValueObject<?>> allColumnNamesAndValues =
        ImmutableMap.<String, PreparedStatementValueObject<?>>builder()
            .putAll(pkColumnNameValues)
            .putAll(columnNameValues)
            .build();
    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      return getUpsertStatementCQL(sourceTable.name(), timestamp, allColumnNamesAndValues);
    } else if ("DELETE".equals(modType)) {
      return getDeleteStatementCQL(sourceTable.name(), timestamp, pkColumnNameValues);
    } else {
      throw new InvalidDMLGenerationException(
          "Unsupported modType: " + dmlGeneratorRequest.getModType());
    }
  }

  /**
   * Constructs an upsert (insert or update) CQL statement for a Cassandra or similar database using
   * the provided table name, timestamp, column values, and primary key values.
   *
   * @param tableName the name of the table to which the upsert statement applies.
   * @param timestamp the timestamp (in java.sql.Timestamp) to use for the operation.
   * @param allColumnNamesAndValues a map of column names and their corresponding prepared statement
   *     value objects for non-primary key columns.
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
      java.sql.Timestamp timestamp,
      Map<String, PreparedStatementValueObject<?>> allColumnNamesAndValues) {

    String escapedTableName = "\"" + tableName.replace("\"", "\"\"") + "\"";

    String allColumns =
        allColumnNamesAndValues.keySet().stream()
            .map(columnName -> "\"" + columnName.replace("\"", "\"\"") + "\"")
            .collect(Collectors.joining(", "));

    String placeholders =
        allColumnNamesAndValues.keySet().stream()
            .map(columnName -> "?")
            .collect(Collectors.joining(", "));

    List<PreparedStatementValueObject<?>> values =
        new ArrayList<>(allColumnNamesAndValues.values());

    PreparedStatementValueObject<Long> timestampObj =
        PreparedStatementValueObject.create("USING_TIMESTAMP", timestamp.getTime());
    values.add(timestampObj);

    String preparedStatement =
        String.format(
            "INSERT INTO %s (%s) VALUES (%s) USING TIMESTAMP ?",
            escapedTableName, allColumns, placeholders);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  /**
   * Constructs a delete statement in CQL (Cassandra Query Language) using the provided table name,
   * primary key values, and timestamp.
   *
   * @param tableName the name of the table from which records will be deleted.
   * @param timestamp the timestamp (in java.sql.Timestamp) to use for the delete operation.
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
      java.sql.Timestamp timestamp,
      Map<String, PreparedStatementValueObject<?>> allColumnNamesAndValues) {

    String escapedTableName = "\"" + tableName.replace("\"", "\"\"") + "\"";

    String deleteConditions =
        allColumnNamesAndValues.keySet().stream()
            .map(columnName -> "\"" + columnName.replace("\"", "\"\"") + "\" = ?")
            .collect(Collectors.joining(" AND "));

    List<PreparedStatementValueObject<?>> values =
        new ArrayList<>(allColumnNamesAndValues.values());

    if (timestamp != null) {
      PreparedStatementValueObject<Long> timestampObj =
          PreparedStatementValueObject.create("USING_TIMESTAMP", timestamp.getTime());
      values.add(0, timestampObj);
    }

    String preparedStatement =
        String.format(
            "DELETE FROM %s USING TIMESTAMP ? WHERE %s", escapedTableName, deleteConditions);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  /**
   * Extracts the column values from the source table based on the provided Spanner schema, new
   * values, and key values JSON objects.
   *
   * @param schemaMapper the schema mapper interface.
   * @param spannerTable the Spanner table schema.
   * @param sourceTable the source table schema.
   * @param newValuesJson the JSON object containing new values for columns.
   * @param keyValuesJson the JSON object containing key values for columns.
   * @param sourceDbTimezoneOffset the timezone offset of the source database.
   * @param customTransformationResponse the custom transformation
   * @return a map of column names to their corresponding prepared statement value objects.
   *     <p>This method: 1. Iterates over the non-primary key column definitions in the source table
   *     schema. 2. Maps each column in the source table schema to its corresponding column in the
   *     Spanner schema. 3. Checks if the column values exist in the `keyValuesJson` or
   *     `newValuesJson` and retrieves the appropriate value. 4. Skips columns that do not exist in
   *     any of the JSON objects or are marked as null.
   */
  private static Map<String, PreparedStatementValueObject<?>> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }
    for (SourceColumn sourceColDef : sourceTable.columns()) {
      String colName = sourceColDef.name();
      if (sourcePKs.contains(colName)) {
        continue; // we only need non-primary keys
      }
      PreparedStatementValueObject<?> columnValue;
      if (customTransformColumns != null && customTransformColumns.contains(colName)) {
        String cassandraType = sourceColDef.type().toLowerCase();
        Object customValue = customTransformationResponse.get(colName);
        columnValue =
            PreparedStatementValueObject.create(
                cassandraType,
                customValue == null ? CassandraTypeHandler.NullClass.INSTANCE : customValue);
        response.put(colName, columnValue);
        continue;
      }
      String spannerColumnName = "";
      try {
        spannerColumnName = schemaMapper.getSpannerColumnName("", sourceTable.name(), colName);
      } catch (NoSuchElementException e) {
        continue;
      }
      Column spannerColDef = spannerTable.column(spannerColumnName);
      if (spannerColDef == null) {
        continue;
      }
      if (keyValuesJson.has(spannerColumnName)) {
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        continue;
      }
      response.put(colName, columnValue);
    }
    return response;
  }

  /**
   * Extracts the primary key column values from the source table based on the provided Spanner
   * schema, new values, and key values JSON objects.
   *
   * @param schemaMapper the schema mapper interface.
   * @param spannerTable the Spanner table schema.
   * @param sourceTable the source table schema.
   * @param newValuesJson the JSON object containing new values for columns.
   * @param keyValuesJson the JSON object containing key values for columns.
   * @param sourceDbTimezoneOffset the timezone offset of the source database.
   * @param customTransformationResponse the user defined transformation.
   * @return a map of primary key column names to their corresponding prepared statement value
   *     objects, or null if a required column is missing.
   *     <p>This method: 1. Iterates over the primary key definitions in the source table schema. 2.
   *     Maps each primary key column in the source table schema to its corresponding column in the
   *     Spanner schema. 3. Checks if the primary key column values exist in the `keyValuesJson` or
   *     `newValuesJson` and retrieves the appropriate value. 4. Returns null if any required
   *     primary key column is missing in the JSON objects.
   */
  private static Map<String, PreparedStatementValueObject<?>> getPkColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }
    for (String sourceColName : sourcePKs) {
      SourceColumn sourceColDef = sourceTable.column(sourceColName);
      if (sourceColDef == null) {
        LOG.warn(
            "The source column definition for {} was not found in source schema",
            sourceColName); // aastha warning - caught by above function
        return null;
      }

      PreparedStatementValueObject<?> columnValue;
      if (customTransformColumns != null && customTransformColumns.contains(sourceColName)) {
        String cassandraType = sourceColDef.type().toLowerCase();
        Object customValue = customTransformationResponse.get(sourceColName);
        columnValue =
            PreparedStatementValueObject.create(
                cassandraType,
                customValue == null ? CassandraTypeHandler.NullClass.INSTANCE : customValue);
        response.put(sourceColName, columnValue);
        continue;
      }

      String spannerColName = "";
      try {
        spannerColName = schemaMapper.getSpannerColumnName("", sourceTable.name(), sourceColName);
      } catch (NoSuchElementException e) {
        continue;
      }
      if (spannerColName == null || spannerColName == "") {
        LOG.warn(
            "The corresponding spanner table for {} was not found in schema mapping",
            sourceColName); // aastha warning - problematic  - caught by above function
        return null;
      }
      Column spannerColDef = spannerTable.column(spannerColName);
      if (spannerColDef == null) {
        LOG.warn(
            "The spanner column definition for {} was not found in spanner schema",
            spannerColName); // aastha warning - problematic  - caught by above function
        return null;
      }
      if (keyValuesJson.has(spannerColName)) {
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColName)) {
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn(
            "The column {} was not found in input record",
            spannerColName); // aastha warning - problematic  - caught by above function
        return null;
      }
      response.put(sourceColName, columnValue);
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
      Column spannerColDef,
      SourceColumn sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {
    return CassandraTypeHandler.getColumnValueByType(
        spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);
  }
}
