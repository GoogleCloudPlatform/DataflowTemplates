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
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.common.collect.ImmutableMap;
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

    ISchemaMapper schemaMapper = dmlGeneratorRequest.getSchemaMapper();
    String spannerTableName = dmlGeneratorRequest.getSpannerTableName();
    String sourceTableName = "";
    try {
      sourceTableName = schemaMapper.getSourceTableName("", spannerTableName);
    } catch (Exception e) {
      LOG.warn("The table {} was not found in source", spannerTableName);
      return new DMLGeneratorResponse("");
    }

    SourceSchema sourceSchema = dmlGeneratorRequest.getSourceSchema();
    SourceTable sourceTable = sourceSchema.tables().get(sourceTableName);
    if (sourceTable.primaryKeyColumns() == null || sourceTable.primaryKeyColumns().size() == 0) {
      LOG.warn(
          "Cannot reverse replicate table {} without primary key. Skipping the record.",
          sourceTableName);
      return new DMLGeneratorResponse("");
    }

    Ddl spannerDdl = dmlGeneratorRequest.getDdl();
    Table spannerTable = spannerDdl.table(spannerTableName);

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
      LOG.warn(
          "Failed to generate primary key values for table {}. Skipping the record.",
          sourceTableName);
      return new DMLGeneratorResponse("");
    }

    java.sql.Timestamp timestamp = dmlGeneratorRequest.getCommitTimestamp().toSqlTimestamp();
    String modType = dmlGeneratorRequest.getModType();

    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      return generateUpsertStatement(
          schemaMapper,
          spannerTable,
          sourceTable,
          dmlGeneratorRequest,
          pkColumnNameValues,
          timestamp);
    } else if ("DELETE".equals(modType)) {
      return getDeleteStatementCQL(sourceTableName, timestamp, pkColumnNameValues);
    } else {
      LOG.error("Unsupported modType: {} for table {}", modType, spannerTableName);
      return new DMLGeneratorResponse("");
    }
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues,
      java.sql.Timestamp timestamp) {
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
    return getUpsertStatementCQL(sourceTable.name(), timestamp, allColumnNamesAndValues);
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

  private static Map<String, PreparedStatementValueObject<?>> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    Set<String> sourcePKs = new HashSet<>(sourceTable.primaryKeyColumns());
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    for (SourceColumn sourceColumn : sourceTable.columns()) {
      String colName = sourceColumn.name();
      if (sourcePKs.contains(colName)) {
        continue; // we only need non-primary keys
      }

      if (customTransformColumns != null && customTransformColumns.contains(colName)) {
        String cassandraType = sourceColumn.type().toLowerCase();
        Object customValue = customTransformationResponse.get(colName);
        response.put(
            colName,
            PreparedStatementValueObject.create(
                cassandraType,
                customValue == null ? CassandraTypeHandler.NullClass.INSTANCE : customValue));
        continue;
      }

      String spannerColumnName = "";
      try {
        spannerColumnName = schemaMapper.getSpannerColumnName("", sourceTable.name(), colName);
      } catch (Exception e) {
        continue;
      }

      Column spannerColumn = spannerTable.column(spannerColumnName);
      if (spannerColumn == null) {
        continue;
      }

      PreparedStatementValueObject<?> columnValue;
      if (keyValuesJson.has(spannerColumnName)) {
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, newValuesJson, sourceDbTimezoneOffset);
      } else {
        continue;
      }

      response.put(colName, columnValue);
    }

    return response;
  }

  private static Map<String, PreparedStatementValueObject<?>> getPkColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    Set<String> sourcePKs = new HashSet<>(sourceTable.primaryKeyColumns());
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    for (String currentSourcePK : sourcePKs) {
      String currentSpannerPk = "";
      try {
        currentSpannerPk =
            schemaMapper.getSpannerColumnName("", sourceTable.name(), currentSourcePK);
      } catch (Exception e) {
        LOG.warn(
            "The corresponding primary key column {} was not found in Spanner", currentSourcePK);
        return null;
      }

      if (customTransformColumns != null && customTransformColumns.contains(currentSourcePK)) {
        String cassandraType =
            sourceTable.columns().stream()
                .filter(col -> col.name().equals(currentSourcePK))
                .findFirst()
                .get()
                .type()
                .toLowerCase();
        Object customValue = customTransformationResponse.get(currentSourcePK);
        response.put(
            currentSourcePK,
            PreparedStatementValueObject.create(
                cassandraType,
                customValue == null ? CassandraTypeHandler.NullClass.INSTANCE : customValue));
        continue;
      }

      Column spannerColumn = spannerTable.column(currentSpannerPk);
      SourceColumn sourceColumn =
          sourceTable.columns().stream()
              .filter(col -> col.name().equals(currentSourcePK))
              .findFirst()
              .orElse(null);

      PreparedStatementValueObject<?> columnValue;
      if (keyValuesJson.has(currentSpannerPk)) {
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(currentSpannerPk)) {
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn("The column {} was not found in input record", currentSpannerPk);
        return null;
      }

      response.put(currentSourcePK, columnValue);
    }

    return response;
  }

  private static PreparedStatementValueObject<?> getMappedColumnValue(
      Column spannerColumn,
      SourceColumn sourceColumn,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {
    return CassandraTypeHandler.getColumnValueByType(
        spannerColumn, sourceColumn, valuesJson, sourceDbTimezoneOffset);
  }
}
