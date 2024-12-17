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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
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

/** Creates DML statements For Cassandra */
public class CassandraDMLGenerator implements IDMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDMLGenerator.class);

  /**
   * @param dmlGeneratorRequest the request containing necessary information to construct the DML
   *     statement, including modification type, table schema, new values, and key values.
   * @return DMLGeneratorResponse
   */
  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
    if (dmlGeneratorRequest
            .getSchema()
            .getSpannerToID()
            .get(dmlGeneratorRequest.getSpannerTableName())
        == null) {
      LOG.warn(
          "The spanner table {} was not found in session file, dropping the record",
          dmlGeneratorRequest.getSpannerTableName());
      return new DMLGeneratorResponse("");
    }

    String spannerTableId =
        dmlGeneratorRequest
            .getSchema()
            .getSpannerToID()
            .get(dmlGeneratorRequest.getSpannerTableName())
            .getName();
    SpannerTable spannerTable = dmlGeneratorRequest.getSchema().getSpSchema().get(spannerTableId);

    if (spannerTable == null) {
      LOG.warn(
          "The spanner table {} was not found in session file, dropping the record",
          dmlGeneratorRequest.getSpannerTableName());
      return new DMLGeneratorResponse("");
    }

    SourceTable sourceTable = dmlGeneratorRequest.getSchema().getSrcSchema().get(spannerTableId);
    if (sourceTable == null) {
      LOG.warn("The table {} was not found in source", dmlGeneratorRequest.getSpannerTableName());
      return new DMLGeneratorResponse("");
    }

    if (sourceTable.getPrimaryKeys() == null || sourceTable.getPrimaryKeys().length == 0) {
      LOG.warn(
          "Cannot reverse replicate for table {} without primary key, skipping the record",
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
          "Cannot reverse replicate for table {} without primary key, skipping the record",
          sourceTable.getName());
      return new DMLGeneratorResponse("");
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          spannerTable, sourceTable, dmlGeneratorRequest, pkColumnNameValues);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatementCQL(
          sourceTable.getName(), pkColumnNameValues, Instant.now().toEpochMilli() * 1000);
    } else {
      LOG.warn("Unsupported modType: {}", dmlGeneratorRequest.getModType());
      return new DMLGeneratorResponse("");
    }
  }

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
      if (colValue.getValue() != null) {
        allColumns.append(colName).append(", ");
        placeholders.append("?, ");
        values.add(colValue);
      }
    }

    for (Map.Entry<String, PreparedStatementValueObject<?>> entry : columnNameValues.entrySet()) {
      String colName = entry.getKey();
      PreparedStatementValueObject<?> colValue = entry.getValue();
      if (colValue.getValue() != null) {
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
        new PreparedStatementValueObject<>("USING_TIMESTAMP", timestamp);
    values.add(timestampObj);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  private static DMLGeneratorResponse getDeleteStatementCQL(
      String tableName,
      Map<String, PreparedStatementValueObject<?>> pkColumnNameValues,
      long timestamp) {

    StringBuilder deleteConditions = new StringBuilder();
    List<PreparedStatementValueObject<?>> values = new ArrayList<>();

    for (Map.Entry<String, PreparedStatementValueObject<?>> entry : pkColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      PreparedStatementValueObject<?> colValue = entry.getValue();
      if (colValue.getValue() != null) {
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
        new PreparedStatementValueObject<>("USING_TIMESTAMP", timestamp);
    values.add(timestampObj);

    return new PreparedStatementGeneratedResponse(preparedStatement, values);
  }

  private static Map<String, PreparedStatementValueObject<?>> getColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();

    /*
    Get all non-primary key col ids from source table
    For each - get the corresponding column name from spanner Schema
    if the column cannot be found in spanner schema - continue to next,
      as the column will be stored with default/null values
    check if the column name found in Spanner schema exists in keyJson -
      if so, get the string value
    else
    check if the column name found in Spanner schema exists in valuesJson -
      if so, get the string value
    if the column does not exist in any of the JSON - continue to next,
      as the column will be stored with default/null values
    */
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

  private static Map<String, PreparedStatementValueObject<?>> getPkColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
    Map<String, PreparedStatementValueObject<?>> response = new HashMap<>();
    /*
    Get all primary key col ids from source table
    For each - get the corresponding column name from spanner Schema
    if the column cannot be found in spanner schema - return null
    check if the column name found in Spanner schema exists in keyJson -
      if so, get the string value
    else
    check if the column name found in Spanner schema exists in valuesJson -
      if so, get the string value
    if the column does not exist in any of the JSON - return null
    */
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

  private static PreparedStatementValueObject<?> getMappedColumnValue(
      SpannerColumnDefinition spannerColDef,
      SourceColumnDefinition sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {
    return CassandraTypeHandler.getColumnValueByType(
        spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);
  }
}
