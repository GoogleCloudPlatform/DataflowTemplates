/*
 * Copyright (C) 2026 Google LLC
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
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/** Creates DML statements for PostgreSQL. */
public class PostgreSQLDMLGenerator implements IDMLGenerator {

  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
    if (dmlGeneratorRequest == null) {
      throw new InvalidDMLGenerationException(
          "DMLGeneratorRequest is null. Cannot process the request.");
    }
    String spannerTableName = dmlGeneratorRequest.getSpannerTableName();
    ISchemaMapper schemaMapper = dmlGeneratorRequest.getSchemaMapper();
    Ddl spannerDdl = dmlGeneratorRequest.getSpannerDdl();
    SourceSchema sourceSchema = dmlGeneratorRequest.getSourceSchema();

    if (schemaMapper == null) {
      throw new InvalidDMLGenerationException("Schema Mapper must be not null");
    }
    if (spannerDdl == null) {
      throw new InvalidDMLGenerationException("Spanner Ddl must be not null.");
    }
    if (sourceSchema == null) {
      throw new InvalidDMLGenerationException("SourceSchema must be not null.");
    }

    Table spannerTable = spannerDdl.table(spannerTableName);
    if (spannerTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "The spanner table %s was not found in ddl found on spanner", spannerTableName));
    }

    String sourceTableName = "";
    try {
      sourceTableName = schemaMapper.getSourceTableName("", spannerTableName);
    } catch (NoSuchElementException e) {
      throw new InvalidDMLGenerationException(
          "Could not find source table name for spanner table: " + spannerTableName, e);
    }
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable sourceTable =
        sourceSchema.table(sourceTableName);
    if (sourceTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Equivalent table %s was not found in source for spanner table %s",
              sourceTableName, spannerTableName));
    }

    if (sourceTable.primaryKeyColumns() == null || sourceTable.primaryKeyColumns().size() == 0) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for source table %s without primary key, skipping the record.",
              sourceTableName));
    }

    Map<String, String> pkcolumnNameValues =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            PostgreSQLDMLGenerator::getMappedColumnValue);
    if (pkcolumnNameValues == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for table %s without primary key, skipping the record",
              sourceTableName));
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          spannerTable, sourceTable, dmlGeneratorRequest, pkcolumnNameValues);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatement(sourceTable.name(), pkcolumnNameValues);
    } else {
      throw new InvalidDMLGenerationException(
          String.format(
              "Unsupported modType: %s for table %s",
              dmlGeneratorRequest.getModType(), spannerTableName));
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      String tableName, Map<String, String> allColumnNameValues, List<String> primaryKeys) {

    String allColumns = "";
    String allValues = "";

    int index = 0;

    for (Map.Entry<String, String> entry : allColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      allColumns += "\"" + colName + "\"";
      allValues += colValue;

      // Add comma if not the last item in this loop
      if (index + 1 < allColumnNameValues.size()) {
        allColumns += ",";
        allValues += ",";
      }
      index++;
    }

    String conflictCols =
        primaryKeys.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(","));
    String updateValues =
        allColumnNameValues.keySet().stream()
            .filter(k -> !primaryKeys.contains(k))
            .map(k -> "\"" + k + "\" = EXCLUDED.\"" + k + "\"")
            .collect(Collectors.joining(","));

    String returnVal =
        "INSERT INTO \"" + tableName + "\" (" + allColumns + ") VALUES (" + allValues + ")";

    if (updateValues.isEmpty()) {
      returnVal += " ON CONFLICT (" + conflictCols + ") DO NOTHING";
    } else {
      returnVal += " ON CONFLICT (" + conflictCols + ") DO UPDATE SET " + updateValues;
    }

    return new DMLGeneratorResponse(returnVal);
  }

  private static DMLGeneratorResponse getDeleteStatement(
      String tableName, Map<String, String> pkcolumnNameValues) {
    String deleteValues = "";

    int index = 0;
    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();

      deleteValues += " \"" + colName + "\" = " + colValue;
      if (index + 1 < pkcolumnNameValues.size()) {
        deleteValues += " AND ";
      }
      index++;
    }
    String returnVal = "DELETE FROM \"" + tableName + "\" WHERE " + deleteValues;

    return new DMLGeneratorResponse(returnVal);
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      Table spannerTable,
      com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, String> pkcolumnNameValues) {
    Map<String, String> columnNameValues =
        DMLGeneratorUtils.getColumnValues(
            dmlGeneratorRequest.getSchemaMapper(),
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            PostgreSQLDMLGenerator::getMappedColumnValue);
    columnNameValues.putAll(pkcolumnNameValues);
    return getUpsertStatement(
        sourceTable.name(), columnNameValues, sourceTable.primaryKeyColumns());
  }

  private static String getMappedColumnValue(
      Column spannerColDef,
      SourceColumn sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {

    String colInputValue = "";
    Type colType = spannerColDef.type();
    String colName = spannerColDef.name();
    if (colType.getCode().equals(Type.Code.FLOAT64)
        || colType.getCode().equals(Type.Code.FLOAT32)
        || colType.getCode().equals(Type.Code.PG_FLOAT4)
        || colType.getCode().equals(Type.Code.PG_FLOAT8)
        || colType.getCode().equals(Type.Code.PG_NUMERIC)) {
      colInputValue = valuesJson.getBigDecimal(colName).toString();
    } else if (colType.getCode().equals(Type.Code.BOOL)
        || colType.getCode().equals(Type.Code.PG_BOOL)) {
      colInputValue = String.valueOf(valuesJson.getBoolean(colName));
    } else if ((colType.getCode().equals(Type.Code.ARRAY)
            && colType.getArrayElementType().getCode().equals(Type.Code.STRING))
        || (colType.getCode().equals(Type.Code.PG_ARRAY)
            && (colType.getArrayElementType().getCode().equals(Type.Code.PG_VARCHAR)
                || colType.getArrayElementType().getCode().equals(Type.Code.PG_TEXT)))) {

      colInputValue =
          valuesJson.getJSONArray(colName).toList().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
    } else if (colType.getCode().equals(Type.Code.BYTES)
        || colType.getCode().equals(Type.Code.PG_BYTEA)) {
      if (sourceColDef.type().toLowerCase().equals("bytea")) {
        colInputValue = convertBase64ToHex(valuesJson.getString(colName));
      } else {
        // Postgres decode: decode('base64string', 'base64')
        colInputValue = "decode('" + valuesJson.getString(colName) + "', 'base64')";
      }
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    String response =
        getColumnValueByType(
            sourceColDef.type().toLowerCase(),
            colInputValue,
            sourceDbTimezoneOffset,
            colType.toString());
    return response;
  }

  @VisibleForTesting
  protected static String convertBase64ToHex(String base64EncodedString) {
    if (base64EncodedString == null) {
      return null;
    }
    if (StringUtils.isEmpty(base64EncodedString)) {
      return "''";
    }

    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      StringBuilder hexStringBuilder = new StringBuilder(decodedBytes.length * 2);
      for (byte b : decodedBytes) {
        hexStringBuilder.append(String.format("%02x", b & 0xFF));
      }
      return "'\\x" + hexStringBuilder.toString() + "'";

    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid Base64 encoded string provided: " + e.getMessage(), e);
    }
  }

  private static String getColumnValueByType(
      String columnType, String colValue, String sourceDbTimezoneOffset, String spannerColType) {
    String response = "";
    switch (columnType) {
      case "varchar":
      case "char":
      case "text":
      case "character varying":
      case "character":
      case "json":
      case "jsonb":
      case "date":
      case "time":
      case "uuid":
        response = getQuotedEscapedString(colValue, spannerColType);
        break;
      case "timestamp":
      case "timestamp without time zone":
      case "timestamp with time zone":
      case "timestamptz":
        // For postgres, we can use the timestamptz string directly or cast it.
        // E.g., '2023-01-01T12:00:00Z'
        response = getQuotedEscapedString(colValue, spannerColType) + "::timestamptz";
        // Optionally applying timezone offset if necessary via AT TIME ZONE, but if the
        // string has a Z, Postgres handles it.
        break;
      case "bytea":
      case "binary":
      case "varbinary":
        response = colValue; // Handled in getMappedColumnValue via decode() or convertBase64ToHex()
        break;
      default:
        response = colValue;
    }
    return response;
  }

  private static String escapeString(String input) {
    String cleanedNullBytes = StringUtils.replace(input, "\u0000", "");
    cleanedNullBytes = StringUtils.replace(cleanedNullBytes, "'", "''");
    // PostgreSQL defaults to standard conforming strings, so backslash is just a
    // backslash, except in E'' strings.
    // For standard string literals '', we just need to escape the single quote as
    // ''
    return cleanedNullBytes;
  }

  private static String getQuotedEscapedString(String input, String spannerColType) {
    if ("BYTES".equals(spannerColType) || "PG_BYTEA".equals(spannerColType)) {
      return input;
    }
    String cleanedString = escapeString(input);
    String response = "'" + cleanedString + "'";
    return response;
  }
}
