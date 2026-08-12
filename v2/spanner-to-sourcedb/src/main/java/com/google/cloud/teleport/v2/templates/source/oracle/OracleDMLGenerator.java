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
package com.google.cloud.teleport.v2.templates.source.oracle;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.dbutils.dml.DMLGeneratorUtils;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/** Creates DML statements for PostgreSQL. */
public class OracleDMLGenerator implements IDMLGenerator {

  private static final ThreadLocal<java.util.List<Object>> threadLocalParameters =
      new ThreadLocal<>();

  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
    try {
      threadLocalParameters.set(new java.util.ArrayList<>());
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
      SourceTable sourceTable = sourceSchema.table(sourceTableName);
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
              OracleDMLGenerator::getMappedColumnValue);
      if (pkcolumnNameValues == null || pkcolumnNameValues.isEmpty()) {
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
        DMLGeneratorResponse resp = getDeleteStatement(sourceTable.name(), pkcolumnNameValues);
        resp.setPreparedStatementParameters(new java.util.ArrayList<>(threadLocalParameters.get()));
        return resp;
      } else {
        throw new InvalidDMLGenerationException(
            String.format(
                "Unsupported modType: %s for table %s",
                dmlGeneratorRequest.getModType(), spannerTableName));
      }
    } finally {
      threadLocalParameters.remove();
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      String tableName,
      Map<String, String> allColumnNameValues,
      Map<String, String> generatedColumnValues,
      List<String> primaryKeys) {

    Map<String, String> queryColumns = new java.util.LinkedHashMap<>(allColumnNameValues);
    if (generatedColumnValues != null) {
      queryColumns.putAll(generatedColumnValues);
    }

    StringBuilder usingSelect = new StringBuilder("SELECT ");
    int index = 0;
    for (Map.Entry<String, String> entry : queryColumns.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      String sqlValue = (colValue == null) ? "NULL" : colValue;
      usingSelect.append(sqlValue).append(" AS \"").append(colName).append("\"");
      if (index + 1 < queryColumns.size()) {
        usingSelect.append(", ");
      }
      index++;
    }
    usingSelect.append(" FROM DUAL");

    String onClause =
        primaryKeys.stream()
            .map(k -> "t.\"" + k + "\" = s.\"" + k + "\"")
            .collect(Collectors.joining(" AND "));

    List<String> nonPkCols =
        allColumnNameValues.keySet().stream()
            .filter(k -> !primaryKeys.contains(k))
            .collect(Collectors.toList());

    StringBuilder mergeQuery = new StringBuilder();
    mergeQuery.append("MERGE INTO \"").append(tableName).append("\" t ");
    mergeQuery.append("USING (").append(usingSelect).append(") s ");
    mergeQuery.append("ON (").append(onClause).append(") ");

    if (!nonPkCols.isEmpty()) {
      String updateSet =
          nonPkCols.stream()
              .map(k -> "t.\"" + k + "\" = s.\"" + k + "\"")
              .collect(Collectors.joining(", "));
      mergeQuery.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
    }

    String insertCols =
        allColumnNameValues.keySet().stream()
            .map(k -> "\"" + k + "\"")
            .collect(Collectors.joining(", "));
    String insertVals =
        allColumnNameValues.keySet().stream()
            .map(k -> "s.\"" + k + "\"")
            .collect(Collectors.joining(", "));
    mergeQuery
        .append("WHEN NOT MATCHED THEN INSERT (")
        .append(insertCols)
        .append(") VALUES (")
        .append(insertVals)
        .append(")");

    return new DMLGeneratorResponse(mergeQuery.toString());
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
      SourceTable sourceTable,
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
            OracleDMLGenerator::getMappedColumnValue);

    Map<String, String> orderedColumnNameValues = new java.util.LinkedHashMap<>();
    orderedColumnNameValues.putAll(pkcolumnNameValues);
    orderedColumnNameValues.putAll(columnNameValues);

    Map<String, String> generatedColumnValues = new java.util.LinkedHashMap<>();
    for (SourceColumn col : sourceTable.columns()) {
      if (col.isGenerated()) {
        try {
          String spannerColName =
              dmlGeneratorRequest
                  .getSchemaMapper()
                  .getSpannerColumnName("", sourceTable.name(), col.name());
          Column spannerColDef = spannerTable.column(spannerColName);
          if (dmlGeneratorRequest.getKeyValuesJson().has(spannerColName)
              && !dmlGeneratorRequest.getKeyValuesJson().isNull(spannerColName)) {
            generatedColumnValues.put(
                col.name(),
                getMappedColumnValue(
                    spannerColDef,
                    col,
                    dmlGeneratorRequest.getKeyValuesJson(),
                    dmlGeneratorRequest.getSourceDbTimezoneOffset()));
          } else if (dmlGeneratorRequest.getNewValuesJson().has(spannerColName)
              && !dmlGeneratorRequest.getNewValuesJson().isNull(spannerColName)) {
            generatedColumnValues.put(
                col.name(),
                getMappedColumnValue(
                    spannerColDef,
                    col,
                    dmlGeneratorRequest.getNewValuesJson(),
                    dmlGeneratorRequest.getSourceDbTimezoneOffset()));
          }
        } catch (Exception e) {
        }
      }
    }

    DMLGeneratorResponse resp =
        getUpsertStatement(
            sourceTable.name(),
            orderedColumnNameValues,
            generatedColumnValues,
            sourceTable.primaryKeyColumns());
    resp.setPreparedStatementParameters(new java.util.ArrayList<>(threadLocalParameters.get()));
    return resp;
  }

  @VisibleForTesting
  static String getMappedColumnValue(
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
      if (threadLocalParameters.get() != null) {
        byte[] decodedBytes = java.util.Base64.getDecoder().decode(valuesJson.getString(colName));
        threadLocalParameters.get().add(decodedBytes);
        colInputValue = "?";
      } else {
        if (sourceColDef.type().toLowerCase().equals("bytea")) {
          colInputValue = convertBase64ToHex(valuesJson.getString(colName));
        } else {
          colInputValue = convertBase64ToHex(valuesJson.getString(colName));
        }
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
    String rawHex = DMLGeneratorUtils.convertBase64ToRawHex(base64EncodedString);
    if (rawHex == null) {
      return null;
    }
    return rawHex.isEmpty() ? "''" : "HEXTORAW('" + rawHex + "')";
  }

  private static String getColumnValueByType(
      String columnType, String colValue, String sourceDbTimezoneOffset, String spannerColType) {
    String response = "";
    // TODO: Add support for array types (e.g., varchar[], integer[]) to generate valid PostgreSQL
    // array literals.
    if (columnType != null && columnType.contains("(")) {
      columnType = columnType.substring(0, columnType.indexOf("(")).trim();
    }
    switch (columnType) {
      case "nvarchar2":
      case "nclob":
      case "nchar":
      case "nchar varying":
      case "national char":
      case "national char varying":
      case "national character":
      case "national character varying":
        response = getQuotedEscapedString(colValue, spannerColType);
        if (response.startsWith("HEXTORAW(")) {
          response = "TO_NCLOB(UTL_RAW.CAST_TO_NVARCHAR2(" + response + "))";
        } else {
          response = "N" + response;
        }
        break;
      case "varchar":
      case "varchar2":
      case "clob":
      case "char":
      case "text":
      case "character varying":
      case "character":
      case "json":
      case "jsonb":
      case "time":
      case "uuid":
        response = getQuotedEscapedString(colValue, spannerColType);
        break;
      case "date":
        if (spannerColType.equalsIgnoreCase("DATE")) {
          response =
              "TO_DATE(" + getQuotedEscapedString(colValue, spannerColType) + ", 'YYYY-MM-DD')";
        } else {
          response =
              "TO_TIMESTAMP_TZ("
                  + getQuotedEscapedString(colValue, spannerColType)
                  + ", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"')";
        }
        break;
      case "timestamp":
      case "timestamp without time zone":
      case "timestamp with time zone":
      case "timestamp with local time zone":
      case "timestamptz":
        if (sourceDbTimezoneOffset != null
            && !sourceDbTimezoneOffset.isEmpty()
            && !"+00:00".equals(sourceDbTimezoneOffset)
            && !"Z".equalsIgnoreCase(sourceDbTimezoneOffset)) {
          response =
              "CAST(FROM_TZ(CAST(TO_TIMESTAMP("
                  + getQuotedEscapedString(colValue, spannerColType)
                  + ", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"') AS TIMESTAMP), 'UTC') AT TIME ZONE '"
                  + sourceDbTimezoneOffset
                  + "' AS TIMESTAMP)";
        } else {
          response =
              "TO_TIMESTAMP("
                  + getQuotedEscapedString(colValue, spannerColType)
                  + ", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"')";
        }
        break;
      case "bytea":
      case "blob":
      case "raw":
      case "binary":
      case "varbinary":
        response = colValue;
        break;
      case "number":
      case "numeric":
      case "decimal":
      case "float":
      case "double precision":
      case "integer":
      case "smallint":
      case "int":
        if ("true".equalsIgnoreCase(colValue)) {
          response = "1";
        } else if ("false".equalsIgnoreCase(colValue)) {
          response = "0";
        } else {
          response = colValue;
        }
        break;
      default:
        if ("true".equalsIgnoreCase(colValue)) {
          response = "1";
        } else if ("false".equalsIgnoreCase(colValue)) {
          response = "0";
        } else {
          response = colValue;
        }
    }
    return response;
  }

  private static String escapeString(String input) {
    String cleanedNullBytes = StringUtils.replace(input, "\u0000", "");
    cleanedNullBytes = StringUtils.replace(cleanedNullBytes, "'", "''");
    // PostgreSQL defaults to standard conforming strings, so backslash is just a
    // backslash.
    // For standard string literals '', we just need to escape the single quote as
    // ''
    return cleanedNullBytes;
  }

  static String getQuotedEscapedString(String input, String spannerColType) {
    if ("BYTES".equals(spannerColType) || "PG_BYTEA".equals(spannerColType)) {
      return input;
    }
    String cleanedString = escapeString(input);
    String response = "'" + cleanedString + "'";
    return response;
  }
}
