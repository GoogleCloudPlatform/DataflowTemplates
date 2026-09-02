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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

public class SQLServerDMLGenerator implements IDMLGenerator {

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
            SQLServerDMLGenerator::getMappedColumnValue,
            new ArrayList<>());
    if (pkcolumnNameValues == null || pkcolumnNameValues.isEmpty()) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for table %s without primary key, skipping the record",
              sourceTableName));
    }

    Map<String, String> realPkColumnValues = new java.util.LinkedHashMap<>();
    for (String pkColName : sourceTable.primaryKeyColumns()) {
      SourceColumn pkColDef = sourceTable.column(pkColName);
      if (pkColDef == null) {
        continue;
      }
      if (pkColDef.isGenerated()) {
        String spannerColName = "";
        try {
          spannerColName = schemaMapper.getSpannerColumnName("", sourceTable.name(), pkColName);
        } catch (NoSuchElementException e) {
          continue;
        }
        if (spannerColName != null) {
          Column spannerColDef = spannerTable.column(spannerColName);
          if (spannerColDef != null) {
            JSONObject keyValuesJson = dmlGeneratorRequest.getKeyValuesJson();
            JSONObject newValuesJson = dmlGeneratorRequest.getNewValuesJson();
            if (keyValuesJson != null
                && keyValuesJson.has(spannerColName)
                && !keyValuesJson.isNull(spannerColName)) {
              realPkColumnValues.put(
                  pkColName,
                  getMappedColumnValue(
                      spannerColDef,
                      pkColDef,
                      keyValuesJson,
                      dmlGeneratorRequest.getSourceDbTimezoneOffset(),
                      new ArrayList<>()));
            } else if (newValuesJson != null
                && newValuesJson.has(spannerColName)
                && !newValuesJson.isNull(spannerColName)) {
              realPkColumnValues.put(
                  pkColName,
                  getMappedColumnValue(
                      spannerColDef,
                      pkColDef,
                      newValuesJson,
                      dmlGeneratorRequest.getSourceDbTimezoneOffset(),
                      new ArrayList<>()));
            }
          }
        }
      } else if (pkcolumnNameValues.containsKey(pkColName)) {
        realPkColumnValues.put(pkColName, pkcolumnNameValues.get(pkColName));
      }
    }
    if (realPkColumnValues.isEmpty()) {
      realPkColumnValues = pkcolumnNameValues;
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          spannerTable, sourceTable, dmlGeneratorRequest, realPkColumnValues, pkcolumnNameValues);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatement(sourceTable.name(), realPkColumnValues);
    } else {
      throw new InvalidDMLGenerationException(
          String.format(
              "Unsupported modType: %s for table %s",
              dmlGeneratorRequest.getModType(), spannerTableName));
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      SourceTable sourceTable,
      String tableName,
      Map<String, String> allColumnNameValues,
      Map<String, String> pkColumnNameValues) {

    String updateValues = "";
    String insertColumns = "";
    String insertValues = "";
    String onCondition = "";

    int pkIndex = 0;
    for (Map.Entry<String, String> entry : pkColumnNameValues.entrySet()) {
      if (pkIndex > 0) {
        onCondition += " AND ";
      }
      onCondition += "target.[" + entry.getKey() + "] = " + entry.getValue();
      pkIndex++;
    }

    for (Map.Entry<String, String> entry : allColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      SourceColumn sourceCol = (sourceTable != null) ? sourceTable.column(colName) : null;
      if (sourceCol != null && sourceCol.isGenerated()) {
        continue;
      }
      String colValue = entry.getValue();
      String sqlValue = (colValue == null) ? "NULL" : colValue;

      if (!insertColumns.isEmpty()) {
        insertColumns += ", ";
        insertValues += ", ";
      }
      insertColumns += "[" + colName + "]";
      insertValues += sqlValue;

      if (!pkColumnNameValues.containsKey(colName)) {
        if (updateValues.length() > 0) {
          updateValues += ", ";
        }
        updateValues += "target.[" + colName + "] = " + sqlValue;
      }
    }

    String returnVal =
        "MERGE INTO ["
            + tableName
            + "] AS target "
            + "USING (SELECT 1 AS dummy) AS source "
            + "ON ("
            + onCondition
            + ") ";

    if (updateValues.length() > 0) {
      returnVal += "WHEN MATCHED THEN UPDATE SET " + updateValues + " ";
    }
    if (!insertColumns.isEmpty()) {
      returnVal +=
          "WHEN NOT MATCHED THEN INSERT (" + insertColumns + ") VALUES (" + insertValues + ");";
    } else {
      returnVal += "WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;";
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

      deleteValues += " [" + colName + "] = " + colValue;
      if (index + 1 < pkcolumnNameValues.size()) {
        deleteValues += " AND ";
      }
      index++;
    }
    String returnVal = "DELETE FROM [" + tableName + "] WHERE " + deleteValues;

    return new DMLGeneratorResponse(returnVal);
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      Table spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, String> realPkColumnValues,
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
            SQLServerDMLGenerator::getMappedColumnValue,
            new ArrayList<>());
    Map<String, String> allColumnNameValues = new java.util.LinkedHashMap<>();
    allColumnNameValues.putAll(pkcolumnNameValues);
    allColumnNameValues.putAll(columnNameValues);
    allColumnNameValues.putAll(realPkColumnValues);
    return getUpsertStatement(
        sourceTable, sourceTable.name(), allColumnNameValues, realPkColumnValues);
  }

  @VisibleForTesting
  static String getMappedColumnValue(
      Column spannerColDef,
      SourceColumn sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset,
      List<Object> preparedStatementParameters) {

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
      // SQL Server bit: 1 for true, 0 for false
      boolean b = valuesJson.getBoolean(colName);
      colInputValue = b ? "1" : "0";
    } else if (colType.getCode().equals(Type.Code.BYTES)
        || colType.getCode().equals(Type.Code.PG_BYTEA)) {
      colInputValue = convertBase64ToHex(valuesJson.getString(colName));
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    String response =
        getColumnValueByType(
            sourceColDef.type(), colInputValue, sourceDbTimezoneOffset, colType.toString());
    return response;
  }

  @VisibleForTesting
  protected static String convertBase64ToHex(String base64EncodedString) {
    String rawHex = DMLGeneratorUtils.convertBase64ToRawHex(base64EncodedString);
    if (rawHex == null) {
      return null;
    }
    return rawHex.isEmpty() ? "0x" : "0x" + rawHex;
  }

  @VisibleForTesting
  static String getColumnValueByType(
      String columnType, String colValue, String sourceDbTimezoneOffset, String spannerColType) {
    String response = "";
    switch (columnType.toLowerCase()) {
      case "varchar":
      case "char":
      case "text":
      case "nvarchar":
      case "nchar":
      case "ntext":
      case "sysname":
      case "xml":
      case "date":
      case "time":
        response = getQuotedEscapedString(colValue, spannerColType);
        break;
      case "datetimeoffset":
        if (sourceDbTimezoneOffset != null
            && !sourceDbTimezoneOffset.isEmpty()
            && ("TIMESTAMP".equals(spannerColType) || "PG_TIMESTAMPTZ".equals(spannerColType))) {
          if (colValue == null || "null".equalsIgnoreCase(colValue)) {
            response = "NULL";
          } else {
            response =
                "SWITCHOFFSET("
                    + getQuotedEscapedString(colValue, spannerColType)
                    + ", '"
                    + sourceDbTimezoneOffset
                    + "')";
          }
        } else {
          response = getQuotedEscapedString(colValue, spannerColType);
        }
        break;
      case "datetime2":
      case "datetime":
      case "smalldatetime":
        if (sourceDbTimezoneOffset != null
            && !sourceDbTimezoneOffset.isEmpty()
            && ("TIMESTAMP".equals(spannerColType) || "PG_TIMESTAMPTZ".equals(spannerColType))) {
          if (colValue == null || "null".equalsIgnoreCase(colValue)) {
            response = "NULL";
          } else {
            response =
                "CAST(SWITCHOFFSET("
                    + getQuotedEscapedString(colValue, spannerColType)
                    + ", '"
                    + sourceDbTimezoneOffset
                    + "') AS "
                    + columnType.toUpperCase()
                    + ")";
          }
        } else {
          response = getQuotedEscapedString(colValue, spannerColType);
        }
        break;
      case "uniqueidentifier":
        if ("BYTES".equals(spannerColType) || "PG_BYTEA".equals(spannerColType)) {
          if (colValue == null || "null".equalsIgnoreCase(colValue)) {
            response = "NULL";
          } else {
            response = "CAST(" + colValue + " AS UNIQUEIDENTIFIER)";
          }
        } else {
          response = getQuotedEscapedString(colValue, spannerColType);
        }
        break;
      case "binary":
      case "varbinary":
      case "image":
        if (colValue == null || "null".equalsIgnoreCase(colValue)) {
          response = "NULL";
        } else if (!colValue.startsWith("0x") && !colValue.startsWith("0X")) {
          response = "0x" + colValue;
        } else {
          response = colValue;
        }
        break;
      case "bit":
        response = colValue.equals("true") || colValue.equals("1") ? "1" : "0";
        break;
      default:
        response = colValue;
    }
    return response;
  }

  private static String escapeString(String input) {
    String cleanedNullBytes = StringUtils.replace(input, "\u0000", "");
    cleanedNullBytes = StringUtils.replace(cleanedNullBytes, "'", "''");
    return cleanedNullBytes;
  }

  private static String getQuotedEscapedString(String input, String spannerColType) {
    if ("BYTES".equals(spannerColType) || "PG_BYTEA".equals(spannerColType)) {
      if (input == null || "null".equalsIgnoreCase(input)) {
        return "NULL";
      }
      return "CAST(" + input + " AS VARCHAR(MAX))";
    }
    String cleanedString = escapeString(input);
    String response = "\'" + cleanedString + "\'";
    return response;
  }
}
