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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
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

    if (sourceTable.primaryKeyColumns() == null || sourceTable.primaryKeyColumns().isEmpty()) {
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

  /**
   * Generates a SQL Server MERGE statement.
   *
   * <p>TODO: for generated PK column, populate onConditionColumnNameValues with old values.
   *
   * @param tableName The target SQL Server table name.
   * @param allColumnNameValues Map of all non-generated column names to their SQL values.
   * @param onConditionColumnNameValues Map of column names to SQL values used in the MERGE ON
   *     clause. When the primary key is a generated column, this contains all non-generated columns
   *     so the database evaluates the generated key; otherwise it contains the primary key columns.
   * @param primaryKeyColumns Set of actual primary key column names defined on the source table,
   *     used to exclude primary key columns from the WHEN MATCHED THEN UPDATE SET clause.
   */
  private static DMLGeneratorResponse getUpsertStatement(
      String tableName,
      Map<String, String> allColumnNameValues,
      Map<String, String> onConditionColumnNameValues,
      Set<String> primaryKeyColumns) {

    StringBuilder updateValues = new StringBuilder();
    StringBuilder insertColumns = new StringBuilder();
    StringBuilder insertValues = new StringBuilder();
    StringBuilder onCondition = new StringBuilder();

    int pkIndex = 0;
    for (Map.Entry<String, String> entry : onConditionColumnNameValues.entrySet()) {
      if (pkIndex > 0) {
        onCondition.append(" AND ");
      }
      // ON Condition: target.[col_name] = col_value or target.[col_name] IS NULL
      if (entry.getValue() == null) {
        onCondition.append("target.[").append(entry.getKey()).append("] IS NULL");
      } else {
        onCondition
            .append("target.[")
            .append(entry.getKey())
            .append("] = ")
            .append(entry.getValue());
      }
      pkIndex++;
    }

    for (Map.Entry<String, String> entry : allColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      String sqlValue = (colValue == null) ? "NULL" : colValue;

      if (!insertColumns.isEmpty()) {
        insertColumns.append(", ");
        insertValues.append(", ");
      }
      // [col_name]
      insertColumns.append("[").append(colName).append("]");
      insertValues.append(sqlValue);

      if (!primaryKeyColumns.contains(colName)) {
        if (!updateValues.isEmpty()) {
          updateValues.append(", ");
        }
        // target.[col_name] = col_value
        updateValues.append("target.[").append(colName).append("] = ").append(sqlValue);
      }
    }

    // MERGE INTO TargetTable AS target
    // USING (SELECT 1 AS dummy) AS source
    //    ON target.Id = source.Id
    // WHEN MATCHED THEN
    //    UPDATE SET
    //        target.Name = source.Name,
    //        target.Age = source.Age
    // WHEN NOT MATCHED THEN
    //    INSERT (Id, Name, Age)
    //    VALUES (source.Id, source.Name, source.Age);
    String returnVal =
        "MERGE INTO ["
            + tableName
            + "] AS target "
            + "USING (SELECT 1 AS dummy) AS source "
            + "ON ("
            + onCondition
            + ") ";

    if (!updateValues.isEmpty()) {
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
      String tableName, Map<String, String> pkColumnNameValues) {
    StringBuilder deleteValues = new StringBuilder();

    int index = 0;
    for (Map.Entry<String, String> entry : pkColumnNameValues.entrySet()) {
      if (index > 0) {
        deleteValues.append(" AND ");
      }
      String colName = entry.getKey();
      String colValue = entry.getValue();

      // [col_name] = col_value or [col_name] IS NULL
      if (colValue == null) {
        deleteValues.append(" [").append(colName).append("] IS NULL");
      } else {
        deleteValues.append(" [").append(colName).append("] = ").append(colValue);
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
    return getUpsertStatement(
        sourceTable.name(),
        allColumnNameValues,
        pkcolumnNameValues,
        new HashSet<>(sourceTable.primaryKeyColumns()));
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
    } else if (colType.getCode().equals(Type.Code.ARRAY)
        || colType.getCode().equals(Type.Code.PG_ARRAY)) {
      if (valuesJson.optJSONArray(colName) != null) {
        colInputValue = valuesJson.getJSONArray(colName).toString();
      } else {
        colInputValue = valuesJson.getString(colName);
      }
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    return getColumnValueByType(
        sourceColDef.type(), colInputValue, sourceDbTimezoneOffset, colType.toString());
  }

  @VisibleForTesting
  protected static String convertBase64ToHex(String base64EncodedString) {
    String rawHex = DMLGeneratorUtils.convertBase64ToRawHex(base64EncodedString);
    return rawHex == null ? null : "0x" + rawHex;
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
      case "json":
      case "vector":
      case "date":
      case "time":
        response = getQuotedEscapedString(colValue, spannerColType);
        break;
      case "datetimeoffset":
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
    cleanedNullBytes = StringUtils.replace(cleanedNullBytes, "\\", "\\\\");
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
    return "\'" + cleanedString + "\'";
  }
}
