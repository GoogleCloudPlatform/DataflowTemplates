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

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates DML statements. */
public class MySQLDMLGenerator implements IDMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLDMLGenerator.class);

  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
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
          "Cannot reverse replicate for table {} without primary key, skipping the record",
          sourceTableName);
      return new DMLGeneratorResponse("");
    }
    Ddl spannerDdl = dmlGeneratorRequest.getDdl();
    Table spannerTable = spannerDdl.table(spannerTableName);

    Map<String, String> pkcolumnNameValues =
        getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
    if (pkcolumnNameValues == null) {
      LOG.warn(
          "Cannot reverse replicate for table {} without primary key, skipping the record",
          sourceTableName);
      return new DMLGeneratorResponse("");
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          schemaMapper, spannerTable, sourceTable, dmlGeneratorRequest, pkcolumnNameValues);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatement(sourceTableName, pkcolumnNameValues);
    } else {
      LOG.warn("Unsupported modType: " + dmlGeneratorRequest.getModType());
      return new DMLGeneratorResponse("");
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      String tableName,
      ImmutableList<String> primaryKeys,
      Map<String, String> columnNameValues,
      Map<String, String> pkcolumnNameValues) {

    String allColumns = "";
    String allValues = "";
    String updateValues = "";

    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();

      allColumns += "`" + colName + "`,";
      allValues += colValue + ",";
    }

    if (columnNameValues.size() == 0) { // if there are only PKs
      // trim the last ','
      allColumns = allColumns.substring(0, allColumns.length() - 1);
      allValues = allValues.substring(0, allValues.length() - 1);

      String returnVal =
          "INSERT INTO `" + tableName + "`(" + allColumns + ")" + " VALUES (" + allValues + ") ";
      return new DMLGeneratorResponse(returnVal);
    }
    int index = 0;

    for (Map.Entry<String, String> entry : columnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      allColumns += "`" + colName + "`";
      allValues += colValue;
      if (!primaryKeys.contains(colName)) {
        updateValues += " `" + colName + "` = " + colValue;
      }

      if (index + 1 < columnNameValues.size()) {
        allColumns += ",";
        allValues += ",";
        updateValues += ",";
      }
      index++;
    }
    String returnVal =
        "INSERT INTO `"
            + tableName
            + "`("
            + allColumns
            + ")"
            + " VALUES ("
            + allValues
            + ") "
            + "ON DUPLICATE KEY UPDATE "
            + updateValues;

    return new DMLGeneratorResponse(returnVal);
  }

  private static DMLGeneratorResponse getDeleteStatement(
      String tableName, Map<String, String> pkcolumnNameValues) {
    String deleteValues = "";

    int index = 0;
    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();

      deleteValues += " `" + colName + "` = " + colValue;
      if (index + 1 < pkcolumnNameValues.size()) {
        deleteValues += " AND ";
      }
      index++;
    }
    String returnVal = "DELETE FROM `" + tableName + "` WHERE " + deleteValues;

    return new DMLGeneratorResponse(returnVal);
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, String> pkcolumnNameValues) {
    Map<String, String> columnNameValues =
        getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
    return getUpsertStatement(
        sourceTable.name(), sourceTable.primaryKeyColumns(), columnNameValues, pkcolumnNameValues);
  }

  private static Map<String, String> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, String> response = new HashMap<>();

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
    ImmutableList<String> sourcePKs = sourceTable.primaryKeyColumns();
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
        response.put(colName, customTransformationResponse.get(colName).toString());
        continue;
      }
      String spannerColumnName = "";
      try {
        spannerColumnName = schemaMapper.getSpannerColumnName("", sourceTable.name(), colName);
      } catch (NoSuchElementException e) {
        continue;
      }
      Column spannerColumn = spannerTable.column(spannerColumnName);
      if (spannerColumn == null) {
        continue;
      }
      String columnValue = "";
      if (keyValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColumnName)) {
          response.put(colName, "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          response.put(colName, "NULL");
          continue;
        }
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

  private static Map<String, String> getPkColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse) {
    Map<String, String> response = new HashMap<>();
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
    ImmutableList<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    for (int i = 0; i < sourcePKs.size(); i++) {
      String currentSourcePK = sourcePKs.get(i);
      String currentSpannerPk =
          schemaMapper.getSpannerColumnName("", sourceTable.name(), currentSourcePK);

      if (customTransformColumns != null && customTransformColumns.contains(currentSourcePK)) {
        response.put(currentSourcePK, customTransformationResponse.get(currentSourcePK).toString());
        continue;
      }

      String columnValue = "";
      Column spannerColumn = spannerTable.column(currentSpannerPk);
      SourceColumn sourceColumn =
          sourceTable.columns().stream()
              .filter(col -> col.name().equals(currentSourcePK))
              .findFirst()
              .orElse(null);
      if (keyValuesJson.has(currentSpannerPk)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(currentSpannerPk)) {
          response.put(currentSourcePK, "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColumn, sourceColumn, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(currentSpannerPk)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(currentSpannerPk)) {
          response.put(currentSourcePK, "NULL");
          continue;
        }
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

  private static String getMappedColumnValue(
      Column spannerColumn,
      SourceColumn sourceColumn,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {

    String colInputValue = "";
    String colType = spannerColumn.type().toString();
    String colName = spannerColumn.name();
    if ("FLOAT64".equals(colType)) {
      colInputValue = valuesJson.getBigDecimal(colName).toString();
    } else if ("BOOL".equals(colType)) {
      colInputValue = (new Boolean(valuesJson.getBoolean(colName))).toString();
    } else if ("ARRAY<STRING>".equals(colType)) {
      colInputValue =
          valuesJson.getJSONArray(colName).toList().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
    } else if ("BYTES".equals(colType)) {
      colInputValue = "FROM_BASE64('" + valuesJson.getString(colName) + "')";
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    String response =
        getColumnValueByType(sourceColumn.type(), colInputValue, sourceDbTimezoneOffset, colType);
    return response;
  }

  private static String getColumnValueByType(
      String columnType, String colValue, String sourceDbTimezoneOffset, String spannerColType) {
    String response = "";
    String cleanedNullBytes = "";
    String decodedString = "";
    switch (columnType) {
      case "varchar":
      case "char":
      case "text":
      case "tinytext":
      case "mediumtext":
      case "longtext":
      case "enum":
      case "date":
      case "time":
      case "year":
      case "set":
      case "json":
      case "geometry":
      case "geometrycollection":
      case "point":
      case "multipoint":
      case "linestring":
      case "multilinestring":
      case "polygon":
      case "multipolygon":
      case "tinyblob":
      case "mediumblob":
      case "blob":
      case "longblob":
        response = getQuotedEscapedString(colValue, spannerColType);
        break;
      case "timestamp":
      case "datetime":
        colValue = colValue.substring(0, colValue.length() - 1); // trim the Z for mysql
        response =
            " CONVERT_TZ("
                + getQuotedEscapedString(colValue, spannerColType)
                + ",'+00:00','"
                + sourceDbTimezoneOffset
                + "')";

        break;
      case "binary":
      case "varbinary":
      case "bit":
        response = getBinaryString(colValue, spannerColType);
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
    if ("BYTES".equals(spannerColType)) {
      return input;
    }
    String cleanedString = escapeString(input);
    String response = "\'" + cleanedString + "\'";
    return response;
  }

  private static String getBinaryString(String input, String spannerColType) {
    String response = "BINARY(" + getQuotedEscapedString(input, spannerColType) + ")";
    return response;
  }
}
