/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.processing.dml;

import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates DML statements. */
public class DMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(DMLGenerator.class);

  public static String getDMLStatement(
      String modType,
      String spannerTableName,
      Schema schema,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {

    String spannerTableId = schema.getSpannerToID().get(spannerTableName).getName();
    SpannerTable spannerTable = schema.getSpSchema().get(spannerTableId);

    SourceTable sourceTable = schema.getSrcSchema().get(spannerTableId);
    if (sourceTable == null) {
      LOG.warn("The table {} was not found in source", spannerTableName);
      return "";
    }

    if (sourceTable.getPrimaryKeys() == null || sourceTable.getPrimaryKeys().length == 0) {
      LOG.warn(
          "Cannot reverse replicate for table {} without primary key, skipping the record",
          sourceTable.getName());
      return "";
    }

    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      Map<String, String> pkcolumnNameValues =
          getPkColumnValues(
              spannerTable, sourceTable, newValuesJson, keyValuesJson, sourceDbTimezoneOffset);
      if (pkcolumnNameValues == null) {
        LOG.warn(
            "Cannot reverse replicate for table {} without primary key, skipping the record",
            sourceTable.getName());
        return "";
      }
      Map<String, String> columnNameValues =
          getColumnValues(
              spannerTable, sourceTable, newValuesJson, keyValuesJson, sourceDbTimezoneOffset);
      return getUpsertStatement(
          sourceTable.getName(),
          sourceTable.getPrimaryKeySet(),
          columnNameValues,
          pkcolumnNameValues);
    } else if ("DELETE".equals(modType)) {

      Map<String, String> pkcolumnNameValues =
          getPkColumnValues(
              spannerTable, sourceTable, newValuesJson, keyValuesJson, sourceDbTimezoneOffset);
      if (pkcolumnNameValues == null) {
        LOG.warn(
            "Cannot reverse replicate for table {} without primary key, skipping the record",
            sourceTable.getName());
        return "";
      }
      return getDeleteStatement(sourceTable.getName(), pkcolumnNameValues);
    } else {
      LOG.warn("Unsupported modType: " + modType);
      return "";
    }
  }

  private static String getUpsertStatement(
      String tableName,
      Set<String> primaryKeys,
      Map<String, String> columnNameValues,
      Map<String, String> pkcolumnNameValues) {

    String allColumns = "";
    String allValues = "";
    String updateValues = "";

    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();

      allColumns += colName + ",";
      allValues += colValue + ",";
    }

    if (columnNameValues.size() == 0) { // if there are only PKs
      // trim the last ','
      allColumns = allColumns.substring(0, allColumns.length() - 1);
      allValues = allValues.substring(0, allValues.length() - 1);

      String returnVal =
          "INSERT INTO " + tableName + "(" + allColumns + ")" + " VALUES (" + allValues + ") ";
      return returnVal;
    }
    int index = 0;

    for (Map.Entry<String, String> entry : columnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      allColumns += colName;
      allValues += colValue;
      if (!primaryKeys.contains(colName)) {
        updateValues += " " + colName + " = " + colValue;
      }

      if (index + 1 < columnNameValues.size()) {
        allColumns += ",";
        allValues += ",";
        updateValues += ",";
      }
      index++;
    }
    String returnVal =
        "INSERT INTO "
            + tableName
            + "("
            + allColumns
            + ")"
            + " VALUES ("
            + allValues
            + ") "
            + "ON DUPLICATE KEY UPDATE "
            + updateValues;

    return returnVal;
  }

  private static String getDeleteStatement(
      String tableName, Map<String, String> pkcolumnNameValues) {
    String deleteValues = "";

    int index = 0;
    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();

      deleteValues += " " + colName + " = " + colValue;
      if (index + 1 < pkcolumnNameValues.size()) {
        deleteValues += ",";
      }
      index++;
    }
    String returnVal = "DELETE FROM " + tableName + " WHERE " + deleteValues;

    return returnVal;
  }

  private static Map<String, String> getColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
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
      String columnValue = "";
      if (keyValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColumnName)) {
          response.put(sourceColDef.getName(), "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          response.put(sourceColDef.getName(), "NULL");
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

  private static Map<String, String> getPkColumnValues(
      SpannerTable spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset) {
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
    ColumnPK[] sourcePKs = sourceTable.getPrimaryKeys();

    for (int i = 0; i < sourcePKs.length; i++) {
      ColumnPK currentSourcePK = sourcePKs[i];
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
      String columnValue = "";
      if (keyValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColumnName)) {
          response.put(sourceColDef.getName(), "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          response.put(sourceColDef.getName(), "NULL");
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

  private static String getMappedColumnValue(
      SpannerColumnDefinition spannerColDef,
      SourceColumnDefinition sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {

    String colInputValue = "";
    String colType = spannerColDef.getType().getName();
    String colName = spannerColDef.getName();
    if ("FLOAT64".equals(colType)) {
      colInputValue = valuesJson.getBigDecimal(colName).toString();
    } else if ("BOOL".equals(colType)) {
      colInputValue = (new Boolean(valuesJson.getBoolean(colName))).toString();
    } else if ("STRING".equals(colType) && spannerColDef.getType().getIsArray()) {
      colInputValue =
          valuesJson.getJSONArray(colName).toList().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
    } else if ("STRING".equals(colType)
        && ("binary".equals(sourceColDef.getType().getName())
            || "varbinary".equals(sourceColDef.getType().getName()))) {

      // Spanner has the hex string in this case
      try {
        colInputValue = new String(Hex.decodeHex(valuesJson.getString(colName)));
      } catch (DecoderException e) {
        // return the same string value
        colInputValue = valuesJson.getString(colName);
      }

    } else if ("BYTES".equals(colType)) {
      colInputValue = new String(Base64.decodeBase64(valuesJson.getString(colName).getBytes()));
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    String response =
        getColumnValueByType(
            sourceColDef.getType().getName(), colInputValue, sourceDbTimezoneOffset);
    return response;
  }

  private static String getColumnValueByType(
      String columnType, String colValue, String sourceDbTimezoneOffset) {
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
        response = getQuotedEscapedString(colValue);
        break;
      case "timestamp":
      case "datetime":
        colValue = colValue.substring(0, colValue.length() - 1); // trim the Z for mysql
        response =
            " CONVERT_TZ("
                + getQuotedEscapedString(colValue)
                + ",'+00:00','"
                + sourceDbTimezoneOffset
                + "')";

        break;
      case "binary":
      case "varbinary":
        response = getHexString(colValue);
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

  private static String getQuotedEscapedString(String input) {
    String cleanedString = escapeString(input);
    String response = "\'" + cleanedString + "\'";
    return response;
  }

  private static String getHexString(String input) {
    String cleanedString = escapeString(input);
    String hexString = Hex.encodeHexString(cleanedString.getBytes());
    String response = "X\'" + hexString + "\'";
    return response;
  }
}
