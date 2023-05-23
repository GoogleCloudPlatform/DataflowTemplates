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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.templates.common.ColumnDefinition;
import com.google.cloud.teleport.v2.templates.common.ColumnPK;
import com.google.cloud.teleport.v2.templates.common.Schema;
import com.google.cloud.teleport.v2.templates.common.SourceSchema;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates DML statements. */
public class DMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(DMLGenerator.class);

  public static String getDMLStatement(
      String modType,
      String tableName,
      Schema sourceSchema,
      JSONObject newValuesJson,
      JSONObject keyValuesJson) {

    SourceSchema tableSchema = sourceSchema.getSourceSchemaForTable(tableName);

    Set<String> primaryKeys = tableSchema.getPrimaryKeySet();
    LOG.debug(
        "The input is : "
            + tableSchema
            + " newVal "
            + newValuesJson.toString()
            + " keyValuesJson: "
            + keyValuesJson.toString());
    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      Map<String, String> columnNameValues =
          getColumnValues(tableSchema.getColDefs(), newValuesJson, primaryKeys);
      Map<String, String> pkcolumnNameValues =
          getPkColumnValues(tableSchema.getColDefs(), tableSchema.getPrimaryKeys(), keyValuesJson);
      return getUpsertStatement(tableName, primaryKeys, columnNameValues, pkcolumnNameValues);
    } else if ("DELETE".equals(modType)) {
      Map<String, String> pkcolumnNameValues =
          getPkColumnValues(tableSchema.getColDefs(), tableSchema.getPrimaryKeys(), keyValuesJson);
      return getDeleteStatement(tableName, pkcolumnNameValues);
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
    LOG.info("The UPSERT statement : " + returnVal);
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

    LOG.info("The DELETE statement : " + returnVal);

    return returnVal;
  }

  private static Map<String, String> getColumnValues(
      Map<String, ColumnDefinition> colDefs, JSONObject newValuesJson, Set<String> primaryKeys) {

    Map<String, String> response = new HashMap<>();
    for (Map.Entry<String, ColumnDefinition> entry : colDefs.entrySet()) {
      ColumnDefinition colDef = entry.getValue();

      String colName = colDef.getName();
      if (primaryKeys.contains(colName)) {
        continue; // new values does not have primary key data
      }

      String colInputValue = newValuesJson.getString(colName);
      LOG.debug("The column name and value : " + colName + " : " + colInputValue);
      if (colInputValue.isEmpty()) {
        continue;
      }
      String colValue = getColumnValueByType(colDef.getType().getName(), colInputValue);

      response.put(colName, colValue);
    }

    return response;
  }

  private static Map<String, String> getPkColumnValues(
      Map<String, ColumnDefinition> colDefs, ColumnPK[] primaryKeys, JSONObject keyValuesJson) {

    Map<String, String> response = new HashMap<>();
    for (int i = 0; i < primaryKeys.length; i++) {
      ColumnDefinition colDef = colDefs.get(primaryKeys[i].getColId());

      String colName = colDef.getName();
      String colInputValue = keyValuesJson.getString(colName);

      if (colInputValue.isEmpty()) {
        continue;
      }

      LOG.debug("The column name and value : " + colName + " : " + colInputValue);
      String colValue = getColumnValueByType(colDef.getType().getName(), colInputValue);

      response.put(colName, colValue);
    }
    return response;
  }

  private static String getColumnValueByType(String columnType, String colValue) {
    String response = "";
    switch (columnType) {
      case "varchar":
      case "char":
      case "text":
      case "tinytext":
      case "mediumtext":
      case "longtext":
      case "set":
      case "enum":
      case "binary":
      case "varbinary":
      case "tinyblob":
      case "mediumblob":
      case "blob":
      case "longblob":
        String cleanedNullBytes = StringUtils.replace(colValue, "\u0000", "");
        cleanedNullBytes = StringUtils.replace(cleanedNullBytes, "'", "''");
        response = "\'" + cleanedNullBytes + "\'";
        break;
      default:
        response = colValue;
    }
    return response;
  }
}
