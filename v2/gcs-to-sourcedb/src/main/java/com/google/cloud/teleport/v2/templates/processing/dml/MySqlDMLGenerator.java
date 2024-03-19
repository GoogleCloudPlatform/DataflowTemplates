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

import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates DML statements. */
public class MySqlDMLGenerator extends DMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlDMLGenerator.class);

  @Override
  String getUpsertStatement(
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

  @Override
  String getDeleteStatement(String tableName, Map<String, String> pkcolumnNameValues) {
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

  @Override
  String getColumnValueByType(String columnType, String colValue, String sourceDbTimezoneOffset) {
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
}
