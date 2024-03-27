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
package com.google.cloud.teleport.v2.templates.processing.dml;

import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates PostgreSQL DML statements. */
public class PostgreSQLDMLGenerator extends DMLGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLDMLGenerator.class);

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

      allColumns += "\"" + colName + "\",";
      allValues += colValue + ",";
    }

    if (columnNameValues.size() == 0) { // if there are only PKs
      // trim the last ','
      allColumns = allColumns.substring(0, allColumns.length() - 1);
      allValues = allValues.substring(0, allValues.length() - 1);

      String returnVal =
          "INSERT INTO \"" + tableName + "\"(" + allColumns + ")" + " VALUES (" + allValues + ")";
      return returnVal;
    }
    int index = 0;

    for (Map.Entry<String, String> entry : columnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      allColumns += "\"" + colName + "\"";
      allValues += colValue;
      if (!primaryKeys.contains(colName)) {
        updateValues += " \"" + colName + "\" = " + "EXCLUDED.\"" + colName + "\"";
      }

      if (index + 1 < columnNameValues.size()) {
        allColumns += ",";
        allValues += ",";
        updateValues += ",";
      }
      index++;
    }

    String compositePrimaryKey = String.join("\",\"", primaryKeys);

    String returnVal =
        "INSERT INTO \""
            + tableName
            + "\"("
            + allColumns
            + ")"
            + " VALUES ("
            + allValues
            + ") "
            + "ON CONFLICT(\""
            + compositePrimaryKey
            + "\") DO UPDATE SET" // TODO for concurrent updates this need to be tested
            + updateValues;
    // LOG.debug("Generated upsert query: " + returnVal);

    return returnVal;
  }

  @Override
  String getDeleteStatement(String tableName, Map<String, String> pkcolumnNameValues) {
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
    String returnVal = "DELETE FROM " + tableName + " WHERE " + deleteValues;

    return returnVal;
  }

  @Override
  String getColumnValueByType(String columnType, String colValue, String sourceDbTimezoneOffset) {
    String response = "";
    String cleanedNullBytes = "";
    String decodedString = "";
    columnType = columnType.toLowerCase();
    switch (columnType) {
      case "varchar":
      case "char":
      case "bpchar":
      case "character":
      case "character varying":
      case "text":
      case "tsvector":
      case "tsquery":
      case "uuid":

      case "user-defined": // USER-DEFINED - for enum for certain versions

      case "date":
      case "time":
      case "time without time zone":
      case "timestamp":
      case "timestamp without time zone":

      case "interval":

      case "json":
      case "jsonb":

      case "path":

      case "polygon":
      case "line":
      case "lseg":
      case "circle":
      case "box":

      case "macaddr":
      case "macaddr8":
      case "cidr":
      case "inet":

      case "money":

      case "bytea":
        response = getQuotedEscapedString(colValue);
        break;
      case "timetz": // this type should be avoided as it cannot deal with DST rules
      case "time with time zone": // this should be avoided as it cannot deal with DST rules
        colValue = colValue.substring(0, colValue.length() - 1); // trim the Z
        response =
            " TO_CHAR((TIMESTAMP "
                + getQuotedEscapedString(colValue)
                + " AT TIME ZONE '+00:00' AT TIME ZONE '"
                + sourceDbTimezoneOffset
                + "'), "
                + "'HH24:MI:SS')";
        break;
      case "timestamp with time zone":
      case "timestamptz":
        colValue = colValue.substring(0, colValue.length() - 1); // trim the Z
        response =
            " TO_CHAR((TIMESTAMP "
                + getQuotedEscapedString(colValue)
                + " AT TIME ZONE '+00:00' AT TIME ZONE '"
                + sourceDbTimezoneOffset
                + "'), "
                + "'YYYY-MM-DD HH24:MI:SS')";
        break;
      case "bit":
      case "bit varying":
        response = getHexString(colValue);
        break;
      case "xml":
        response = " XMLPARSE(" + getQuotedEscapedString(colValue) + ")";
        break;

      case "point":
        response = " point (" + colValue + ")";
        break;

      default:
        response = colValue;
    }
    return response;
  }
}
