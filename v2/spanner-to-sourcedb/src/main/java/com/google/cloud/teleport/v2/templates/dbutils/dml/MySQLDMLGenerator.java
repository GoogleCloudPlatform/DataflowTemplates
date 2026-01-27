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
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
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
        getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
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
            String.format("Unsupported modType: %s for table %s", dmlGeneratorRequest.getModType(), spannerTableName));
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      String tableName, Map<String, String> allColumnNameValues) {

    String allColumns = "";
    String allValues = "";
    String updateValues = "";

    int index = 0;

    for (Map.Entry<String, String> entry : allColumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = entry.getValue();
      allColumns += "`" + colName + "`";
      allValues += colValue;
      updateValues += " `" + colName + "` = " + colValue;

      // Add comma if not the last item in this loop
      if (index + 1 < allColumnNameValues.size()) {
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
      Table spannerTable,
      com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, String> pkcolumnNameValues) {
    Map<String, String> columnNameValues =
        getColumnValues(
            dmlGeneratorRequest.getSchemaMapper(),
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());
    columnNameValues.putAll(pkcolumnNameValues);
    return getUpsertStatement(sourceTable.name(), columnNameValues);
  }

  private static Map<String, String> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable sourceTable,
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
    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }
    for (SourceColumn sourceColDef : sourceTable.columns()) {
      String colName = sourceColDef.name();
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
      Column spannerColDef = spannerTable.column(spannerColumnName);
      if (spannerColDef == null) {
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
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColumnName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColumnName)) {
          response.put(colName, "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
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
      com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable sourceTable,
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
    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    for (int i = 0; i < sourcePKs.size(); i++) {
      String sourceColName = sourcePKs.get(i);
      SourceColumn sourceColDef = sourceTable.column(sourceColName);
      if (sourceColDef == null) {
        LOG.warn(
            "The source column definition for {} was not found in source schema", sourceColName);
        return null;
      }

      if (customTransformColumns != null && customTransformColumns.contains(sourceColName)) {
        response.put(sourceColName, customTransformationResponse.get(sourceColName).toString());
        continue;
      }

      String spannerColName = "";
      try {
        spannerColName = schemaMapper.getSpannerColumnName("", sourceTable.name(), sourceColName);
      } catch (NoSuchElementException e) {
        continue;
      }
      if (spannerColName == null) {
        LOG.warn(
            "The corresponding spanner table for {} was not found in schema mapping",
            sourceColName);
        return null;
      }
      Column spannerColDef = spannerTable.column(spannerColName);
      if (spannerColDef == null) {
        LOG.warn(
            "The spanner column definition for {} was not found in spanner schema", spannerColName);
        return null;
      }
      String columnValue = "";
      if (keyValuesJson.has(spannerColName)) {
        // get the value based on Spanner and Source type
        if (keyValuesJson.isNull(spannerColName)) {
          response.put(sourceColName, "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(spannerColName)) {
        // get the value based on Spanner and Source type
        if (newValuesJson.isNull(spannerColName)) {
          response.put(sourceColName, "NULL");
          continue;
        }
        columnValue =
            getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn("The column {} was not found in input record", spannerColName);
        return null;
      }

      response.put(sourceColName, columnValue);
    }

    return response;
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
        || colType.getCode().equals(Type.Code.FLOAT32)) {
      // TODO Test and Handle NAN/Infinity.
      colInputValue = valuesJson.getBigDecimal(colName).toString();
    } else if (colType.getCode().equals(Type.Code.BOOL)) {
      colInputValue = (new Boolean(valuesJson.getBoolean(colName))).toString();
    } else if (colType.getCode().equals(Type.Code.ARRAY)
        && colType.getArrayElementType().getCode().equals(Type.Code.STRING)) {
      colInputValue =
          valuesJson.getJSONArray(colName).toList().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
    } else if (colType.getCode().equals(Type.Code.BYTES)) {
      if (sourceColDef.type().toLowerCase().equals("bit")) {
        colInputValue = convertBase64ToHex(valuesJson.getString(colName));
      } else {
        colInputValue = "FROM_BASE64('" + valuesJson.getString(colName) + "')";
      }
    } else {
      colInputValue = valuesJson.getString(colName);
    }
    String response =
        getColumnValueByType(
            sourceColDef.type(), colInputValue, sourceDbTimezoneOffset, colType.toString());
    return response;
  }

  /**
   * Decodes a Base64 encoded string and formats the resulting bytes into a hexadecimal string
   * prefixed with 'x'.
   *
   * @param base64EncodedString The Base64 encoded string to decode.
   * @return A string in the format x'&lt;hex representation of the bytes$gt;', or x'' if the input
   *     is null or empty after decoding.
   * @throws IllegalArgumentException If the input string is not a valid Base64 encoding.
   */
  @VisibleForTesting
  protected static String convertBase64ToHex(String base64EncodedString) {
    if (base64EncodedString == null) {
      return null;
    }
    if (StringUtils.isEmpty(base64EncodedString)) {
      return "x''";
    }

    try {
      // 1. Decode the Base64 string into bytes
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);

      // 2. Convert the bytes to a hexadecimal string
      StringBuilder hexStringBuilder = new StringBuilder(decodedBytes.length * 2);
      for (byte b : decodedBytes) {
        // Use Integer.toHexString to get the hex representation of each byte.
        // & 0xFF ensures that the byte is treated as an unsigned value
        // (otherwise, negative bytes would get a longer hex string like "ffffffxx").
        // We then format it to always be two characters, padding with '0' if necessary.
        hexStringBuilder.append(String.format("%02x", b & 0xFF));
      }

      // 3. Prefix with x' and suffix with '
      return "x'" + hexStringBuilder.toString() + "'";

    } catch (IllegalArgumentException e) {
      // Re-throw or wrap the exception if Base64 decoding fails.
      throw new IllegalArgumentException(
          "Invalid Base64 encoded string provided: " + e.getMessage(), e);
    }
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
