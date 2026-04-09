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
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for generating DML statements securely and uniformly. */
public class DMLGeneratorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DMLGeneratorUtils.class);

  @FunctionalInterface
  public interface ColumnValueMapper {
    String getMappedColumnValue(
        Column spannerColDef,
        SourceColumn sourceColDef,
        JSONObject valuesJson,
        String sourceDbTimezoneOffset);
  }

  public static String convertBase64ToRawHex(String base64EncodedString) {
    if (base64EncodedString == null) {
      return null;
    }
    if (StringUtils.isEmpty(base64EncodedString)) {
      return "";
    }
    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      StringBuilder hexStringBuilder = new StringBuilder(decodedBytes.length * 2);
      for (byte b : decodedBytes) {
        hexStringBuilder.append(String.format("%02x", b & 0xFF));
      }
      return hexStringBuilder.toString();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid Base64 encoded string provided: " + e.getMessage(), e);
    }
  }

  public static Map<String, String> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnValueMapper columnValueMapper) {
    Map<String, String> response = new HashMap<>();

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
      if (sourceColDef.isGenerated()) {
        continue;
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
      String actualColName = spannerColDef.name();
      if (keyValuesJson.has(actualColName)) {
        if (keyValuesJson.isNull(actualColName)) {
          response.put(colName, null);
          continue;
        }
        columnValue =
            columnValueMapper.getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(actualColName)) {
        if (newValuesJson.isNull(actualColName)) {
          response.put(colName, null);
          continue;
        }
        columnValue =
            columnValueMapper.getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        continue;
      }

      response.put(colName, columnValue);
    }

    return response;
  }

  public static Map<String, String> getPkColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnValueMapper columnValueMapper) {
    Map<String, String> response = new HashMap<>();

    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    boolean doesGeneratedColumnExist = false;

    for (int i = 0; i < sourcePKs.size(); i++) {
      String sourceColName = sourcePKs.get(i);
      SourceColumn sourceColDef = sourceTable.column(sourceColName);
      if (sourceColDef == null) {
        LOG.warn(
            "The source column definition for {} was not found in source schema", sourceColName);
        return null;
      }

      if (sourceColDef.isGenerated()) {
        doesGeneratedColumnExist = true;
        continue;
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
      String actualColName = spannerColDef.name();
      if (keyValuesJson.has(actualColName)) {
        if (keyValuesJson.isNull(actualColName)) {
          response.put(sourceColName, null);
          continue;
        }
        columnValue =
            columnValueMapper.getMappedColumnValue(
                spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(actualColName)) {
        if (newValuesJson.isNull(actualColName)) {
          response.put(sourceColName, null);
          continue;
        }
        columnValue =
            columnValueMapper.getMappedColumnValue(
                spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn("The column {} was not found in input record", actualColName);
        return null;
      }

      response.put(sourceColName, columnValue);
    }

    if (doesGeneratedColumnExist) {
      Map<String, String> generatedColumnValues =
          getColumnValues(
              schemaMapper,
              spannerTable,
              sourceTable,
              newValuesJson,
              keyValuesJson,
              sourceDbTimezoneOffset,
              customTransformationResponse,
              columnValueMapper);
      response.putAll(generatedColumnValues);
    }

    return response;
  }
}
