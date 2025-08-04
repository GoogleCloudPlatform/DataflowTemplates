/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatastreamToPostgresDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToPostgresDML.class);

  private DatastreamToPostgresDML(DataSourceConfiguration config) {
    super(config);
  }

  public static DatastreamToPostgresDML of(DataSourceConfiguration config) {
    return new DatastreamToPostgresDML(config);
  }

  @Override
  public String getColumnsUpdateSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String onUpdateSql = "";
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (!tableSchema.containsKey(columnName)) {
        continue;
      }

      String quotedColumnName = quote(columnName);
      String columnValue = getValueSql(rowObj, columnName, tableSchema);

      if (onUpdateSql.equals("")) {
        onUpdateSql = quotedColumnName + "=EXCLUDED." + quotedColumnName;
      } else {
        onUpdateSql = onUpdateSql + "," + quotedColumnName + "=EXCLUDED." + quotedColumnName;
      }
    }

    return onUpdateSql;
  }

  @Override
  public String getDefaultQuoteCharacter() {
    return "\"";
  }

  @Override
  public String getDeleteDmlStatement() {
    return "DELETE FROM {quoted_schema_name}.{quoted_table_name} WHERE {primary_key_kv_sql};";
  }

  @Override
  public String getUpsertDmlStatement() {
    return "INSERT INTO {quoted_schema_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql}) "
        + "ON CONFLICT ({primary_key_names_sql}) DO UPDATE SET {column_kv_sql};";
  }

  @Override
  public String getInsertDmlStatement() {
    return "INSERT INTO {quoted_schema_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql});";
  }

  @Override
  public String getTargetCatalogName(DatastreamRow row) {
    return "";
  }

  @Override
  public String getTargetSchemaName(DatastreamRow row) {
    String schemaName = row.getSchemaName();
    return cleanSchemaName(schemaName);
  }

  @Override
  public String getTargetTableName(DatastreamRow row) {
    String tableName = row.getTableName();
    return cleanTableName(tableName);
  }

  @Override
  public String cleanDataTypeValueSql(
      String columnValue, String columnName, Map<String, String> tableSchema) {
    String dataType = tableSchema.get(columnName);
    if (dataType == null) {
      return columnValue;
    }
    switch (dataType.toUpperCase()) {
      case "INT2":
      case "INT4":
      case "INT8":
      case "FLOAT4":
      case "FLOAT8":
      case "SMALLINT":
      case "INTEGER":
      case "BIGINT":
      case "DECIMAL":
      case "NUMERIC":
      case "REAL":
      case "DOUBLE PRECISION":
      case "SMALLSERIAL":
      case "SERIAL":
      case "ENUM":
      case "BIGSERIAL":
        if (columnValue.equals("") || columnValue.equals("''")) {
          return getNullValueSql();
        }
        break;
      case "INTERVAL":
        return convertJsonToPostgresInterval(columnValue, columnName);
      case "BYTEA":
        // Byte arrays are converted to base64 string representation.
        return "decode(" + columnValue + ",'base64')";
      case "HSTORE":
        String hstoreLiteral = convertJsonToHstoreLiteral(columnValue);

        if (hstoreLiteral.equals(getNullValueSql())) {
          return hstoreLiteral;
        }
        return hstoreLiteral + "::hstore";
      case "LTREE":
        if (columnValue.equals("")
            || columnValue.equals("''")
            || columnValue.equalsIgnoreCase("'NULL'")
            || columnValue.equalsIgnoreCase("NULL")) {
          return getNullValueSql();
        }
        return columnValue + "::ltree";
    }

    // Arrays in Postgres are prefixed with underscore e.g. _INT4 for integer array.
    if (dataType.startsWith("_")) {
      return convertJsonToPostgresArray(columnValue, dataType.toUpperCase(), columnName);
    }
    return columnValue;
  }

  private String unquote(String value) {
    if (value != null && value.length() > 1 && value.startsWith("'") && value.endsWith("'")) {
      return value.substring(1, value.length() - 1).replace("''", "'");
    }
    return value;
  }

  public String convertJsonToHstoreLiteral(String jsonValue) {
    // 1. Handle null, empty, or literal "null" strings
    if (jsonValue == null || jsonValue.isEmpty() || jsonValue.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      String unquotedJsonValue = unquote(jsonValue);
      JsonNode rootNode = mapper.readTree(unquotedJsonValue);

      // 2. An hstore must be created from a JSON object.
      if (!rootNode.isObject()) {
        LOG.warn("Cannot convert non-object JSON to hstore. Value: {}", jsonValue);
        return getNullValueSql();
      }

      StringBuilder hstoreBuilder = new StringBuilder();
      Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();

      boolean isFirst = true;
      while (fields.hasNext()) {
        if (!isFirst) {
          hstoreBuilder.append(", ");
        }

        Map.Entry<String, JsonNode> entry = fields.next();
        String key = entry.getKey();
        JsonNode valueNode = entry.getValue();

        // 3. Format the key (always double-quoted and escaped)
        hstoreBuilder.append("\"").append(escapeHstoreValue(key)).append("\"");
        hstoreBuilder.append("=>");

        // 4. Format the value (handle SQL NULL or double-quote and escape)
        if (valueNode.isNull()) {
          hstoreBuilder.append("NULL");
        } else {
          hstoreBuilder.append("\"").append(escapeHstoreValue(valueNode.asText())).append("\"");
        }
        isFirst = false;
      }

      // 5. Wrap the final result in single quotes to make it a PG string literal.
      return "'" + hstoreBuilder.toString() + "'";

    } catch (JsonProcessingException e) {
      LOG.error(
          "Error parsing JSON for hstore conversion: {}. Error: {}", jsonValue, e.getMessage());
      return getNullValueSql();
    }
  }

  /** Escapes backslashes and double-quotes within a string for hstore compatibility. */
  private String escapeHstoreValue(String value) {
    if (value == null) {
      return "";
    }
    // Important: The order matters. Escape backslashes first.
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  public String convertJsonToPostgresInterval(String jsonValue, String columnName) {
    if (jsonValue == null || jsonValue.equals("''") || jsonValue.equals("")) {
      return getNullValueSql();
    }

    try {
      String unquotedJsonValue = unquote(jsonValue);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(unquotedJsonValue);

      if (!rootNode.isObject()
          || !rootNode.has("months")
          || !rootNode.has("hours")
          || !rootNode.has("micros")) {
        LOG.warn("Invalid interval format for column {}, value: {}", columnName, jsonValue);
        return getNullValueSql();
      }

      int months = rootNode.get("months").asInt();
      int hours = rootNode.get("hours").asInt();
      double seconds = rootNode.get("micros").asLong() / 1_000_000.0;

      // Build the ISO 8601 string
      String intervalStr = String.format("P%dMT%dH%.6fS", months, hours, seconds);

      return "'" + intervalStr + "'";

    } catch (JsonProcessingException e) {
      LOG.error("Error parsing JSON interval: {}", jsonValue, e);
      return getNullValueSql();
    }
  }

  private String convertJsonToPostgresArray(String jsonValue, String dataType, String columnName) {
    if (jsonValue == null || jsonValue.equals("''") || jsonValue.equals("")) {
      return getNullValueSql();
    }

    try {
      String unquotedJsonValue = unquote(jsonValue);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(unquotedJsonValue);
      if (!(rootNode.isObject() && rootNode.has("nestedArray"))) {
        LOG.warn("Null array for column {}, value {}", columnName, jsonValue);
        return getNullValueSql();
      }

      JsonNode arrayNode = rootNode.get("nestedArray");

      // Handle nested structure with elementValue
      List<String> elements = new ArrayList<>();
      if (arrayNode.isArray()) {
        for (JsonNode element : arrayNode) {
          if (element.has("elementValue")) {
            JsonNode elementValue = element.get("elementValue");
            if (!elementValue.isNull()) {
              elements.add(formatArrayElement(elementValue));
            } else {
              elements.add(getNullValueSql());
            }
          } else if (!element.isNull()) {
            elements.add(formatArrayElement(element));
          }
        }
      }

      if (elements.isEmpty()) {
        // Use array literal for empty arrays otherwise type inferencing fails.
        return "'{}'";
      }
      String arrayStatement = "ARRAY[" + String.join(",", elements) + "]";
      if (dataType.equals("_JSON")) {
        // Cast string array to json array.
        return arrayStatement + "::json[]";
      }
      if (dataType.equals("_JSONB")) {
        // Cast string array to jsonb array.
        return arrayStatement + "::jsonb[]";
      }
      if (dataType.equals("_UUID")) {
        // Cast string array to uuid array.
        return arrayStatement + "::uuid[]";
      }

      return arrayStatement;

    } catch (JsonProcessingException e) {
      LOG.error("Error parsing JSON array: {}", jsonValue);
      return getNullValueSql();
    }
  }

  private String formatArrayElement(JsonNode element) {
    if (element.isTextual()) {
      return "\'" + cleanSql(element.textValue()) + "\'";
    }
    return element.toString();
  }
}
