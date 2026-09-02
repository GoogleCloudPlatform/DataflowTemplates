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

import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatastreamToMySQLDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToMySQLDML.class);

  private DatastreamToMySQLDML(DataSourceConfiguration config) {
    super(config);
  }

  public static DatastreamToMySQLDML of(DataSourceConfiguration config) {
    return new DatastreamToMySQLDML(config);
  }

  @Override
  public String getDefaultQuoteCharacter() {
    return "`";
  }

  @Override
  public String getDeleteDmlStatement() {
    return "DELETE FROM {quoted_catalog_name}.{quoted_table_name} WHERE {primary_key_kv_sql};";
  }

  @Override
  public String getUpsertDmlStatement() {
    return "INSERT INTO {quoted_catalog_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql}) "
        + "ON DUPLICATE KEY UPDATE {column_kv_sql};";
  }

  @Override
  public String getInsertDmlStatement() {
    return "INSERT INTO {quoted_catalog_name}.{quoted_table_name} "
        + "({quoted_column_names}) VALUES ({column_value_sql});";
  }

  @Override
  public String getTargetCatalogName(DatastreamRow row) {
    String fullSourceTableName = getFullSourceTableName(row);
    if (tableMappings.containsKey(fullSourceTableName)) {
      return tableMappings.get(fullSourceTableName).split("\\.")[0];
    }
    return schemaMappings.getOrDefault(row.getSchemaName(), applyCasing(row.getSchemaName()));
  }

  @Override
  public String getTargetSchemaName(DatastreamRow row) {
    return "";
  }

  @Override
  public String cleanDataTypeValueSql(
      String columnValue, String columnName, Map<String, String> tableSchema) {
    String dataType = getDataType(columnName, tableSchema);

    if (columnValue == null
        || columnValue.isEmpty()
        || columnValue.equals("''")
        || columnValue.equalsIgnoreCase("NULL")
        || columnValue.equalsIgnoreCase("'NULL'")) {
      if (dataType != null
          && isStringType(dataType)
          && columnValue != null
          && columnValue.equals("''")) {
        return "''";
      }
      return getNullValueSql();
    }

    if (dataType == null) {
      return columnValue;
    }

    String normalizedType = normalizeDataType(dataType);

    switch (normalizedType) {
      case "TINYINT":
      case "SMALLINT":
      case "MEDIUMINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
      case "DECIMAL":
      case "DEC":
      case "NUMERIC":
      case "FLOAT":
      case "DOUBLE":
      case "DOUBLE PRECISION":
      case "REAL":
      case "BIT":
        return cleanNumericValue(columnValue);

      case "DATE":
      case "DATETIME":
      case "TIMESTAMP":
      case "TIME":
      case "YEAR":
        return cleanDateTimeValue(columnValue, normalizedType);

      case "JSON":
        return cleanJsonValue(columnValue);

      case "BOOLEAN":
      case "BOOL":
        return cleanBooleanValue(columnValue);

      case "GEOMETRY":
      case "POINT":
      case "LINESTRING":
      case "POLYGON":
      case "MULTIPOINT":
      case "MULTILINESTRING":
      case "MULTIPOLYGON":
      case "GEOMETRYCOLLECTION":
        return cleanSpatialValue(columnValue);

      default:
        return columnValue;
    }
  }

  private String getDataType(String columnName, Map<String, String> tableSchema) {
    if (tableSchema == null || tableSchema.isEmpty() || columnName == null) {
      return null;
    }
    String matchedColumn = getMatchingTableColumn(columnName, tableSchema);
    if (matchedColumn != null && tableSchema.containsKey(matchedColumn)) {
      return tableSchema.get(matchedColumn);
    }
    if (tableSchema.containsKey(columnName)) {
      return tableSchema.get(columnName);
    }
    for (Map.Entry<String, String> entry : tableSchema.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(columnName)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private String normalizeDataType(String dataType) {
    if (dataType == null) {
      return "";
    }
    String normalized = dataType.trim().toUpperCase();
    int parenIndex = normalized.indexOf('(');
    if (parenIndex > 0) {
      normalized = normalized.substring(0, parenIndex).trim();
    }
    if (normalized.endsWith(" UNSIGNED")) {
      normalized = normalized.substring(0, normalized.length() - " UNSIGNED".length()).trim();
    }
    if (normalized.endsWith(" ZEROFILL")) {
      normalized = normalized.substring(0, normalized.length() - " ZEROFILL".length()).trim();
    }
    return normalized;
  }

  private boolean isStringType(String dataType) {
    String normalized = normalizeDataType(dataType);
    switch (normalized) {
      case "CHAR":
      case "VARCHAR":
      case "TINYTEXT":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
      case "ENUM":
      case "SET":
        return true;
      default:
        return false;
    }
  }

  private String cleanNumericValue(String columnValue) {
    String unquoted = unquote(columnValue);
    if (unquoted == null || unquoted.trim().isEmpty() || unquoted.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    return unquoted.trim();
  }

  private String cleanDateTimeValue(String columnValue, String normalizedType) {
    String raw = unquote(columnValue);
    if (raw == null || raw.trim().isEmpty() || raw.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    raw = raw.trim();

    if (normalizedType.equals("DATE")) {
      if (raw.length() >= 10 && raw.charAt(4) == '-' && raw.charAt(7) == '-') {
        return "'" + cleanSql(raw.substring(0, 10)) + "'";
      }
      return "'" + cleanSql(raw) + "'";
    }

    if (normalizedType.equals("YEAR")) {
      if (raw.length() >= 4) {
        return "'" + cleanSql(raw.substring(0, 4)) + "'";
      }
      return "'" + cleanSql(raw) + "'";
    }

    if (normalizedType.equals("TIME")) {
      if (raw.contains("T")) {
        raw = raw.substring(raw.indexOf('T') + 1);
      } else if (raw.contains(" ")) {
        raw = raw.substring(raw.indexOf(' ') + 1);
      }
      if (raw.endsWith("Z") || raw.endsWith("z")) {
        raw = raw.substring(0, raw.length() - 1);
      }
      raw = raw.replaceAll("([+-]\\d{2}:?\\d{2}|[+-]\\d{2})$", "").trim();
      raw = raw.replaceAll("(\\.\\d{6})\\d+", "$1");
      return "'" + cleanSql(raw) + "'";
    }

    // DATETIME and TIMESTAMP
    if (raw.endsWith("Z") || raw.endsWith("z")) {
      raw = raw.substring(0, raw.length() - 1);
    }
    raw = raw.replace('T', ' ');
    raw = raw.replaceAll("([+-]\\d{2}:?\\d{2}|[+-]\\d{2})$", "").trim();
    raw = raw.replaceAll("(\\.\\d{6})\\d+", "$1");

    return "'" + cleanSql(raw) + "'";
  }

  private String cleanJsonValue(String columnValue) {
    String unquoted = unquote(columnValue);
    if (unquoted == null || unquoted.trim().isEmpty() || unquoted.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    return "'" + cleanSql(unquoted) + "'";
  }

  private String cleanBooleanValue(String columnValue) {
    String unquoted = unquote(columnValue);
    if (unquoted == null || unquoted.trim().isEmpty() || unquoted.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    if (unquoted.equalsIgnoreCase("true") || unquoted.equals("1")) {
      return "1";
    }
    if (unquoted.equalsIgnoreCase("false") || unquoted.equals("0")) {
      return "0";
    }
    return unquoted;
  }

  private String cleanSpatialValue(String columnValue) {
    String unquoted = unquote(columnValue);
    if (unquoted == null || unquoted.trim().isEmpty() || unquoted.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    return columnValue;
  }
}
