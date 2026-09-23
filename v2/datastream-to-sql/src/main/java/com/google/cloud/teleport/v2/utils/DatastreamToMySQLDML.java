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
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatastreamToMySQLDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToMySQLDML.class);

  private static final Pattern ISO_DATETIME_PATTERN =
      Pattern.compile(
          "^(\\d{4}-\\d{2}-\\d{2})[Tt ](\\d{2}:\\d{2}:\\d{2})(?:\\.(\\d+))?"
              + "(?:([Zz]|[+-]\\d{2}:\\d{2}|[+-]\\d{4}|[+-]\\d{2}))?$");

  private static final Pattern ISO_TIME_PATTERN =
      Pattern.compile(
          "^([+-]?\\d{2,3}:\\d{2}:\\d{2})(?:\\.(\\d+))?"
              + "(?:([Zz]|[+-]\\d{2}:\\d{2}|[+-]\\d{4}|[+-]\\d{2}))?$");

  private static final Pattern YEAR_PREFIX_PATTERN = Pattern.compile("^\\d{4}-.*");

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
    if (tableSchema == null || columnName == null) {
      return columnValue;
    }
    String dataType = tableSchema.get(columnName);
    if (dataType == null) {
      return columnValue;
    }

    if (columnValue == null || columnValue.isEmpty() || columnValue.equalsIgnoreCase("NULL")) {
      return getNullValueSql();
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
      case "BOOLEAN":
      case "BOOL":
        if (columnValue.equals("''")) {
          return getNullValueSql();
        }
        return columnValue;

      case "DATETIME":
      case "TIMESTAMP":
        return cleanDateTimeOrTimestampValue(columnValue);

      case "DATE":
        return cleanDateValue(columnValue);

      case "TIME":
        return cleanTimeValue(columnValue);

      case "YEAR":
        return cleanYearValue(columnValue);

      case "JSON":
        if (columnValue.equals("''")) {
          return getNullValueSql();
        }
        return "'" + cleanSql(unquote(columnValue)) + "'";

      default:
        return columnValue;
    }
  }

  private String normalizeDataType(String dataType) {
    String normalized = dataType.trim().toUpperCase();
    int parenIndex = normalized.indexOf('(');
    if (parenIndex > 0) {
      normalized = normalized.substring(0, parenIndex).trim();
    }
    if (normalized.endsWith(" ZEROFILL")) {
      normalized = normalized.substring(0, normalized.length() - " ZEROFILL".length()).trim();
    }
    if (normalized.endsWith(" UNSIGNED")) {
      normalized = normalized.substring(0, normalized.length() - " UNSIGNED".length()).trim();
    }
    return normalized;
  }

  private String cleanDateTimeOrTimestampValue(String columnValue) {
    String raw = unquote(columnValue);
    if (raw == null || raw.trim().isEmpty() || raw.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    raw = raw.trim();

    Matcher matcher = ISO_DATETIME_PATTERN.matcher(raw);
    if (!matcher.matches()) {
      return columnValue;
    }

    String datePart = matcher.group(1);
    String timePart = matcher.group(2);
    String fracPart = matcher.group(3);
    String offsetPart = matcher.group(4);

    String truncatedFrac =
        (fracPart != null && fracPart.length() > 6) ? fracPart.substring(0, 6) : fracPart;

    if (offsetPart != null && !isZeroOffset(offsetPart)) {
      try {
        LocalDate localDate = LocalDate.parse(datePart);
        LocalTime localTime = LocalTime.parse(timePart);
        int nanos =
            truncatedFrac != null
                ? Integer.parseInt((truncatedFrac + "000000000").substring(0, 9))
                : 0;
        ZoneOffset offset = ZoneOffset.of(offsetPart);
        OffsetDateTime utcDateTime =
            OffsetDateTime.of(localDate, localTime.withNano(nanos), offset)
                .withOffsetSameInstant(ZoneOffset.UTC);
        datePart = utcDateTime.toLocalDate().toString();
        timePart =
            String.format(
                "%02d:%02d:%02d",
                utcDateTime.getHour(), utcDateTime.getMinute(), utcDateTime.getSecond());
      } catch (DateTimeException e) {
        LOG.warn("Failed to convert offset datetime '{}' to UTC", raw, e);
        return columnValue;
      }
    }

    String formatted =
        truncatedFrac != null
            ? datePart + " " + timePart + "." + truncatedFrac
            : datePart + " " + timePart;
    return "'" + cleanSql(formatted) + "'";
  }

  private String cleanDateValue(String columnValue) {
    String raw = unquote(columnValue);
    if (raw == null || raw.trim().isEmpty() || raw.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    raw = raw.trim();

    Matcher matcher = ISO_DATETIME_PATTERN.matcher(raw);
    if (matcher.matches()) {
      return "'" + cleanSql(matcher.group(1)) + "'";
    }
    return columnValue;
  }

  private String cleanTimeValue(String columnValue) {
    String raw = unquote(columnValue);
    if (raw == null || raw.trim().isEmpty() || raw.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    raw = raw.trim();

    String upperRaw = raw.toUpperCase();
    if (upperRaw.startsWith("PT") || upperRaw.startsWith("-PT") || upperRaw.startsWith("+PT")) {
      try {
        Duration duration = Duration.parse(raw);
        boolean negative = duration.isNegative();
        duration = duration.abs();
        long hours = duration.toHours();
        int minutes = duration.toMinutesPart();
        int seconds = duration.toSecondsPart();
        int micros = duration.toNanosPart() / 1_000;
        String sign = negative ? "-" : "";
        if (micros > 0) {
          return String.format("'%s%02d:%02d:%02d.%06d'", sign, hours, minutes, seconds, micros);
        }
        return String.format("'%s%02d:%02d:%02d'", sign, hours, minutes, seconds);
      } catch (DateTimeParseException e) {
        return columnValue;
      }
    }

    Matcher dtMatcher = ISO_DATETIME_PATTERN.matcher(raw);
    if (dtMatcher.matches()) {
      String timePart = dtMatcher.group(2);
      String fracPart = dtMatcher.group(3);
      String truncatedFrac =
          (fracPart != null && fracPart.length() > 6) ? fracPart.substring(0, 6) : fracPart;
      String formatted = truncatedFrac != null ? timePart + "." + truncatedFrac : timePart;
      return "'" + cleanSql(formatted) + "'";
    }

    Matcher timeMatcher = ISO_TIME_PATTERN.matcher(raw);
    if (timeMatcher.matches()) {
      String timePart = timeMatcher.group(1);
      String fracPart = timeMatcher.group(2);
      String truncatedFrac =
          (fracPart != null && fracPart.length() > 6) ? fracPart.substring(0, 6) : fracPart;
      String formatted = truncatedFrac != null ? timePart + "." + truncatedFrac : timePart;
      return "'" + cleanSql(formatted) + "'";
    }

    return columnValue;
  }

  private String cleanYearValue(String columnValue) {
    String raw = unquote(columnValue);
    if (raw == null || raw.trim().isEmpty() || raw.equalsIgnoreCase("null")) {
      return getNullValueSql();
    }
    raw = raw.trim();

    if (YEAR_PREFIX_PATTERN.matcher(raw).matches()) {
      return "'" + cleanSql(raw.substring(0, 4)) + "'";
    }
    return columnValue;
  }

  private boolean isZeroOffset(String offsetPart) {
    return offsetPart.equalsIgnoreCase("Z")
        || offsetPart.equals("+00:00")
        || offsetPart.equals("-00:00")
        || offsetPart.equals("+0000")
        || offsetPart.equals("-0000")
        || offsetPart.equals("+00")
        || offsetPart.equals("-00");
  }

  private String unquote(String value) {
    if (value != null && value.length() > 1 && value.startsWith("'") && value.endsWith("'")) {
      return value.substring(1, value.length() - 1).replace("''", "'");
    }
    return value;
  }
}
