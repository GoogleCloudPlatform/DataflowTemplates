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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatastreamToMySQLDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToMySQLDML.class);
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .toFormatter(Locale.ROOT);

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
    String dataType = tableSchema.get(columnName);
    if (!("TIMESTAMP".equalsIgnoreCase(dataType) || "DATETIME".equalsIgnoreCase(dataType))
        || columnValue.length() < 2
        || !columnValue.startsWith("'")
        || !columnValue.endsWith("'")) {
      return columnValue;
    }
    try {
      OffsetDateTime timestamp =
          OffsetDateTime.parse(columnValue.substring(1, columnValue.length() - 1));
      return "'"
          + TIMESTAMP_FORMATTER.format(timestamp.withOffsetSameInstant(ZoneOffset.UTC))
          + "'";
    } catch (DateTimeParseException e) {
      // Leave local timestamp literals and invalid values to the existing sink handling.
      return columnValue;
    }
  }
}
