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

  // In DatastreamToMySQLDML.java...

  private String getFullSourceTableName(DatastreamRow row) {
    return row.getSchemaName() + "." + row.getTableName();
  }

  @Override
  public String getTargetCatalogName(DatastreamRow row) {
    String fullSourceTableName = getFullSourceTableName(row);
    // 1. Check for a specific table mapping first.
    if (tableMappings.containsKey(fullSourceTableName)) {
      // If found, the new schema is the catalog for MySQL.
      return tableMappings.get(fullSourceTableName).split("\\.")[0];
    }
    // 2. Fall back to the general schema mapping.
    return schemaMappings.getOrDefault(row.getSchemaName(), row.getSchemaName());
  }

  @Override
  public String getTargetSchemaName(DatastreamRow row) {
    // MySQL does not use a separate schema concept in the same way as Postgres.
    // The catalog is the database/schema, so this can return an empty string.
    return "";
  }

  @Override
  public String getTargetTableName(DatastreamRow row) {
    String fullSourceTableName = getFullSourceTableName(row);
    // 1. Check for a specific table mapping first.
    if (tableMappings.containsKey(fullSourceTableName)) {
      // If found, parse the new table name from the value.
      return tableMappings.get(fullSourceTableName).split("\\.")[1];
    }
    // 2. If no table mapping is found, fall back to the original table name.
    return row.getTableName();
  }
}
