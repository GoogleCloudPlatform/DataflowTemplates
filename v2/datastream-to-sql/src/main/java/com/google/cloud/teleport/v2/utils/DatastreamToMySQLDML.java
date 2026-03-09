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
import java.util.ArrayList;
import java.util.List;
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
  public String getCreateTableSql(
      String catalogName,
      String schemaName,
      String tableName,
      List<String> primaryKeys,
      Map<String, String> sourceSchema) {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE IF NOT EXISTS ")
        .append(quote(catalogName))
        .append(".")
        .append(quote(tableName))
        .append(" (");

    List<String> columns = new ArrayList<>();
    for (Map.Entry<String, String> entry : sourceSchema.entrySet()) {
      columns.add(quote(applyCasingLogic(entry.getKey(), this.columnCasing)) + " " + entry.getValue());
    }
    sql.append(String.join(", ", columns));

    if (!primaryKeys.isEmpty()) {
      sql.append(", PRIMARY KEY (");
      List<String> casedPks = new ArrayList<>();
      for (String pk : primaryKeys) {
        casedPks.add(quote(applyCasingLogic(pk, this.columnCasing)));
      }
      sql.append(String.join(", ", casedPks));
      sql.append(")");
    }

    sql.append(");");
    return sql.toString();
  }

  @Override
  public String getAddColumnSql(
      String catalogName, String schemaName, String tableName, String columnName, String columnType) {
    return String.format(
        "ALTER TABLE %s.%s ADD COLUMN %s %s;",
        quote(catalogName), quote(tableName), quote(columnName), columnType);
  }

  @Override
  public String getDestinationType(String sourceType, Long precision, Long scale) {
    switch (sourceType.toUpperCase()) {
      case "BOOL":
      case "BOOLEAN":
        return "BOOLEAN";
      case "INT64":
      case "INTEGER":
      case "BIGINT":
        return "BIGINT";
      case "FLOAT64":
      case "DOUBLE":
        return "DOUBLE";
      case "NUMERIC":
      case "BIGNUMERIC":
      case "DECIMAL":
        return "DECIMAL";
      case "BYTES":
      case "BLOB":
        return "BLOB";
      case "DATETIME":
        return "DATETIME";
      case "TIMESTAMP":
        return "TIMESTAMP";
      case "DATE":
        return "DATE";
      case "TIME":
        return "TIME";
      case "STRING":
      case "TEXT":
      case "VARCHAR":
      default:
        return "TEXT";
    }
  }
}
