/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.teleport.v2.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.values.DatastreamRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of Database Migration utilities to convert JSON data to DML.
 */
public class DatastreamToPostgresDML extends DatastreamToDML {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToPostgresDML.class);


  private DatastreamToPostgresDML(DataSourceConfiguration config) {
    super(config);
  }

  public static DatastreamToPostgresDML of(DataSourceConfiguration config) {
    return new DatastreamToPostgresDML(config);
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
    return schemaName.toLowerCase();
  }

  @Override
  public String getTargetTableName(DatastreamRow row) {
    String tableName = row.getTableName();
    return tableName.toLowerCase();
  }
}
