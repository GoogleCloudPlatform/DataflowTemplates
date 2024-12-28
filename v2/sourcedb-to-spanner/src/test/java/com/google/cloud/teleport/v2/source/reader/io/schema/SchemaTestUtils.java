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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;

/** Test Utility class for Generating test schema. */
public class SchemaTestUtils {

  static final String TEST_FIELD_NAME_1 = "firstName";
  static final String TEST_FIELD_NAME_2 = "lastName";

  public static SourceSchemaReference generateSchemaReference(String namespace, String dbName) {
    return SourceSchemaReference.ofJdbc(
        JdbcSchemaReference.builder().setNamespace(namespace).setDbName(dbName).build());
  }

  public static SourceTableSchema generateTestTableSchema(String tableName) {
    return SourceTableSchema.builder(SQLDialect.MYSQL)
        .setTableName(tableName)
        .addSourceColumnNameToSourceColumnType(
            TEST_FIELD_NAME_1, new SourceColumnType("varchar", new Long[] {20L}, null))
        .addSourceColumnNameToSourceColumnType(
            TEST_FIELD_NAME_2, new SourceColumnType("varchar", new Long[] {20L}, null))
        .build();
  }
}
