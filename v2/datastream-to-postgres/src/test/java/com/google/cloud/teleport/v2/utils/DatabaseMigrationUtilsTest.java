/*
 * Copyright (C) 2019 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link DatabaseMigrationUtils} class. */
public class DatabaseMigrationUtilsTest {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseMigrationUtilsTest.class);
  private String jsonString =
      "{"
          + "\"text_column\":\"value\","
          + "\"quoted_text_column\":\"Test Values: '!@#$%^\","
          + "\"null_byte_text_column\":\"Test Values: He\\u0000s made\","
          + "\"_metadata_schema\":\"MY_SCHEMA\","
          + "\"_metadata_table\":\"MY_TABLE$NAME\""
          + "}";

  private JsonNode getRowObj() {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rowObj;
    try {
      rowObj = mapper.readTree(jsonString);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return rowObj;
  }

  /**
   * Test whether {@link DatabaseMigrationUtils#getValueSql(JsonNode, String, Map)} converts data
   * into correct strings. String columnValue = getValueSql(rowObj, columnName, tableSchema);
   */
  @Test
  public void testGetValueSql() {
    JsonNode rowObj = this.getRowObj();

    String expectedTextContent = "'value'";
    String testSqlContent = DatabaseMigrationUtils.getValueSql(rowObj, "text_column", null);
    assertEquals(expectedTextContent, testSqlContent);

    // Single quotes are escaped by 2 single quotes in SQL
    String expectedQuotedTextContent = "'Test Values: ''!@#$%^'";
    String testQuotedSqlContent =
        DatabaseMigrationUtils.getValueSql(rowObj, "quoted_text_column", null);
    assertEquals(expectedQuotedTextContent, testQuotedSqlContent);

    // Null bytes are escaped with blanks values
    String expectedNullByteTextContent = "'Test Values: Hes made'";
    String testNullByteSqlContent =
        DatabaseMigrationUtils.getValueSql(rowObj, "null_byte_text_column", null);
    assertEquals(expectedNullByteTextContent, testNullByteSqlContent);
  }

  /**
   * Test whether {@link DatabaseMigrationUtils#getPostgresSchemaName(JsonNode)} converts the Oracle
   * schema into the correct Postgres schema.
   */
  @Test
  public void testGetPostgresSchemaName() {
    JsonNode rowObj = this.getRowObj();

    String expectedSchemaName = "my_schema";
    String schemaName = DatabaseMigrationUtils.getPostgresSchemaName(rowObj);
    assertEquals(schemaName, expectedSchemaName);
  }

  /**
   * Test whether {@link DatabaseMigrationUtils#getPostgresTableName(JsonNode)} converts the Oracle
   * table into the correct Postgres table.
   */
  @Test
  public void testGetPostgresTableName() {
    JsonNode rowObj = this.getRowObj();

    String expectedTableName = "my_table$name";
    String tableName = DatabaseMigrationUtils.getPostgresTableName(rowObj);
    assertEquals(expectedTableName, tableName);
  }
}
