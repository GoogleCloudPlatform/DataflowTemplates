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

import com.google.cloud.teleport.v2.values.DatastreamRow;
import java.io.IOException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link DatastreamToDML} class. */
public class DatastreamToDMLTest {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToDMLTest.class);
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
   * Test whether {@link DatastreamToPostgresDML#getValueSql(rowObj, columnName, tableSchema)}
   * converts data into correct strings. String columnValue = getValueSql(rowObj, columnName,
   * tableSchema);
   */
  @Test
  public void testGetValueSql() {
    JsonNode rowObj = this.getRowObj();

    String expectedTextContent = "'value'";
    String testSqlContent = DatastreamToPostgresDML.getValueSql(rowObj, "text_column", null);
    assertEquals(expectedTextContent, testSqlContent);

    // Single quotes are escaped by 2 single quotes in SQL
    String expectedQuotedTextContent = "'Test Values: ''!@#$%^'";
    String testQuotedSqlContent =
        DatastreamToPostgresDML.getValueSql(rowObj, "quoted_text_column", null);
    assertEquals(expectedQuotedTextContent, testQuotedSqlContent);

    // Null bytes are escaped with blanks values
    String expectedNullByteTextContent = "'Test Values: Hes made'";
    String testNullByteSqlContent =
        DatastreamToPostgresDML.getValueSql(rowObj, "null_byte_text_column", null);
    assertEquals(expectedNullByteTextContent, testNullByteSqlContent);
  }

  /**
   * Test whether {@link DatastreamToDML#getTargetSchemaName(row)} converts the Oracle schema into
   * the correct Postgres schema.
   */
  @Test
  public void testGetPostgresSchemaName() {
    DatastreamToDML datastreamToDML = DatastreamToPostgresDML.of(null);
    JsonNode rowObj = this.getRowObj();
    DatastreamRow row = DatastreamRow.of(rowObj);

    String expectedSchemaName = "my_schema";
    String schemaName = datastreamToDML.getTargetSchemaName(row);
    assertEquals(schemaName, expectedSchemaName);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getTargetTableName(row)} converts the Oracle table
   * into the correct Postgres table.
   */
  @Test
  public void testGetPostgresTableName() {
    DatastreamToDML datastreamToDML = DatastreamToPostgresDML.of(null);
    JsonNode rowObj = this.getRowObj();
    DatastreamRow row = DatastreamRow.of(rowObj);

    String expectedTableName = "my_table$name";
    String tableName = datastreamToDML.getTargetTableName(row);
    assertEquals(expectedTableName, tableName);
  }
}
