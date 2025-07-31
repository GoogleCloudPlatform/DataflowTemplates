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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test cases for the {@link DatastreamToDML} class. */
public class DatastreamToDMLTest {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToDMLTest.class);
  private static final String JSON_STRING =
      "{"
          + "\"text_column\":\"value\","
          + "\"quoted_text_column\":\"Test Values: '!@#$%^\","
          + "\"null_byte_text_column\":\"Test Values: He\\u0000s made\","
          + "\"_metadata_schema\":\"MY_SCHEMA\","
          + "\"_metadata_table\":\"MY_TABLE$NAME\""
          + "}";

  private JsonNode getRowObj(String jsonString) {
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
   * Mocks the JDBC metadata calls to simulate finding a table with a specific casing.
   *
   * @param dataSourceMock The mock DataSource to configure.
   * @param expectedSchema The correctly cased schema name to "find" in the database.
   * @param expectedTable The correctly cased table name to "find" in the database.
   * @throws SQLException
   */
  // Inside DatastreamToDMLTest class

  private void setupMockJdbc(DataSource dataSourceMock, String expectedSchema, String expectedTable)
      throws SQLException {
    ResultSet tablesResultSetMock = mock(ResultSet.class);
    when(tablesResultSetMock.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSetMock.getString("TABLE_SCHEM")).thenReturn(expectedSchema);
    when(tablesResultSetMock.getString("TABLE_NAME")).thenReturn(expectedTable);

    ResultSet columnsResultSetMock = mock(ResultSet.class);
    when(columnsResultSetMock.next()).thenReturn(true).thenReturn(false);
    when(columnsResultSetMock.getString("COLUMN_NAME")).thenReturn("id");
    when(columnsResultSetMock.getString("TYPE_NAME")).thenReturn("INTEGER");

    ResultSet primaryKeysResultSetMock = mock(ResultSet.class);
    when(primaryKeysResultSetMock.next()).thenReturn(false);

    DatabaseMetaData metadataMock = mock(DatabaseMetaData.class);

    // FIX: The mock needs to handle the lowercase names from the source JSON.
    when(metadataMock.getTables(any(), eq("myschema"), eq("mytable"), any(String[].class)))
        .thenReturn(tablesResultSetMock);

    // The mock for getColumns() needs to handle the canonical names.
    when(metadataMock.getColumns(any(), eq(expectedSchema), eq(expectedTable), any()))
        .thenReturn(columnsResultSetMock);

    // The mock for getPrimaryKeys() also needs to handle the canonical names.
    when(metadataMock.getPrimaryKeys(any(), eq(expectedSchema), eq(expectedTable)))
        .thenReturn(primaryKeysResultSetMock);

    Connection connectionMock = mock(Connection.class);
    when(connectionMock.getMetaData()).thenReturn(metadataMock);
    when(dataSourceMock.getConnection()).thenReturn(connectionMock);
  }

  /**
   * Tests the end-to-end case-insensitive lookup. Simulates a lowercase source table name and an
   * uppercase target table name.
   */
  @Test
  public void testConvertJsonToDmlInfo_handlesCaseInsensitiveNames() throws SQLException {
    // Arrange: Source event has lowercase names.
    String json =
        "{\"id\":1,"
            + "\"_metadata_schema\":\"myschema\","
            + "\"_metadata_table\":\"mytable\"," // lowercase in source
            + "\"_metadata_deleted\":false,"
            + "\"_metadata_timestamp\":1672531200,"
            + "\"_metadata_source_type\":\"mysql\""
            + "}";
    JsonNode rowObj = getRowObj(json);

    // Arrange: Mock the database to contain uppercase names.
    DataSource dataSourceMock = mock(DataSource.class);
    // Configure the mock to "find" the table, but with a different casing.
    String canonicalSchema = "MySchema";
    String canonicalTable = "MyTable";
    setupMockJdbc(dataSourceMock, canonicalSchema, canonicalTable);

    CdcJdbcIO.DataSourceConfiguration mockConfig = mock(CdcJdbcIO.DataSourceConfiguration.class);
    when(mockConfig.buildDatasource()).thenReturn(dataSourceMock);

    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(mockConfig);

    // Act: Call the main method. The input table name is lowercase.
    DmlInfo dmlInfo = dmlBuilder.convertJsonToDmlInfo(rowObj, json);

    // Assert: Check the DmlInfo object for the correctly cased names from the database.
    assertNotNull(dmlInfo);
    assertEquals(canonicalSchema, dmlInfo.getSchemaName());
    assertEquals(canonicalTable, dmlInfo.getTableName());
    // The DML statement should also contain "MyTable"
    assertThat(dmlInfo.getDmlSql()).contains(canonicalTable);
  }

  /**
   * Test whether {@link DatastreamToDML#getTargetSchemaName} preserves the Oracle schema casing
   * when no match is found in the target DB.
   */
  @Test
  public void testGetPostgresSchemaName() throws SQLException {
    // ARRANGE: Create a mock configuration that finds no matching tables.
    DataSource dataSourceMock = mock(DataSource.class);
    ResultSet emptyResultSet = mock(ResultSet.class);
    when(emptyResultSet.next()).thenReturn(false);
    DatabaseMetaData metadataMock = mock(DatabaseMetaData.class);
    when(metadataMock.getTables(any(), any(), anyString(), any(String[].class)))
        .thenReturn(emptyResultSet);
    Connection connectionMock = mock(Connection.class);
    when(connectionMock.getMetaData()).thenReturn(metadataMock);
    when(dataSourceMock.getConnection()).thenReturn(connectionMock);
    CdcJdbcIO.DataSourceConfiguration mockConfig = mock(CdcJdbcIO.DataSourceConfiguration.class);
    when(mockConfig.buildDatasource()).thenReturn(dataSourceMock);

    DatastreamToDML datastreamToDML = DatastreamToPostgresDML.of(mockConfig);
    JsonNode rowObj = this.getRowObj(JSON_STRING);
    DatastreamRow row = DatastreamRow.of(rowObj);

    // ACT
    String schemaName = datastreamToDML.getTargetSchemaName(row);

    // ASSERT: Should preserve the original casing from the JSON string as a fallback.
    assertEquals("MY_SCHEMA", schemaName);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getTargetTableName} preserves the Oracle table
   * casing when no match is found in the target DB.
   */
  @Test
  public void testGetPostgresTableName() throws SQLException {
    // ARRANGE: Create a mock configuration that finds no matching tables.
    DataSource dataSourceMock = mock(DataSource.class);
    ResultSet emptyResultSet = mock(ResultSet.class);
    when(emptyResultSet.next()).thenReturn(false);
    DatabaseMetaData metadataMock = mock(DatabaseMetaData.class);
    when(metadataMock.getTables(any(), any(), anyString(), any(String[].class)))
        .thenReturn(emptyResultSet);
    Connection connectionMock = mock(Connection.class);
    when(connectionMock.getMetaData()).thenReturn(metadataMock);
    when(dataSourceMock.getConnection()).thenReturn(connectionMock);
    CdcJdbcIO.DataSourceConfiguration mockConfig = mock(CdcJdbcIO.DataSourceConfiguration.class);
    when(mockConfig.buildDatasource()).thenReturn(dataSourceMock);

    DatastreamToDML datastreamToDML = DatastreamToPostgresDML.of(mockConfig);
    JsonNode rowObj = this.getRowObj(JSON_STRING);
    DatastreamRow row = DatastreamRow.of(rowObj);

    // ACT
    String tableName = datastreamToDML.getTargetTableName(row);

    // ASSERT: Should preserve the original casing from the JSON string as a fallback.
    assertEquals("MY_TABLE$NAME", tableName);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts data
   * into correct strings. String columnValue = getValueSql(rowObj, columnName, tableSchema);
   */
  @Test
  public void testGetValueSql() {
    JsonNode rowObj = this.getRowObj(JSON_STRING);

    String expectedTextContent = "'value'";
    String testSqlContent =
        DatastreamToPostgresDML.of(null)
            .getValueSql(rowObj, "text_column", new HashMap<String, String>());
    assertEquals(expectedTextContent, testSqlContent);

    // Single quotes are escaped by 2 single quotes in SQL
    String expectedQuotedTextContent = "'Test Values: ''!@#$%^'";
    String testQuotedSqlContent =
        DatastreamToPostgresDML.of(null)
            .getValueSql(rowObj, "quoted_text_column", new HashMap<String, String>());
    assertEquals(expectedQuotedTextContent, testQuotedSqlContent);

    // Null bytes are escaped with blanks values
    String expectedNullByteTextContent = "'Test Values: Hes made'";
    String testNullByteSqlContent =
        DatastreamToPostgresDML.of(null)
            .getValueSql(rowObj, "null_byte_text_column", new HashMap<String, String>());
    assertEquals(expectedNullByteTextContent, testNullByteSqlContent);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts array
   * data into correct integer array syntax.
   */
  @Test
  public void testIntArrayWithNullTypeCoercion() {
    String arrayJson =
        "{\"number_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": null},"
            + "  {\"nestedArray\": null, \"elementValue\": 456}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = this.getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("number_array", "_int4");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expectedInt = "ARRAY[NULL,456]";

    String actualInt =
        DatastreamToPostgresDML.of(null).getValueSql(rowObj, "number_array", tableSchema);

    assertEquals(expectedInt, actualInt);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#convertJsonToHstoreLiteral(String)} correctly
   * escapes special characters.
   */
  @Test
  public void testHstoreConversion_shouldHandleSpecialCharacters() {
    // Arrange
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String inputJsonWithQuotes = "{\"key\\\"\":\"val'ue\"}";
    String expectedOutput = "'\"key\\\"\"=>\"val'ue\"'";

    // Act
    String actualOutput = dmlBuilder.convertJsonToHstoreLiteral(inputJsonWithQuotes);

    // Assert
    assertEquals(expectedOutput, actualOutput);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} correctly
   * handles null and empty JSON for HSTORE types.
   */
  @Test
  public void testHstoreTypeCoercion_handlesNullAndEmpty() {
    // Test null value
    String nullJson = "{\"product_details\":null}";
    JsonNode nullRowObj = getRowObj(nullJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("product_details", "HSTORE");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);

    String actualNull = dmlBuilder.getValueSql(nullRowObj, "product_details", tableSchema);
    assertEquals("NULL", actualNull);

    // Test empty JSON object
    String emptyJson = "{\"product_details\":\"{}\"}";
    JsonNode emptyRowObj = getRowObj(emptyJson);
    String expectedEmpty = "''::hstore";

    String actualEmpty = dmlBuilder.getValueSql(emptyRowObj, "product_details", tableSchema);
    assertEquals(expectedEmpty, actualEmpty);
  }

  /** Test hstore conversion with multiple key-value pairs and a null value. */
  @Test
  public void testHstoreConversion_withMultipleAndNullValues() {
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String json = "{\"key1\":\"value1\",\"key2\":null,\"key3\":\"value3\"}";
    String expected = "'\"key1\"=>\"value1\", \"key2\"=>NULL, \"key3\"=>\"value3\"'";

    String actual = dmlBuilder.convertJsonToHstoreLiteral(json);

    assertEquals(expected, actual);
  }

  /** Test that hstore conversion returns NULL for malformed JSON. */
  @Test
  public void testHstoreConversion_handlesMalformedJson() {
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String malformedJson = "{\"key1\":\"value1\""; // Missing closing brace
    String expected = "NULL";

    String actual = dmlBuilder.convertJsonToHstoreLiteral(malformedJson);

    assertEquals(expected, actual);
  }

  /** Test that hstore conversion returns NULL for non-object JSON input. */
  @Test
  public void testHstoreConversion_handlesNonObjectJson() {
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String nonObjectJson = "[\"a\", \"b\"]"; // A JSON array is not a JSON object
    String expected = "NULL";

    String actual = dmlBuilder.convertJsonToHstoreLiteral(nonObjectJson);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} correctly
   * formats LTREE types.
   */
  @Test
  public void testLtreeTypeCoercion() {
    // Arrange
    String json = "{\"path_info\":\"Top.Science.Astronomy.Cosmology\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("path_info", "LTREE");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String expected = "'Top.Science.Astronomy.Cosmology'::ltree";

    // Act
    String actual = dmlBuilder.getValueSql(rowObj, "path_info", tableSchema);

    // Assert
    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} correctly
   * handles a null value for LTREE types.
   */
  @Test
  public void testLtreeTypeCoercion_handlesNull() {
    // Arrange
    String json = "{\"path_info\":null}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("path_info", "LTREE");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String expected = "NULL";

    String actual = dmlBuilder.getValueSql(rowObj, "path_info", tableSchema);

    assertEquals(expected, actual);
  }

  /** Test whether LTREE type coercion handles the literal string "NULL". */
  @Test
  public void testLtreeTypeCoercion_handlesNullString() {
    String json = "{\"path_info\":\"NULL\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("path_info", "LTREE");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);

    String actual = dmlBuilder.getValueSql(rowObj, "path_info", tableSchema);

    assertEquals("NULL", actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} correctly
   * handles ENUM types, treating empty strings as NULL.
   */
  @Test
  public void testEnumTypeCoercion_shouldHandleEmptyStringAsNull() {
    // Arrange
    String json = "{\"status_enum\":\"\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("status_enum", "ENUM");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String expected = "NULL";

    String actual = dmlBuilder.getValueSql(rowObj, "status_enum", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} correctly
   * handles a valid ENUM value.
   */
  @Test
  public void testEnumTypeCoercion_handlesValidEnum() {
    // Arrange
    String json = "{\"current_status\":\"in_progress\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("current_status", "ENUM");
    DatastreamToPostgresDML dmlBuilder = DatastreamToPostgresDML.of(null);
    String expected = "'in_progress'";

    String actual = dmlBuilder.getValueSql(rowObj, "current_status", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts array
   * data into correct integer array syntax.
   */
  @Test
  public void testIntArrayTypeCoercion() {
    String arrayJson =
        "{\"number_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": 123},"
            + "  {\"nestedArray\": null, \"elementValue\": 456}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = this.getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("number_array", "_int4");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expectedInt = "ARRAY[123,456]";

    String actualInt =
        DatastreamToPostgresDML.of(null).getValueSql(rowObj, "number_array", tableSchema);

    assertEquals(expectedInt, actualInt);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts array
   * data into correct text array syntax.
   */
  @Test
  public void testTextArrayTypeCoercion() {
    String arrayJson =
        "{\"text_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": \"apple\"},"
            + "  {\"nestedArray\": null, \"elementValue\": \"cherry\"}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = this.getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("text_array", "_text");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expectedInt = "ARRAY['apple','cherry']";

    String actualInt =
        DatastreamToPostgresDML.of(null).getValueSql(rowObj, "text_array", tableSchema);

    assertEquals(expectedInt, actualInt);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts an
   * empty array into the correct PostgreSQL empty array literal '{}'.
   */
  @Test
  public void testEmptyArray() {
    String arrayJson = "{\"empty_array\": {\"nestedArray\": []}}";
    JsonNode rowObj = getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("empty_array", "_TEXT"); // Use a generic array type; could be any array
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'{}'";

    String actual = dml.getValueSql(rowObj, "empty_array", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a
   * JSONB array into the correct PostgreSQL array syntax with type casting.
   */
  @Test
  public void testJsonbArray() {
    String arrayJson =
        "{\"jsonb_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": {\"a\": 1, \"b\": \"test\"}},"
            + "  {\"nestedArray\": null, \"elementValue\": {\"c\": true}}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("jsonb_array", "_JSONB"); // Explicitly specify JSONB array type
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "ARRAY[{\"a\":1,\"b\":\"test\"},{\"c\":true}]::jsonb[]";

    String actual = dml.getValueSql(rowObj, "jsonb_array", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a
   * JSONB array into the correct PostgreSQL array syntax with type casting.
   */
  @Test
  public void testUuidArray() {
    String arrayJson =
        "{\"uuid_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": \"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13\"},"
            + "  {\"nestedArray\": null, \"elementValue\": \"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14\"}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("uuid_array", "_UUID");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected =
        "ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14']::uuid[]";

    String actual = dml.getValueSql(rowObj, "uuid_array", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a byte
   * array into the correct PostgreSQL array syntax with type casting.
   */
  @Test
  public void testByteArray() {
    // Byte arrays are converted to base64 encoded strings by Jackson ObjectNode.toString() in
    // FormatDataStreamRecordToJson.
    String arrayJson = "{\"binary_content\": \"3q2+7w==\"}";
    JsonNode rowObj = getRowObj(arrayJson);

    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("binary_content", "BYTEA");

    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);

    // getValueSql converts byte array to base64 encoded string
    String expected = "decode('3q2+7w==','base64')";

    String actual = dml.getValueSql(rowObj, "binary_content", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * array into the correct PostgreSQL array syntax with type casting.
   */
  @Test
  public void testJsonArray() {
    String arrayJson =
        "{\"json_array\": {"
            + "\"nestedArray\": ["
            + "  {\"nestedArray\": null, \"elementValue\": {\"x\": 10, \"y\": \"abc\"}},"
            + "  {\"nestedArray\": null, \"elementValue\": {\"z\": false}}"
            + "], \"elementValue\": null}}";
    JsonNode rowObj = getRowObj(arrayJson);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("json_array", "_JSON"); // Explicitly specify JSON array type
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "ARRAY[{\"x\":10,\"y\":\"abc\"},{\"z\":false}]::json[]";

    String actual = dml.getValueSql(rowObj, "json_array", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * INTERVAL array into the correct PostgreSQL syntax.
   */
  @Test
  public void testValidInterval() {
    String json = "{\"interval_field\": {\"months\": 1, \"hours\": 2, \"micros\": 3000000}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P1MT2H3.000000S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  /** Test interval conversion with an empty string, which should result in NULL. */
  @Test
  public void testIntervalConversion_withEmptyString() {
    String json = "{\"interval_field\": \"\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);

    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);

    assertEquals("NULL", actual);
  }

  /** Test interval conversion with a JSON object missing the 'months' key. */
  @Test
  public void testIntervalConversion_missingMonthsKey() {
    String json = "{\"interval_field\": {\"hours\": 5, \"micros\": 123}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    // Expecting a default or error state, returning NULL
    String expected = "NULL";

    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);

    assertEquals(expected, actual);
  }

  /**
   * Test interval conversion with a 'nestedArray' key, which should be ignored and result in NULL.
   */
  @Test
  public void testIntervalConversion_withNestedArrayKey() {
    String json = "{\"interval_field\": {\"nestedArray\": [1,2,3]}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);

    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);

    assertEquals("NULL", actual);
  }

  /** Test interval conversion with malformed JSON text. */
  @Test
  public void testIntervalConversion_withMalformedJson() {
    String json = "{\"interval_field\": \"{not-a-json-object\"}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);

    // This should result in an exception being caught and returning a NULL value
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);

    assertEquals("NULL", actual);
  }

  /** Test interval conversion with a JSON object missing the 'hours' key. */
  @Test
  public void testIntervalConversion_missingHoursKey() {
    String json = "{\"interval_field\": {\"months\": 1, \"micros\": 123}}"; // Missing 'hours'
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);

    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);

    assertEquals("NULL", actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * INTERVAL array into the correct PostgreSQL syntax.
   */
  @Test
  public void testOnlyMonths() {
    String json = "{\"interval_field\": {\"months\": 12, \"hours\": 0, \"micros\": 0}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P12MT0H0.000000S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * INTERVAL array into the correct PostgreSQL syntax.
   */
  @Test
  public void testOnlyHours() {
    String json = "{\"interval_field\": {\"months\": 0, \"hours\": 5, \"micros\": 0}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P0MT5H0.000000S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * INTERVAL array into the correct PostgreSQL syntax.
   */
  @Test
  public void testOnlyMicros() {
    String json = "{\"interval_field\": {\"months\": 0, \"hours\": 0, \"micros\": 123456}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P0MT0H0.123456S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  /**
   * Test whether {@link DatastreamToPostgresDML#getValueSql(JsonNode, String, Map)} converts a JSON
   * INTERVAL array into the correct PostgreSQL syntax.
   */
  @Test
  public void testLargeMicros() {
    String json = "{\"interval_field\": {\"months\": 0, \"hours\": 0, \"micros\": 999999999}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P0MT0H999.999999S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  @Test
  public void testZeroValues() {
    String json = "{\"interval_field\": {\"months\": 0, \"hours\": 0, \"micros\": 0}}";
    JsonNode rowObj = getRowObj(json);
    Map<String, String> tableSchema = new HashMap<>();
    tableSchema.put("interval_field", "INTERVAL");
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "'P0MT0H0.000000S'";
    String actual = dml.getValueSql(rowObj, "interval_field", tableSchema);
    assertEquals(expected, actual);
  }

  /** Test cleaning schema map. */
  @Test
  public void testParseSchemaMap() {
    Map<String, String> singleItemExpected =
        new HashMap<String, String>() {
          {
            put("a", "b");
          }
        };
    Map<String, String> doubleItemExpected =
        new HashMap<String, String>() {
          {
            put("a", "b");
            put("c", "d");
          }
        };

    assertThat(DataStreamToSQL.parseSchemaMap("")).isEmpty();
    assertThat(DataStreamToSQL.parseSchemaMap("a:b")).isEqualTo(singleItemExpected);
    assertThat(DataStreamToSQL.parseSchemaMap("a:b,c:d")).isEqualTo(doubleItemExpected);
  }
}
