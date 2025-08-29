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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL.Options;
import com.google.cloud.teleport.v2.transforms.CreateDml;
import com.google.common.truth.Truth;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
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

    // Act
    String actual = dmlBuilder.getValueSql(rowObj, "path_info", tableSchema);

    // Assert
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

    // Act
    String actual = dmlBuilder.getValueSql(rowObj, "status_enum", tableSchema);

    // Assert
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

    // Act
    String actual = dmlBuilder.getValueSql(rowObj, "current_status", tableSchema);

    // Assert
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

  /**
   * Tests the parseMappings method with a mix of schema and table rules, as well as table-only
   * rules that imply schemas.
   */
  @Test
  public void testParseMappings_withMixedAndTableOnlyRules() {
    // 1. Test with a mix of schema-level and table-level rules
    String mixedMapping = "hr:human_resources|hr.employees:human_resources.staff|finance:fin";
    Map<String, Map<String, String>> mixedResult = DataStreamToSQL.parseMappings(mixedMapping);

    assertThat(mixedResult.get("schemas"))
        .containsExactly("hr", "human_resources", "finance", "fin");
    assertThat(mixedResult.get("tables")).containsExactly("hr.employees", "human_resources.staff");

    // 2. Test with an empty string
    Map<String, Map<String, String>> emptyResult = DataStreamToSQL.parseMappings("");
    assertThat(emptyResult.get("schemas")).isEmpty();
    assertThat(emptyResult.get("tables")).isEmpty();
  }

  /** Verifies that the DML generator correctly applies mappings without unexpected case changes. */
  @Test
  public void testTargetNameLogic_withPostgresMapping() {
    // 1. Arrange: Create a DML converter and configure it with a mapping rule set.
    DatastreamToPostgresDML dmlConverter = DatastreamToPostgresDML.of(null);
    Map<String, String> combinedMap = new HashMap<>();
    combinedMap.put("HR", "HUMAN_RESOURCES"); // Schema rule
    combinedMap.put("HR.EMPLOYEES", "HUMAN_RESOURCES.STAFF_2025"); // Specific table rule
    dmlConverter.withSchemaMap(combinedMap);

    // 2. Act & Assert for the table with a specific rule.
    String tableSpecificJson =
        "{\"_metadata_schema\":\"HR\"," + "\"_metadata_table\":\"EMPLOYEES\"" + "}";
    DatastreamRow tableSpecificRow = DatastreamRow.of(getRowObj(tableSpecificJson));

    String actualTargetSchema1 = dmlConverter.getTargetSchemaName(tableSpecificRow);
    String actualTargetTable1 = dmlConverter.getTargetTableName(tableSpecificRow);

    // Assert that the names are mapped correctly, preserving the case from the map.
    assertThat(actualTargetSchema1).isEqualTo("HUMAN_RESOURCES");
    assertThat(actualTargetTable1).isEqualTo("STAFF_2025");

    // 3. Act & Assert for a table that uses the schema-level fallback rule.
    String schemaFallbackJson =
        "{\"_metadata_schema\":\"HR\"," + "\"_metadata_table\":\"DEPARTMENTS\"" + "}";
    DatastreamRow schemaFallbackRow = DatastreamRow.of(getRowObj(schemaFallbackJson));

    String actualTargetSchema2 = dmlConverter.getTargetSchemaName(schemaFallbackRow);
    String actualTargetTable2 = dmlConverter.getTargetTableName(schemaFallbackRow);

    // Assert that the schema is mapped and the table name is preserved as-is.
    assertThat(actualTargetSchema2).isEqualTo("HUMAN_RESOURCES");
    assertThat(actualTargetTable2).isEqualTo("departments");
  }

  @Test
  public void testScenario1_ExplicitPrecedence() {
    // Arrange
    String mapString = "SCHEMA1:SCHEMA2|SCHEMA1.table1:SCHEMA2.TABLE1";
    Map<String, Map<String, String>> mappings = DataStreamToSQL.parseMappings(mapString);
    DatastreamToPostgresDML dmlConverter = DatastreamToPostgresDML.of(null);
    dmlConverter.withSchemaMap(mappings.get("schemas"));
    dmlConverter.withTableNameMap(mappings.get("tables"));

    // Act & Assert for the specific table
    DatastreamRow row1 =
        DatastreamRow.of(
            getRowObj("{\"_metadata_schema\":\"SCHEMA1\",\"_metadata_table\":\"table1\"}"));
    assertThat(dmlConverter.getTargetSchemaName(row1)).isEqualTo("SCHEMA2");
    assertThat(dmlConverter.getTargetTableName(row1)).isEqualTo("TABLE1");

    // Act & Assert for the fallback table
    DatastreamRow row2 =
        DatastreamRow.of(
            getRowObj("{\"_metadata_schema\":\"SCHEMA1\",\"_metadata_table\":\"table2\"}"));
    assertThat(dmlConverter.getTargetSchemaName(row2)).isEqualTo("SCHEMA2");
    assertThat(dmlConverter.getTargetTableName(row2)).isEqualTo("table2");
  }

  @Test
  public void testScenario2_preservesSourceSchema_whenNoSchemaMapExists() {
    // Arrange: Table-level rules are provided, but no schema-level rule.
    String mapString = "SCHEMA1.table1:SCHEMA2.TABLE1|SCHEMA1.table3:SCHEMA2.TABLE3";
    DatastreamToPostgresDML dmlConverter = DatastreamToPostgresDML.of(null);
    Map<String, Map<String, String>> mappings = DataStreamToSQL.parseMappings(mapString);
    dmlConverter.withSchemaMap(mappings.get("schemas"));
    dmlConverter.withTableNameMap(mappings.get("tables"));

    // Create a row for an unmapped table from SCHEMA1.
    DatastreamRow unmappedRow =
        DatastreamRow.of(
            getRowObj("{\"_metadata_schema\":\"SCHEMA1\",\"_metadata_table\":\"table2\"}"));

    // Act
    String actualTargetSchema = dmlConverter.getTargetSchemaName(unmappedRow);
    String actualTargetTable = dmlConverter.getTargetTableName(unmappedRow);

    // Assert: Verify that the original source schema is preserved (and lowercased),
    // as schema inference is no longer active.
    assertThat(actualTargetSchema).isEqualTo("schema1");
    assertThat(actualTargetTable).isEqualTo("table2");
  }

  @Test
  public void testGeneralSchemaRuleTakesPrecedenceOverInference() {
    // Arrange: A general schema rule (SCHEMA1:SCHEMA3) is provided alongside
    // more specific, fully-qualified table rules that map to SCHEMA2.
    String mapString =
        "SCHEMA1:SCHEMA3|SCHEMA1.table1:SCHEMA2.TABLE1|SCHEMA1.table3:SCHEMA2.TABLE3";
    DatastreamToPostgresDML dmlConverter = DatastreamToPostgresDML.of(null);
    Map<String, Map<String, String>> mappings = DataStreamToSQL.parseMappings(mapString);
    dmlConverter.withSchemaMap(mappings.get("schemas"));
    dmlConverter.withTableNameMap(mappings.get("tables"));

    // Create a row for an unmapped table from SCHEMA1.
    DatastreamRow unmappedRow =
        DatastreamRow.of(
            getRowObj("{\"_metadata_schema\":\"SCHEMA1\",\"_metadata_table\":\"table2\"}"));

    // Act
    String actualTargetSchema = dmlConverter.getTargetSchemaName(unmappedRow);
    String actualTargetTable = dmlConverter.getTargetTableName(unmappedRow);

    // Assert: The unmapped table correctly uses the general SCHEMA1:SCHEMA3 rule,
    // and the schema inference logic is ignored.
    assertThat(actualTargetSchema).isEqualTo("SCHEMA3");
    assertThat(actualTargetTable).isEqualTo("table2");
  }

  @Test
  public void testMySqlMapping_withFullyQualifiedRule() {
    // Arrange (Scenario 1 & 2 for MySQL)
    String mapString = "SOURCE_DB.products:PROD_DB.CATALOG";
    DatastreamToMySQLDML dmlConverter = DatastreamToMySQLDML.of(null);

    Map<String, Map<String, String>> mappings = DataStreamToSQL.parseMappings(mapString);
    dmlConverter.withSchemaMap(mappings.get("schemas"));
    dmlConverter.withTableNameMap(mappings.get("tables"));
    DatastreamRow row =
        DatastreamRow.of(
            getRowObj("{\"_metadata_schema\":\"SOURCE_DB\",\"_metadata_table\":\"products\"}"));

    // Act
    String actualCatalog = dmlConverter.getTargetCatalogName(row);
    String actualTable = dmlConverter.getTargetTableName(row);

    // Assert that the fully-qualified rule was applied correctly.
    assertThat(actualCatalog).isEqualTo("PROD_DB");
    assertThat(actualTable).isEqualTo("CATALOG");
  }

  @Test
  public void getDatastreamToDML_returnsPostgresGenerator_forPostgresDriver() {
    // Arrange
    DataSourceConfiguration mockConfig = mock(DataSourceConfiguration.class);
    when(mockConfig.getDriverClassName())
        .thenReturn(ValueProvider.StaticValueProvider.of("org.postgresql.Driver"));

    // Act
    DatastreamToDML dmlGenerator = CreateDml.of(mockConfig).getDatastreamToDML();

    // Assert
    Truth.assertThat(dmlGenerator).isInstanceOf(DatastreamToPostgresDML.class);
  }

  @Test
  public void getDatastreamToDML_returnsMySqlGenerator_forMySqlDriver() {
    // Arrange
    DataSourceConfiguration mockConfig = mock(DataSourceConfiguration.class);
    when(mockConfig.getDriverClassName())
        .thenReturn(ValueProvider.StaticValueProvider.of("com.mysql.cj.jdbc.Driver"));

    // Act
    DatastreamToDML dmlGenerator = CreateDml.of(mockConfig).getDatastreamToDML();

    // Assert
    Truth.assertThat(dmlGenerator).isInstanceOf(DatastreamToMySQLDML.class);
  }

  @Test
  public void getDatastreamToDML_throwsException_forInvalidDriver() {
    // Arrange
    DataSourceConfiguration mockConfig = mock(DataSourceConfiguration.class);
    when(mockConfig.getDriverClassName())
        .thenReturn(ValueProvider.StaticValueProvider.of("unsupported.driver.class"));
    CreateDml createDml = CreateDml.of(mockConfig);

    // Act & Assert
    try {
      createDml.getDatastreamToDML();
      fail("Expected IllegalArgumentException to be thrown.");
    } catch (IllegalArgumentException e) {
      Truth.assertThat(e)
          .hasMessageThat()
          .contains("Database Driver unsupported.driver.class is not supported.");
    }
  }

  @Test
  public void getDataSourceConfiguration_returnsPostgresConfig() {
    // Arrange: Mock pipeline options for a PostgreSQL connection.
    Options options = mock(Options.class);
    when(options.getDatabaseType()).thenReturn("postgres");
    when(options.getDatabaseHost()).thenReturn("localhost");
    when(options.getDatabasePort()).thenReturn("5432");
    when(options.getDatabaseName()).thenReturn("mydb");
    when(options.getCustomConnectionString()).thenReturn("");

    // Act
    DataSourceConfiguration config = DataStreamToSQL.getDataSourceConfiguration(options);

    // Assert: We verify the correct driver was chosen, which confirms the logic.
    Truth.assertThat(config.getDriverClassName().get()).isEqualTo("org.postgresql.Driver");
  }

  @Test
  public void getDataSourceConfiguration_returnsMySqlConfig() {
    // Arrange: Mock pipeline options for a MySQL connection.
    Options options = mock(Options.class);
    when(options.getDatabaseType()).thenReturn("mysql");
    when(options.getDatabaseHost()).thenReturn("127.0.0.1");
    when(options.getDatabasePort()).thenReturn("3306");
    when(options.getDatabaseName()).thenReturn("testdb");
    when(options.getCustomConnectionString()).thenReturn("");

    // Act
    DataSourceConfiguration config = DataStreamToSQL.getDataSourceConfiguration(options);

    // Assert: We verify the correct driver was chosen, which confirms the logic.
    Truth.assertThat(config.getDriverClassName().get()).isEqualTo("com.mysql.cj.jdbc.Driver");
  }

  @Test
  public void getDataSourceConfiguration_throwsException_forInvalidType() {
    // Arrange: Mock pipeline options with an unsupported database type.
    Options options = mock(Options.class);
    when(options.getDatabaseType()).thenReturn("unsupported-db");

    // Act & Assert: Verify that an IllegalArgumentException is thrown.
    try {
      DataStreamToSQL.getDataSourceConfiguration(options);
      fail("Expected IllegalArgumentException to be thrown.");
    } catch (IllegalArgumentException e) {
      Truth.assertThat(e)
          .hasMessageThat()
          .contains("Database Type unsupported-db is not supported.");
    }
  }

  /** Tests the core logic of the applyCasing method for the SNAKE option. */
  @Test
  public void testApplyCasing_snake() {
    DatastreamToDML dml = DatastreamToPostgresDML.of(null).withDefaultCasing("SNAKE");
    assertEquals("my_table", dml.applyCasing("myTable"));
    assertEquals("my_special_table", dml.applyCasing("mySpecialTable"));
  }

  /** Tests the core logic of the applyCasing method for the CAMEL option. */
  @Test
  public void testApplyCasing_camel() {
    DatastreamToDML dml = DatastreamToPostgresDML.of(null).withDefaultCasing("CAMEL");
    // Verifies conversion from snake_case
    assertEquals("myTable", dml.applyCasing("my_table"));
    // Verifies preservation of existing camelCase
    assertEquals("mySpecialTable", dml.applyCasing("mySpecialTable"));
  }

  /** Tests the core logic of the applyCasing method for the UPPERCASE option. */
  @Test
  public void testApplyCasing_uppercase() {
    DatastreamToDML dml = DatastreamToPostgresDML.of(null).withDefaultCasing("UPPERCASE");
    assertEquals("MY_TABLE", dml.applyCasing("my_table"));
  }

  /**
   * Tests the applyCasing method for the LOWERCASE option and the fallback behavior for invalid
   * options.
   */
  @Test
  public void testApplyCasing_lowercaseAndFallback() {
    // Test with "LOWERCASE"
    DatastreamToDML dmlLowercase = DatastreamToPostgresDML.of(null).withDefaultCasing("LOWERCASE");
    assertEquals("my_table", dmlLowercase.applyCasing("MY_TABLE"));

    // Test with an invalid option, which should fall back to default (lowercase)
    DatastreamToDML dmlFallback = DatastreamToPostgresDML.of(null).withDefaultCasing("foo");
    assertEquals("my_table", dmlFallback.applyCasing("MY_TABLE"));
  }

  /** Tests that the applyCasing method correctly handles a null input. */
  @Test
  public void testApplyCasing_nullInput() {
    DatastreamToDML dml = DatastreamToPostgresDML.of(null);
    assertNull(dml.applyCasing(null));
  }

  /**
   * Verifies that getTargetSchemaName and getTargetTableName correctly apply the new casing rules,
   * including preservation of existing case.
   */
  @Test
  public void testTargetNames_withNewCasingOptions() {
    // Arrange
    DatastreamRow mockRow = mock(DatastreamRow.class);
    when(mockRow.getSchemaName()).thenReturn("my_schema");
    when(mockRow.getTableName()).thenReturn("myTable");

    // Act & Assert for CAMEL
    DatastreamToDML dmlCamel = DatastreamToPostgresDML.of(null).withDefaultCasing("CAMEL");
    assertEquals("mySchema", dmlCamel.getTargetSchemaName(mockRow)); // Converts snake_case
    assertEquals("myTable", dmlCamel.getTargetTableName(mockRow));   // Preserves camelCase

    // Act & Assert for SNAKE
    DatastreamToDML dmlSnake = DatastreamToPostgresDML.of(null).withDefaultCasing("SNAKE");
    assertEquals("my_schema", dmlSnake.getTargetSchemaName(mockRow)); // Preserves snake_case
    assertEquals("my_table", dmlSnake.getTargetTableName(mockRow));    // Converts camelCase
  }
}
