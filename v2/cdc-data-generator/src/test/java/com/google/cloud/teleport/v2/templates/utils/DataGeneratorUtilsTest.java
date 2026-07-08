/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.datafaker.Faker;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

  @Test
  public void testLimitStringLength() {
    assertEquals(Constants.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(null));
    assertEquals(Constants.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(0L));
    assertEquals(Constants.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(-1L));
    assertEquals(50, DataGeneratorUtils.limitStringLength(50L));
    assertEquals(Integer.MAX_VALUE, DataGeneratorUtils.limitStringLength(Long.MAX_VALUE));
  }

  @Test
  public void testMapToBeamFieldType() {
    assertEquals(FieldType.STRING, DataGeneratorUtils.mapToBeamFieldType(LogicalType.STRING));
    assertEquals(FieldType.STRING, DataGeneratorUtils.mapToBeamFieldType(LogicalType.JSON));
    assertEquals(FieldType.STRING, DataGeneratorUtils.mapToBeamFieldType(LogicalType.UUID));
    assertEquals(FieldType.INT64, DataGeneratorUtils.mapToBeamFieldType(LogicalType.INT64));
    assertEquals(FieldType.DOUBLE, DataGeneratorUtils.mapToBeamFieldType(LogicalType.FLOAT64));
    assertEquals(FieldType.DECIMAL, DataGeneratorUtils.mapToBeamFieldType(LogicalType.NUMERIC));
    assertEquals(FieldType.BOOLEAN, DataGeneratorUtils.mapToBeamFieldType(LogicalType.BOOLEAN));
    assertEquals(FieldType.BYTES, DataGeneratorUtils.mapToBeamFieldType(LogicalType.BYTES));
    assertEquals(FieldType.DATETIME, DataGeneratorUtils.mapToBeamFieldType(LogicalType.DATE));
    assertEquals(FieldType.DATETIME, DataGeneratorUtils.mapToBeamFieldType(LogicalType.TIMESTAMP));

    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();
    assertEquals(FieldType.STRING, DataGeneratorUtils.mapToBeamFieldType(col));
  }

  @Test
  public void testGenerateNumeric() {
    Faker faker = new Faker();

    // Case 1: Precision and scale specified
    DataGeneratorColumn col1 =
        DataGeneratorColumn.builder()
            .name("c1")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .precision(5)
            .scale(2)
            .build();
    BigDecimal val1 = DataGeneratorUtils.generateNumeric(col1, faker);
    assertEquals(2, val1.scale());

    // Case 2: Null precision (defaults to 10)
    DataGeneratorColumn col2 =
        DataGeneratorColumn.builder()
            .name("c2")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .scale(2)
            .build();
    BigDecimal val2 = DataGeneratorUtils.generateNumeric(col2, faker);
    assertEquals(2, val2.scale());

    // Case 3: Null scale (defaults to 2)
    DataGeneratorColumn col3 =
        DataGeneratorColumn.builder()
            .name("c3")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .precision(10)
            .build();
    BigDecimal val3 = DataGeneratorUtils.generateNumeric(col3, faker);
    assertEquals(2, val3.scale());

    // Case 4: Scale > Precision
    DataGeneratorColumn col4 =
        DataGeneratorColumn.builder()
            .name("c4")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .precision(5)
            .scale(7)
            .build();
    Assert.assertThrows(
        IllegalArgumentException.class, () -> DataGeneratorUtils.generateNumeric(col4, faker));
  }

  @Test
  public void testGenerateValue() {
    Faker faker = new Faker();

    // STRING
    DataGeneratorColumn strCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object strVal = DataGeneratorUtils.generateValue(strCol, faker);
    assertTrue(strVal instanceof String);

    // JSON
    DataGeneratorColumn jsonCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.JSON)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object jsonVal = DataGeneratorUtils.generateValue(jsonCol, faker);
    assertTrue(jsonVal instanceof String);
    assertTrue(((String) jsonVal).startsWith("{"));

    // UUID
    DataGeneratorColumn uuidCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.UUID)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object uuidVal = DataGeneratorUtils.generateValue(uuidCol, faker);
    assertTrue(uuidVal instanceof String);

    // INT64
    DataGeneratorColumn intCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object intVal = DataGeneratorUtils.generateValue(intCol, faker);
    assertTrue(intVal instanceof Long);

    // FLOAT64
    DataGeneratorColumn floatCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object floatVal = DataGeneratorUtils.generateValue(floatCol, faker);
    assertTrue(floatVal instanceof Double);

    // NUMERIC
    DataGeneratorColumn numCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object numVal = DataGeneratorUtils.generateValue(numCol, faker);
    assertTrue(numVal instanceof BigDecimal);

    // BOOLEAN
    DataGeneratorColumn boolCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.BOOLEAN)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object boolVal = DataGeneratorUtils.generateValue(boolCol, faker);
    assertTrue(boolVal instanceof Boolean);

    // BYTES
    DataGeneratorColumn bytesCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.BYTES)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object bytesVal = DataGeneratorUtils.generateValue(bytesCol, faker);
    assertTrue(bytesVal instanceof byte[]);

    // DATE
    DataGeneratorColumn dateCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object dateVal = DataGeneratorUtils.generateValue(dateCol, faker);
    assertTrue(dateVal instanceof Instant);

    // TIMESTAMP
    DataGeneratorColumn tsCol =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.TIMESTAMP)
            .isNullable(false)
            .isGenerated(false)
            .build();
    Object tsVal = DataGeneratorUtils.generateValue(tsCol, faker);
    assertTrue(tsVal instanceof Instant);
  }

  @Test
  public void testGenerateFromExpression_ValidInputs() {
    // String literal & Faker expression
    DataGeneratorColumn colString =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("fixed_value")
            .build();
    Assert.assertEquals("fixed_value", DataGeneratorUtils.generateValue(colString, faker));

    DataGeneratorColumn colFaker =
        colString.toBuilder().fakerExpression("#{name.fullName}").build();
    Object valFaker = DataGeneratorUtils.generateValue(colFaker, faker);
    assertTrue(valFaker instanceof String);
    Assert.assertFalse(((String) valFaker).isEmpty());

    // JSON
    DataGeneratorColumn colJson =
        colString.toBuilder().logicalType(LogicalType.JSON).fakerExpression("{\"id\": 1}").build();
    Assert.assertEquals("{\"id\": 1}", DataGeneratorUtils.generateValue(colJson, faker));

    // Int64
    DataGeneratorColumn colInt =
        colString.toBuilder().logicalType(LogicalType.INT64).fakerExpression("12345").build();
    Assert.assertEquals(12345L, DataGeneratorUtils.generateValue(colInt, faker));

    // Float64
    DataGeneratorColumn colFloat =
        colString.toBuilder().logicalType(LogicalType.FLOAT64).fakerExpression("123.45").build();
    Assert.assertEquals(123.45, DataGeneratorUtils.generateValue(colFloat, faker));

    // Numeric
    DataGeneratorColumn colNum =
        colString.toBuilder()
            .logicalType(LogicalType.NUMERIC)
            .scale(3)
            .fakerExpression("123.456")
            .build();
    Object valNum = DataGeneratorUtils.generateValue(colNum, faker);
    assertTrue(valNum instanceof BigDecimal);
    Assert.assertEquals(new BigDecimal("123.456").setScale(3), valNum);

    // Numeric with negative scale (exercises scale >= 0 false branch, falling back to default
    // scale)
    DataGeneratorColumn colNumNegScale =
        colString.toBuilder()
            .logicalType(LogicalType.NUMERIC)
            .scale(-1)
            .fakerExpression("123.456")
            .build();
    Object valNumNegScale = DataGeneratorUtils.generateValue(colNumNegScale, faker);
    assertTrue(valNumNegScale instanceof BigDecimal);
    Assert.assertEquals(
        new BigDecimal("123.456").setScale(Constants.DEFAULT_NUMERIC_SCALE, RoundingMode.HALF_UP),
        valNumNegScale);

    // Boolean
    DataGeneratorColumn colBoolTrue =
        colString.toBuilder().logicalType(LogicalType.BOOLEAN).fakerExpression("true").build();
    Assert.assertEquals(true, DataGeneratorUtils.generateValue(colBoolTrue, faker));
    DataGeneratorColumn colBoolFalse = colBoolTrue.toBuilder().fakerExpression("false").build();
    Assert.assertEquals(false, DataGeneratorUtils.generateValue(colBoolFalse, faker));

    // Bytes
    DataGeneratorColumn colBytes =
        colString.toBuilder().logicalType(LogicalType.BYTES).fakerExpression("hello").build();
    Assert.assertArrayEquals(
        "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8),
        (byte[]) DataGeneratorUtils.generateValue(colBytes, faker));

    // Date
    DataGeneratorColumn colDate =
        colString.toBuilder().logicalType(LogicalType.DATE).fakerExpression("2026-04-30").build();
    Object valDate = DataGeneratorUtils.generateValue(colDate, faker);
    assertTrue(valDate instanceof Instant);

    // Timestamp
    DataGeneratorColumn colTs =
        colString.toBuilder()
            .logicalType(LogicalType.TIMESTAMP)
            .fakerExpression("2026-04-30T12:34:56Z")
            .build();
    Object valTs = DataGeneratorUtils.generateValue(colTs, faker);
    assertTrue(valTs instanceof Instant);

    // Localized number format parsing with commas check
    DataGeneratorColumn colIntCommas =
        colString.toBuilder().logicalType(LogicalType.INT64).fakerExpression("12,345").build();
    Assert.assertEquals(12345L, DataGeneratorUtils.generateValue(colIntCommas, faker));

    DataGeneratorColumn colFloatCommas =
        colString.toBuilder().logicalType(LogicalType.FLOAT64).fakerExpression("1,234.56").build();
    Assert.assertEquals(1234.56, DataGeneratorUtils.generateValue(colFloatCommas, faker));

    // UUID parsing path check
    DataGeneratorColumn colUuid =
        colString.toBuilder().logicalType(LogicalType.UUID).fakerExpression("abc-123").build();
    Assert.assertEquals("abc-123", DataGeneratorUtils.generateValue(colUuid, faker));
  }

  @Test
  public void testGenerateValue_NestedJsonOverride() throws Exception {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("email", "#{internet.emailAddress}");
    inner.put("theme", "dark");

    Map<String, Object> schemaMap = new LinkedHashMap<>();
    schemaMap.put("id", 999L);
    schemaMap.put("name", "#{name.fullName}");
    schemaMap.put("prefs", inner);

    DataGeneratorColumn colJson =
        DataGeneratorColumn.builder()
            .name("nested_json")
            .logicalType(LogicalType.JSON)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression(schemaMap)
            .build();

    Object result = DataGeneratorUtils.generateValue(colJson, faker);
    assertTrue(result instanceof String);
    String jsonStr = (String) result;

    JsonNode node = new ObjectMapper().readTree(jsonStr);
    Assert.assertEquals(999, node.get("id").asInt());
    Assert.assertFalse(node.get("name").asText().isEmpty());
    Assert.assertEquals("dark", node.get("prefs").get("theme").asText());
    assertTrue(node.get("prefs").get("email").asText().contains("@"));
  }

  @Test
  public void testGenerateFromExpression_InvalidInputsThrow() {
    // Mock Faker to return null to trigger Null input in convertToLogicalType
    Faker mockFaker = mock(Faker.class);
    when(mockFaker.expression("#{invalid}")).thenReturn(null);
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("#{invalid}")
            .build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(col, mockFaker));

    // Mock Faker to THROW an exception in faker.expression()
    Faker mockFakerThrow = mock(Faker.class);
    when(mockFakerThrow.expression("#{throw}"))
        .thenThrow(new RuntimeException("mock fake failure"));
    DataGeneratorColumn colThrow = col.toBuilder().fakerExpression("#{throw}").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colThrow, mockFakerThrow));

    // Invalid formats for typed columns

    // Invalid Int64
    DataGeneratorColumn colInt =
        col.toBuilder().logicalType(LogicalType.INT64).fakerExpression("abc").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colInt, faker));

    // Invalid Float64
    DataGeneratorColumn colFloat =
        col.toBuilder().logicalType(LogicalType.FLOAT64).fakerExpression("abc").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colFloat, faker));

    // Invalid Numeric
    DataGeneratorColumn colNum =
        col.toBuilder().logicalType(LogicalType.NUMERIC).fakerExpression("abc").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colNum, faker));

    // Invalid Boolean
    DataGeneratorColumn colBool =
        col.toBuilder().logicalType(LogicalType.BOOLEAN).fakerExpression("maybe").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colBool, faker));

    // Invalid Date
    DataGeneratorColumn colDate =
        col.toBuilder().logicalType(LogicalType.DATE).fakerExpression("2026/04/30").build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colDate, faker));

    // Invalid Timestamp
    DataGeneratorColumn colTs =
        col.toBuilder()
            .logicalType(LogicalType.TIMESTAMP)
            .fakerExpression("2026-04-30 12:34:56")
            .build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateValue(colTs, faker));
  }

  @Test
  public void testGenerateValue_OverrideOnNonJsonColumn_ThrowsException() {
    Map<String, Object> testMap = new HashMap<>();
    testMap.put("k", "v");
    DataGeneratorColumn strColWithOverride =
        DataGeneratorColumn.builder()
            .name("c")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression(testMap)
            .build();
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DataGeneratorUtils.generateValue(strColWithOverride, faker));
  }

  @Test
  public void testResolveJsonTemplateNode_WithNestedListTemplate() throws Exception {
    List<Object> nestedList = Arrays.asList("#{lorem.word}", 42L);
    Map<String, Object> schemaMap = new LinkedHashMap<>();
    schemaMap.put("tags", nestedList);

    DataGeneratorColumn colJson =
        DataGeneratorColumn.builder()
            .name("json_list")
            .logicalType(LogicalType.JSON)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression(schemaMap)
            .build();

    Object result = DataGeneratorUtils.generateValue(colJson, faker);
    String jsonStr = (String) result;
    com.fasterxml.jackson.databind.JsonNode node =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(jsonStr);
    Assert.assertTrue(node.get("tags").isArray());
    Assert.assertEquals(42, node.get("tags").get(1).asInt());
  }
}
