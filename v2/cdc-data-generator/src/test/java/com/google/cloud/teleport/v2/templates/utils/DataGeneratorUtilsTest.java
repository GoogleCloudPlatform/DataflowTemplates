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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

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
    Assert.assertTrue(valFaker instanceof String);
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
    Assert.assertTrue(valNum instanceof BigDecimal);
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
    Assert.assertTrue(valNumNegScale instanceof BigDecimal);
    Assert.assertEquals(
        new BigDecimal("123.456")
            .setScale(DataGeneratorUtils.DEFAULT_NUMERIC_SCALE, RoundingMode.HALF_UP),
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
    Assert.assertTrue(valDate instanceof Instant);

    // Timestamp
    DataGeneratorColumn colTs =
        colString.toBuilder()
            .logicalType(LogicalType.TIMESTAMP)
            .fakerExpression("2026-04-30T12:34:56Z")
            .build();
    Object valTs = DataGeneratorUtils.generateValue(colTs, faker);
    Assert.assertTrue(valTs instanceof Instant);
  }

  @Test
  public void testGenerateValue_RichNestedJsonOverride() throws Exception {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("email", "#{internet.emailAddress}");
    inner.put("theme", "dark");

    Map<String, Object> schemaMap = new LinkedHashMap<>();
    schemaMap.put("id", 999L);
    schemaMap.put("name", "#{name.fullName}");
    schemaMap.put("prefs", inner);

    DataGeneratorColumn colJson =
        DataGeneratorColumn.builder()
            .name("rich_json")
            .logicalType(LogicalType.JSON)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression(schemaMap)
            .build();

    Object result = DataGeneratorUtils.generateValue(colJson, faker);
    Assert.assertTrue(result instanceof String);
    String jsonStr = (String) result;

    JsonNode node = new ObjectMapper().readTree(jsonStr);
    Assert.assertEquals(999, node.get("id").asInt());
    Assert.assertFalse(node.get("name").asText().isEmpty());
    Assert.assertEquals("dark", node.get("prefs").get("theme").asText());
    Assert.assertTrue(node.get("prefs").get("email").asText().contains("@"));
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
}
