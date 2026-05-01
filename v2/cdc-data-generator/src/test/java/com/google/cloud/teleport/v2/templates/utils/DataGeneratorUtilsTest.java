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

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

  @Test
  public void testGenerateDate_truncatesTime() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("date_col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Instant);
    Instant instant = (Instant) value;
    // Verify time is midnight in some time zone or just check it's a multiple of day millis if
    // UTC.
    // Faker uses system default timezone, so it might be hard to check exact midnight without
    // knowing zone.
  }

  @Test
  public void testGenerateFloat64_withPrecisionAndScale() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("float_col")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .precision(5)
            .scale(2)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Double);
    Double d = (Double) value;
    Assert.assertTrue(d < 1000.0);
    Assert.assertTrue(d > -1000.0);
  }

  @Test
  public void testGenerateFromExpression_FakerExpression() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("name_col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .fakerExpression("#{name.fullName}")
            .build();

    Object value = DataGeneratorUtils.generateFromExpression(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertFalse(((String) value).isEmpty());
  }

  @Test
  public void testGenerateFromExpression_LiteralString() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("const_col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .fakerExpression("fixed_value")
            .build();

    Object value = DataGeneratorUtils.generateFromExpression(column, faker);
    Assert.assertEquals("fixed_value", value);
  }

  @Test
  public void testGenerateFromExpression_LiteralInteger() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("const_int_col")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .fakerExpression("12345")
            .build();

    Object value = DataGeneratorUtils.generateFromExpression(column, faker);
    Assert.assertEquals(12345L, value);
  }

  @Test
  public void testGenerateValue_String() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("string_col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .size(10L)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertEquals(10, ((String) value).length());
  }

  @Test
  public void testGenerateValue_Json() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("json_col")
            .logicalType(LogicalType.JSON)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertTrue(((String) value).startsWith("{"));
  }

  @Test
  public void testGenerateValue_Int64() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("int_col")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Long);
  }

  @Test
  public void testGenerateValue_Boolean() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("bool_col")
            .logicalType(LogicalType.BOOLEAN)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Boolean);
  }

  @Test
  public void testGenerateValue_Bytes() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("bytes_col")
            .logicalType(LogicalType.BYTES)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .size(5L)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof byte[]);
    Assert.assertEquals(5, ((byte[]) value).length);
  }

  @Test
  public void testGenerateValue_Timestamp() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("ts_col")
            .logicalType(LogicalType.TIMESTAMP)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Instant);
  }

  @Test
  public void testGenerateNumeric_Normal() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("num_col")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .precision(10)
            .scale(2)
            .build();

    java.math.BigDecimal value = DataGeneratorUtils.generateNumeric(column, faker);
    Assert.assertNotNull(value);
    Assert.assertEquals(2, value.scale());
  }

  @Test
  public void testGenerateValue_String_DefaultLength() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("string_col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertEquals(DataGeneratorUtils.DEFAULT_STRING_LENGTH, ((String) value).length());
  }

  @Test
  public void testGenerateFromExpression_LiteralBoolean() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.BOOLEAN)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("true")
            .build();

    Object value = DataGeneratorUtils.generateFromExpression(column, faker);
    Assert.assertEquals(true, value);
  }

  @Test
  public void testGenerateFromExpression_InvalidBooleanThrows() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.BOOLEAN)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("maybe")
            .build();

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          DataGeneratorUtils.generateFromExpression(column, faker);
        });
  }
}
