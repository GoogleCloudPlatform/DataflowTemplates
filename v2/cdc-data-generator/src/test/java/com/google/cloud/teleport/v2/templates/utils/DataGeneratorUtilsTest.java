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
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

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

  @Test
  public void testGenerateFromExpression_Literals() {
    // Float64
    DataGeneratorColumn colFloat =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("123.45")
            .build();
    Assert.assertEquals(123.45, DataGeneratorUtils.generateFromExpression(colFloat, faker));

    // Numeric
    DataGeneratorColumn colNum =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isGenerated(false)
            .scale(3)
            .fakerExpression("123.456")
            .build();
    Object valNum = DataGeneratorUtils.generateFromExpression(colNum, faker);
    Assert.assertTrue(valNum instanceof BigDecimal);
    Assert.assertEquals(new BigDecimal("123.456").setScale(3), valNum);

    // Bytes
    DataGeneratorColumn colBytes =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.BYTES)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("hello")
            .build();
    Assert.assertArrayEquals(
        "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8),
        (byte[]) DataGeneratorUtils.generateFromExpression(colBytes, faker));

    // Date
    DataGeneratorColumn colDate =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("2026-04-30")
            .build();
    Object valDate = DataGeneratorUtils.generateFromExpression(colDate, faker);
    Assert.assertTrue(valDate instanceof org.joda.time.Instant);

    // Timestamp
    DataGeneratorColumn colTs =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.TIMESTAMP)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("2026-04-30T12:34:56Z")
            .build();
    Object valTs = DataGeneratorUtils.generateFromExpression(colTs, faker);
    Assert.assertTrue(valTs instanceof org.joda.time.Instant);
  }

  @Test
  public void testGenerateFromExpression_InvalidFormatsThrow() {
    // Invalid Int64
    DataGeneratorColumn colInt =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("abc")
            .build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateFromExpression(colInt, faker));

    // Invalid Date
    DataGeneratorColumn colDate =
        DataGeneratorColumn.builder()
            .name("col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isGenerated(false)
            .fakerExpression("2026/04/30")
            .build();
    Assert.assertThrows(
        RuntimeException.class, () -> DataGeneratorUtils.generateFromExpression(colDate, faker));
  }
}
