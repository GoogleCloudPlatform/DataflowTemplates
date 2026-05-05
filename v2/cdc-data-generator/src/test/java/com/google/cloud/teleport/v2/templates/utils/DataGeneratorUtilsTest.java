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

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataGeneratorUtilsTest {

  @Test
  public void testLimitStringLength() {
    assertEquals(
        DataGeneratorUtils.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(null));
    assertEquals(
        DataGeneratorUtils.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(0L));
    assertEquals(
        DataGeneratorUtils.DEFAULT_STRING_LENGTH, DataGeneratorUtils.limitStringLength(-1L));
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
    BigDecimal val4 = DataGeneratorUtils.generateNumeric(col4, faker);
    assertEquals(5, val4.scale()); // Scale should be capped at precision
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
}
