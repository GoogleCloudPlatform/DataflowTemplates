/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asBigDecimal;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asBoolean;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asDouble;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asFloat;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asInteger;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asLong;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.asString;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.listFullOfNulls;
import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.mapToString;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

/** Unit tests for {@link DataCastingUtils}. */
public class DataCastingUtilsTest {

  @Test
  public void testAsDouble() {
    assertThat(asDouble("123.45")).isWithin(1e-5).of(123.45);
    assertThat(asDouble(123.45)).isWithin(1e-5).of(123.45);
    assertThat(asDouble(123.45f)).isWithin(1e-5).of(123.45);
    assertThat(asDouble(null)).isNull();
  }

  @Test
  public void testAsFloat() {
    assertThat(asFloat("123.45")).isWithin(1e-5f).of(123.45f);
    assertThat(asFloat(123.45)).isWithin(1e-5f).of(123.45f);
    assertThat(asFloat(123.45f)).isWithin(1e-5f).of(123.45f);
    assertThat(asFloat(null)).isNull();
  }

  @Test
  public void testAsInteger() {
    assertThat(asInteger("123")).isEqualTo(123);
    assertThat(asInteger(123)).isEqualTo(123);
    assertThat(asInteger(123.45)).isEqualTo(123);
    assertThat(asInteger(null)).isNull();
  }

  @Test
  public void testAsLong() {
    assertThat(asLong("123")).isEqualTo(123);
    assertThat(asLong(123)).isEqualTo(123);
    assertThat(asLong(123L)).isEqualTo(123);
    assertThat(asLong(123.45)).isEqualTo(123);
  }

  @Test
  public void testAsBoolean() {
    assertThat(asBoolean("true")).isTrue();
    assertThat(asBoolean("TRUE")).isTrue();
    assertThat(asBoolean("false")).isFalse();
    assertThat(asBoolean("FALSE")).isFalse();
    assertThat(asBoolean(null)).isNull();
  }

  @Test
  public void testAsString() {
    assertThat(asString("true")).isEqualTo("true");
    assertThat(asString(true)).isEqualTo("true");
    assertThat(asString(1)).isEqualTo("1");
    assertThat(asString("1")).isEqualTo("1");
    assertThat(asString(null)).isNull();
  }

  @Test
  public void testAsBigDecimal() {
    assertThat(asBigDecimal("123.45").setScale(2, RoundingMode.HALF_UP))
        .isEqualTo(new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP));
    assertThat(asBigDecimal(123.45).setScale(2, RoundingMode.HALF_UP))
        .isEqualTo(new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP));
    assertThat(asBigDecimal(123.45f).setScale(2, RoundingMode.HALF_UP))
        .isEqualTo(new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP));
    assertThat(asBigDecimal(null)).isNull();
  }

  @Test
  public void testMapToString() {
    assertThat(mapToString(new TreeMap<>(ImmutableMap.of("key1", "value1", "key2", "value2"))))
        .isEqualTo("{key1=value1, key2=value2}");
    assertThat(mapToString(null)).isNull();
  }

  @Test
  public void testListFullOfNulls() {
    List<Object> list = new ArrayList<>();
    assertThat(listFullOfNulls(list)).isTrue();
    list.add(null);
    assertThat(listFullOfNulls(list)).isTrue();
    list.add(null);
    assertThat(listFullOfNulls(list)).isTrue();
    list.add("not null");
    assertThat(listFullOfNulls(list)).isFalse();
    list.add(null);
    assertThat(listFullOfNulls(list)).isFalse();
  }

  @Test
  public void testRowToNeo4jDataMap() {
    Schema schema =
        Schema.of(
            Field.of("id", FieldType.INT32),
            Field.of("name", FieldType.STRING),
            Field.of("salary", FieldType.DOUBLE),
            Field.of("hired", FieldType.BOOLEAN),
            Field.of("hireDate", FieldType.DATETIME));
    DateTime hireDate =
        DateTime.parse("2022-08-15T01:02:03Z", ISODateTimeFormat.dateTimeNoMillis())
            .withZone(DateTimeZone.UTC);
    ZonedDateTime expectedDate =
        ZonedDateTime.of(2022, 8, 15, 1, 2, 3, 0, ZoneId.of("UTC", ZoneId.SHORT_IDS));
    Row row =
        Row.withSchema(schema)
            .withFieldValue("id", 1)
            .withFieldValue("name", "neo4j")
            .withFieldValue("salary", 15.5)
            .withFieldValue("hired", true)
            .withFieldValue("hireDate", hireDate)
            .build();
    Target target = new Target();
    target.setName("neo4j-target");
    target.setFieldNames(ImmutableList.of("id", "salary", "hired"));

    Map<String, Object> stringObjectMap = DataCastingUtils.rowToNeo4jDataMap(row, target);
    assertThat(stringObjectMap)
        .containsExactly(
            "id", 1, "name", "neo4j", "salary", 15.5, "hired", true, "hireDate", expectedDate);
  }

  @Test
  public void testSourceTextToTargetObjects() {
    Schema schema =
        Schema.of(
            Field.of("id", FieldType.INT64),
            Field.of("name", FieldType.STRING),
            Field.of("salary", FieldType.DOUBLE),
            Field.of("hired", FieldType.BOOLEAN),
            Field.of("hireDate", FieldType.DATETIME));
    DateTime hireDate =
        DateTime.parse("2022-08-15T01:02:03Z", ISODateTimeFormat.dateTimeNoMillis())
            .withZone(DateTimeZone.UTC);
    ZonedDateTime expectedDate =
        ZonedDateTime.of(2022, 8, 15, 1, 2, 3, 0, ZoneId.of("UTC", ZoneId.SHORT_IDS));
    Row row =
        Row.withSchema(schema)
            .withFieldValue("id", 1L)
            .withFieldValue("name", "neo4j")
            .withFieldValue("salary", 15.5)
            .withFieldValue("hired", true)
            .withFieldValue("hireDate", hireDate)
            .build();
    Target target = new Target();
    target.setName("neo4j-target");
    target.setFieldNames(ImmutableList.of("id", "name", "salary", "hired", "hireDate"));
    target.setMappings(
        ImmutableList.of(
            new Mapping("id", PropertyType.Long),
            new Mapping("name", PropertyType.String),
            new Mapping("salary", PropertyType.BigDecimal),
            new Mapping("hired", PropertyType.Boolean),
            new Mapping("hireDate", PropertyType.DateTime)));

    List<Object> convertedList = DataCastingUtils.sourceTextToTargetObjects(row, target);
    assertThat(convertedList)
        .containsExactly(1L, "neo4j", new BigDecimal(15.5), true, expectedDate);
  }
}
