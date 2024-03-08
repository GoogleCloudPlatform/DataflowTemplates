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

import static com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.truth.Correspondence;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

/** Unit tests for {@link DataCastingUtils}. */
public class DataCastingUtilsTest {
  private static final String FIELD_NAME = "field";

  @Test
  public void testFromBeamType_byte() {
    assertConversion(FieldType.BYTE, Byte.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_int16() {
    assertConversion(FieldType.INT16, Short.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_int32() {
    assertConversion(FieldType.INT32, Integer.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_int64() {
    assertConversion(FieldType.INT64, Long.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_float() {
    assertConversion(FieldType.FLOAT, Float.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_double() {
    assertConversion(FieldType.DOUBLE, Double.MAX_VALUE);
  }

  @Test
  public void testFromBeamType_boolean() {
    assertConversion(FieldType.BOOLEAN, true);
  }

  @Test
  public void testFromBeamType_string() {
    assertConversion(FieldType.STRING, "a string");
  }

  @Test
  public void testFromBeamType_bytes() {
    assertConversion(FieldType.BYTES, "a string".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testFromBeamType_datetime_joda_utc() {
    assertConversion(
        FieldType.DATETIME,
        new DateTime(2005, 5, 1, 23, 59, 59, 999, DateTimeZone.UTC),
        OffsetDateTime.of(2005, 5, 1, 23, 59, 59, 999000000, ZoneOffset.UTC));
  }

  @Test
  public void testFromBeamType_datetime_joda_offset() {
    assertConversion(
        FieldType.DATETIME,
        new DateTime(2005, 5, 1, 23, 59, 59, 999, DateTimeZone.forOffsetHours(2)),
        OffsetDateTime.of(2005, 5, 1, 23, 59, 59, 999000000, ZoneOffset.ofHours(2))
            .toInstant()
            .atOffset(ZoneOffset.UTC));
  }

  @Test
  public void testFromBeamType_datetime_joda_zone_id() {
    assertConversion(
        FieldType.DATETIME,
        new DateTime(2005, 5, 1, 23, 59, 59, 999, DateTimeZone.forID("Etc/GMT+2")),
        ZonedDateTime.of(2005, 5, 1, 23, 59, 59, 999000000, ZoneId.of("Etc/GMT+2"))
            .toInstant()
            .atOffset(ZoneOffset.UTC));
  }

  @Test
  public void testFromBeamType_decimal() {
    assertConversion(FieldType.DECIMAL, BigDecimal.valueOf(12.6789), 12.6789);
  }

  @Test
  public void testFromBeamType_array() {
    assertConversion(
        FieldType.array(FieldType.INT64),
        LongStream.range(0, 124).boxed().collect(Collectors.toList()));
  }

  @Test
  public void testFromBeamType_iterable() {
    assertConversion(
        FieldType.iterable(FieldType.INT64),
        LongStream.range(0, 10240).boxed().collect(Collectors.toList()));
  }

  @Test
  public void testFromBeamType_map_simple() {
    assertConversion(
        FieldType.map(FieldType.STRING, FieldType.INT64), Map.of("a", 1L, "b", 2L, "c", 3L));
  }

  @Test
  public void testFromBeamType_map_complex() {
    assertConversion(
        FieldType.map(FieldType.STRING, FieldType.array(FieldType.INT32)),
        Map.of("a", List.of(1, 2, 3), "b", List.of(3, 4, 5), "c", List.of(5, 6, 7)));
  }

  @Test
  public void testFromBeamType_map_non_string_key_fails() {
    var ex =
        assertThrows(
            RuntimeException.class,
            () ->
                assertConversion(
                    FieldType.map(FieldType.INT64, FieldType.array(FieldType.INT32)),
                    Map.of(1L, List.of(1, 2, 3))));

    assertThat(ex.getMessage())
        .isEqualTo("Only strings are supported as MAP key values, found 'INT64' in field 'field'");
  }

  @Test
  public void testFromBeamType_row() {
    var schema =
        Schema.builder()
            .addField("a", FieldType.STRING)
            .addField("b", FieldType.INT32)
            .addField("c", FieldType.array(FieldType.STRING))
            .build();

    assertConversion(
        FieldType.row(schema),
        Row.withSchema(schema)
            .withFieldValue("a", "a string")
            .withFieldValue("b", 32)
            .withFieldValue("c", List.of("a", "list", "of", "strings"))
            .build(),
        Map.of("a", "a string", "b", 32, "c", List.of("a", "list", "of", "strings")));
  }

  @Test
  public void testFromBeamType_logicaltype_datetime() {
    assertConversion(
        FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.DateTime()),
        LocalDateTime.of(2004, 5, 1, 23, 59, 59, 999999999));
  }

  @Test
  public void testFromBeamType_logicaltype_date() {
    assertConversion(
        FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.Date()),
        LocalDate.of(2004, 5, 1));
  }

  @Test
  public void testFromBeamType_logicaltype_time() {
    assertConversion(
        FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.Time()),
        LocalTime.of(23, 59, 59, 999999999));
  }

  @Test
  public void testFromBeamType_logicaltype_instant_nanos() {
    assertConversion(
        FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.NanosInstant()),
        LocalDateTime.of(2004, 5, 1, 23, 59, 59, 999999999).toInstant(ZoneOffset.UTC),
        OffsetDateTime.of(2004, 5, 1, 23, 59, 59, 999999999, ZoneOffset.UTC));
  }

  @Test
  public void testFromBeamType_logicaltype_instant_millis() {
    assertConversion(
        FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant()),
        LocalDateTime.of(2004, 5, 1, 23, 59, 59, 999999000).toInstant(ZoneOffset.UTC),
        OffsetDateTime.of(2004, 5, 1, 23, 59, 59, 999999000, ZoneOffset.UTC));
  }

  @Test
  public void testFromBeamType_logicaltype_enum() {
    assertConversion(
        FieldType.logicalType(
            org.apache.beam.sdk.schemas.logicaltypes.EnumerationType.create(
                Map.of("BLUE", 1, "YELLOW", 2, "RED", 3))),
        new EnumerationType.Value(3),
        3);
  }

  @Test
  public void testFromBeamType_logicaltype_unsupported() {
    var ex =
        assertThrows(
            RuntimeException.class,
            () -> {
              assertConversion(
                  FieldType.logicalType(
                      org.apache.beam.sdk.schemas.logicaltypes.FixedString.of(32)),
                  "a fixed string");
            });

    assertThat(ex.getMessage())
        .isEqualTo(
            "Field 'field' of type 'LOGICAL_TYPE' ('beam:logical_type:fixed_char:v1') is not supported.");
  }

  @Test
  public void testAsByteArray() {
    assertThat(asByteArray(null)).isNull();
    assertThat(asByteArray(new byte[] {1, 2, 3})).isEqualTo(new byte[] {1, 2, 3});
    assertThat(asByteArray(Base64.getEncoder().encodeToString(new byte[] {1, 2, 3})))
        .isEqualTo(new byte[] {1, 2, 3});
    assertThat(asByteArray("a string")).isEqualTo("a string".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testAsDuration() {
    assertThat(asDuration(null)).isNull();
    assertThat(asDuration(Duration.ofHours(2))).isEqualTo(Duration.ofHours(2));
    assertThat(asDuration("PT23H59S")).isEqualTo(Duration.ofHours(23).plusSeconds(59));
  }

  @Test
  public void testAsDate() {
    assertThat(asDate(null)).isNull();
    assertThat(asDate(LocalDate.of(2000, 1, 1))).isEqualTo(LocalDate.of(2000, 1, 1));
    assertThat(asDate("2020-01-01")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00.000Z")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00.000123Z")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00.000123456Z")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00.000123456+02:00")).isEqualTo(LocalDate.of(2020, 1, 1));
    assertThat(asDate("2020-01-01T12:00:00.000123456Z[Europe/Paris]"))
        .isEqualTo(LocalDate.of(2020, 1, 1));
  }

  @Test
  public void testAsTime() {
    assertThat(asTime(null)).isNull();
    assertThat(asTime(LocalTime.of(23, 59, 59, 999999999)))
        .isEqualTo(LocalTime.of(23, 59, 59, 999999999));
    assertThat(asTime(LocalDateTime.of(2020, 1, 1, 23, 59, 59, 999999999)))
        .isEqualTo(LocalTime.of(23, 59, 59, 999999999));
    assertThat(asTime(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC)))
        .isEqualTo(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC));
    assertThat(asTime(OffsetDateTime.of(2020, 1, 1, 23, 59, 59, 999999999, ZoneOffset.UTC)))
        .isEqualTo(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC));
    assertThat(asTime("2020-01-01T12:00:00")).isEqualTo(LocalTime.of(12, 0, 0));
    assertThat(asTime("2020-01-01T12:00:00.123456789"))
        .isEqualTo(LocalTime.of(12, 0, 0, 123456789));
    assertThat(asTime("2020-01-01T12:00:00.000Z"))
        .isEqualTo(OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC));
    assertThat(asTime("2020-01-01T12:00:00.000123Z"))
        .isEqualTo(OffsetTime.of(12, 0, 0, 123000, ZoneOffset.UTC));
    assertThat(asTime("2020-01-01T12:00:00.000123456Z"))
        .isEqualTo(OffsetTime.of(12, 0, 0, 123456, ZoneOffset.UTC));
    assertThat(asTime("2020-01-01T12:00:00.000123456+02:00"))
        .isEqualTo(OffsetTime.of(12, 0, 0, 123456, ZoneOffset.ofHours(2)));
  }

  @Test
  public void testAsDateTime() {
    assertThat(asDateTime(null)).isNull();
    assertThat(asDateTime(LocalDateTime.of(2020, 1, 1, 23, 59, 59, 999999999)))
        .isEqualTo(LocalDateTime.of(2020, 1, 1, 23, 59, 59, 999999999));
    assertThat(asDateTime(OffsetDateTime.of(2020, 1, 1, 23, 59, 59, 999999999, ZoneOffset.UTC)))
        .isEqualTo(OffsetDateTime.of(2020, 1, 1, 23, 59, 59, 999999999, ZoneOffset.UTC));
    assertThat(
            asDateTime(ZonedDateTime.of(2020, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Etc/GMT+2"))))
        .isEqualTo(ZonedDateTime.of(2020, 1, 1, 23, 59, 59, 999999999, ZoneId.of("Etc/GMT+2")));
    assertThat(asDateTime("2020-01-01T12:00:00")).isEqualTo(LocalDateTime.of(2020, 1, 1, 12, 0, 0));
    assertThat(asDateTime("2020-01-01T12:00:00.123456789"))
        .isEqualTo(LocalDateTime.of(2020, 1, 1, 12, 0, 0, 123456789));
    assertThat(asDateTime("2020-01-01T12:00:00.000Z", ZonedDateTime::from, OffsetDateTime::from))
        .isEqualTo(OffsetDateTime.of(2020, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC));
    assertThat(asDateTime("2020-01-01T12:00:00.000123Z", ZonedDateTime::from, OffsetDateTime::from))
        .isEqualTo(OffsetDateTime.of(2020, 1, 1, 12, 0, 0, 123000, ZoneOffset.UTC));
    assertThat(
            asDateTime("2020-01-01T12:00:00.000123456Z", ZonedDateTime::from, OffsetDateTime::from))
        .isEqualTo(OffsetDateTime.of(2020, 1, 1, 12, 0, 0, 123456, ZoneOffset.UTC));
    assertThat(
            asDateTime(
                "2020-01-01T12:00:00.000123456+02:00", ZonedDateTime::from, OffsetDateTime::from))
        .isEqualTo(OffsetDateTime.of(2020, 1, 1, 12, 0, 0, 123456, ZoneOffset.ofHours(2)));
    assertThat(
            asDateTime(
                "2020-01-01T12:00:00.000123456+03:00[Europe/Istanbul]",
                ZonedDateTime::from,
                OffsetDateTime::from))
        .isEqualTo(ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 123456, ZoneId.of("Europe/Istanbul")));
  }

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
            Field.of(
                "hireDate",
                FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.DateTime())));
    LocalDateTime hireDate = LocalDateTime.of(2022, 8, 15, 1, 2, 3);
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
            "id",
            1,
            "name",
            "neo4j",
            "salary",
            15.5,
            "hired",
            true,
            "hireDate",
            LocalDateTime.of(2022, 8, 15, 1, 2, 3));
  }

  @Test
  public void testSourceTextToTargetObjects() {
    Schema schema =
        Schema.of(
            Field.of("int64", FieldType.STRING), // int64
            Field.of("string", FieldType.STRING), // string
            Field.of("double", FieldType.STRING), // double
            Field.of("boolean", FieldType.STRING), // boolean
            Field.of("localdate", FieldType.STRING), // localdate
            Field.of("localtime", FieldType.STRING), // localtime
            Field.of("localdatetime", FieldType.STRING), // localdatetime
            Field.of("offsettime", FieldType.STRING), // localtime
            Field.of("zoneddatetime", FieldType.STRING), // zoneddatetime
            Field.of("offsetdatetime", FieldType.STRING), // offsetdatetime
            Field.of("duration", FieldType.STRING), // duration
            Field.of("bytes", FieldType.STRING));

    Row row =
        Row.withSchema(schema)
            .withFieldValue("int64", "123456")
            .withFieldValue("string", "a string")
            .withFieldValue("double", "123456.123")
            .withFieldValue("boolean", "true")
            .withFieldValue("localdate", "2020-01-02")
            .withFieldValue("localtime", "21:53:59.999123456")
            .withFieldValue("localdatetime", "2020-01-02T21:53:59.999123456")
            .withFieldValue("offsettime", "21:53:59.999123456+03:00")
            .withFieldValue("zoneddatetime", "2020-01-02T21:53:59.999123456+03:00[Europe/Istanbul]")
            .withFieldValue("offsetdatetime", "2020-01-02T21:53:59.999123456+03:00")
            .withFieldValue("duration", "PT23H59M59S")
            .withFieldValue("bytes", "SGVsbG8gV29ybGQ=")
            .build();
    Target target = new Target();
    target.setName("neo4j-target");
    target.setFieldNames(
        ImmutableList.of(
            "int64",
            "string",
            "double",
            "boolean",
            "localdate",
            "localtime",
            "localdatetime",
            "offsettime",
            "zoneddatetime",
            "offsetdatetime",
            "duration",
            "bytes"));
    target.setMappings(
        ImmutableList.of(
            mapping("int64", PropertyType.Long),
            mapping("string", PropertyType.String),
            mapping("double", PropertyType.BigDecimal),
            mapping("boolean", PropertyType.Boolean),
            mapping("localdate", PropertyType.Date),
            mapping("localtime", PropertyType.LocalTime),
            mapping("localdatetime", PropertyType.LocalDateTime),
            mapping("offsettime", PropertyType.Time),
            mapping("zoneddatetime", PropertyType.DateTime),
            mapping("offsetdatetime", PropertyType.DateTime),
            mapping("duration", PropertyType.Duration),
            mapping("bytes", PropertyType.ByteArray)));

    List<Object> convertedList = DataCastingUtils.sourceTextToTargetObjects(row, target);
    assertThat(convertedList)
        .comparingElementsUsing(Correspondence.from(Objects::deepEquals, "deep equals"))
        .containsExactly(
            123456L,
            "a string",
            123456.123,
            true,
            LocalDate.of(2020, 1, 2),
            LocalTime.of(21, 53, 59, 999123456),
            LocalDateTime.of(2020, 1, 2, 21, 53, 59, 999123456),
            OffsetTime.of(21, 53, 59, 999123456, ZoneOffset.ofHours(3)),
            ZonedDateTime.of(2020, 1, 2, 21, 53, 59, 999123456, ZoneId.of("Europe/Istanbul")),
            OffsetDateTime.of(2020, 1, 2, 21, 53, 59, 999123456, ZoneOffset.ofHours(3)),
            Duration.ofHours(23).plusMinutes(59).plusSeconds(59),
            "Hello World".getBytes(StandardCharsets.UTF_8));
  }

  private static Mapping mapping(String field, PropertyType type) {
    Mapping mapping = new Mapping(FragmentType.node, RoleType.property, tuple(field, field));
    mapping.setType(type);
    return mapping;
  }

  private static FieldNameTuple tuple(String name, String field) {
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setName(name);
    tuple.setField(field);
    return tuple;
  }

  private void assertConversion(FieldType type, Object input) {
    assertConversion(type, input, input);
  }

  private void assertConversion(FieldType type, Object input, Object expected) {
    var field = Field.of(FIELD_NAME, type);
    var row = newRowWithField(field, input);

    assertThat(fromBeamType(FIELD_NAME, field.getType(), row.getValue(FIELD_NAME)))
        .isEqualTo(expected);
  }

  private Row newRowWithField(Field field, Object fieldValue) {
    return Row.withSchema(Schema.builder().addField(field).build()).addValue(fieldValue).build();
  }
}
