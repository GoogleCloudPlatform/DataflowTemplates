/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import static com.google.cloud.teleport.v2.spanner.migrations.avro.AvroToValueMapper.avroArrayFieldToSpannerArray;
import static com.google.cloud.teleport.v2.spanner.migrations.avro.AvroToValueMapper.getGsqlMap;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.type.Type.Code;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroToValueMapperTest {

  @Test
  public void testAvroFieldToBoolean() {
    Boolean inputValue = true;
    Boolean result =
        AvroToValueMapper.avroFieldToBoolean(inputValue, SchemaBuilder.builder().booleanType());
    assertEquals("Test true boolean input", inputValue, result);

    inputValue = false;
    result =
        AvroToValueMapper.avroFieldToBoolean(inputValue, SchemaBuilder.builder().booleanType());
    assertEquals("Test false boolean input", inputValue, result);

    result = AvroToValueMapper.avroFieldToBoolean("true", SchemaBuilder.builder().stringType());
    assertEquals("Test string input", true, result);

    result = AvroToValueMapper.avroFieldToBoolean(4, SchemaBuilder.builder().intType());
    assertEquals("Test int input", false, result);

    result = AvroToValueMapper.avroFieldToBoolean(1L, SchemaBuilder.builder().longType());
    assertEquals("Test long 1 input", true, result);

    result = AvroToValueMapper.avroFieldToBoolean(0L, SchemaBuilder.builder().longType());
    assertEquals("Test long 0 input", false, result);

    result = AvroToValueMapper.avroFieldToBoolean("1", SchemaBuilder.builder().longType());
    assertEquals("Test string \"1\" input", true, result);

    result = AvroToValueMapper.avroFieldToBoolean("0", SchemaBuilder.builder().longType());
    assertEquals("Test string \"0\" input", false, result);
  }

  @Test
  public void testAvroFieldToBoolean_NullInput() {
    Boolean result =
        AvroToValueMapper.avroFieldToBoolean(null, SchemaBuilder.builder().stringType());
    assertNull(result);
  }

  @Test
  public void testAvroFieldToLong_ValidConversion() {
    Long inputValue = 10L;
    Long result = AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().longType());
    assertEquals(inputValue, result);

    // Test integer values as well.
    Integer intValue = 10;
    result = AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().intType());
    assertEquals("Test int input", Long.valueOf(intValue), result);

    // Test string values as well.
    result = AvroToValueMapper.avroFieldToLong("1536", SchemaBuilder.builder().intType());
    assertEquals("Test string input", (Long) 1536L, result);
  }

  @Test
  public void testAvroFieldToLong_ExtremaConversion() {
    Long inputValue = Long.MAX_VALUE;
    Long result = AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().longType());
    assertEquals("Test long max input", inputValue, result);

    inputValue = Long.MIN_VALUE;
    result = AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().longType());
    assertEquals("Test long min input", inputValue, result);

    // Test integer values as well.
    Integer intValue = Integer.MAX_VALUE;
    result = AvroToValueMapper.avroFieldToLong(intValue, SchemaBuilder.builder().intType());
    assertEquals("Test int max input", Long.valueOf(intValue), result);

    intValue = Integer.MIN_VALUE;
    result = AvroToValueMapper.avroFieldToLong(intValue, SchemaBuilder.builder().intType());
    assertEquals("Test int min input", Long.valueOf(intValue), result);

    // Test string values as well.
    String strVal = ((Long) Long.MAX_VALUE).toString();
    result = AvroToValueMapper.avroFieldToLong(strVal, SchemaBuilder.builder().stringType());
    assertEquals("Test string max input", Long.valueOf(strVal), result);

    strVal = ((Long) Long.MIN_VALUE).toString();
    result = AvroToValueMapper.avroFieldToLong(strVal, SchemaBuilder.builder().stringType());
    assertEquals("Test string min input", Long.valueOf(strVal), result);
  }

  @Test
  public void testAvroFieldToLong_NullInput() {
    Object inputValue = null;
    Long result = AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().longType());
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToLong_NonLongInput() {
    String inputValue = "test";
    AvroToValueMapper.avroFieldToLong(inputValue, SchemaBuilder.builder().stringType());
  }

  @Test
  public void testAvroFieldToDouble_ValidDoubleInput() {
    Double inputValue = 5.75;
    Double result =
        AvroToValueMapper.avroFieldToDouble(inputValue, SchemaBuilder.builder().doubleType());
    assertEquals(inputValue, result);

    result = AvroToValueMapper.avroFieldToDouble(3.14f, SchemaBuilder.builder().floatType());
    assertEquals("Test float input", (Double) 3.14, result);

    result = AvroToValueMapper.avroFieldToDouble("456.346", SchemaBuilder.builder().doubleType());
    assertEquals("Test string input", (Double) 456.346, result);

    result =
        AvroToValueMapper.avroFieldToDouble(Long.MAX_VALUE, SchemaBuilder.builder().longType());
    assertEquals("Test long input", Double.valueOf(Long.MAX_VALUE), result);

    result =
        AvroToValueMapper.avroFieldToDouble(Long.MIN_VALUE, SchemaBuilder.builder().longType());
    assertEquals("Test long input", Double.valueOf(Long.MIN_VALUE), result);

    result = AvroToValueMapper.avroFieldToDouble(10, SchemaBuilder.builder().intType());
    assertEquals("Test int input", Double.valueOf(10), result);
  }

  @Test
  public void testAvroFieldToDouble_NullInput() {
    Object inputValue = null;
    Double result =
        AvroToValueMapper.avroFieldToDouble(inputValue, SchemaBuilder.builder().doubleType());
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToDouble_UnsupportedType() {
    Boolean inputValue = true;
    AvroToValueMapper.avroFieldToDouble(inputValue, SchemaBuilder.builder().booleanType());
  }

  @Test
  public void testAvroFieldToFloat_ValidFloat32Input() {
    Float inputValue = 5.75f;
    Float result =
        AvroToValueMapper.avroFieldToFloat32(inputValue, SchemaBuilder.builder().doubleType());
    assertEquals(inputValue, result);

    /* Test a valid double to float mapping */
    result =
        AvroToValueMapper.avroFieldToFloat32((Double) 3.14, SchemaBuilder.builder().floatType());
    assertEquals("Test float input", (Float) 3.14f, result);

    Value value =
        getGsqlMap().get(Type.float32()).apply(3.14f, SchemaBuilder.builder().floatType());
    assertEquals("Test float input", Value.float32(3.14f), value);

    result = AvroToValueMapper.avroFieldToFloat32("456.346", SchemaBuilder.builder().doubleType());
    assertEquals("Test string input", (Float) 456.346f, result);

    result =
        AvroToValueMapper.avroFieldToFloat32(Float.MAX_VALUE, SchemaBuilder.builder().doubleType());
    assertEquals("Test max Float", (Float) Float.MAX_VALUE, result);

    result =
        AvroToValueMapper.avroFieldToFloat32(Float.MIN_VALUE, SchemaBuilder.builder().doubleType());
    assertEquals("Test min Float", (Float) Float.MIN_VALUE, result);

    result = AvroToValueMapper.avroFieldToFloat32(Float.NaN, SchemaBuilder.builder().doubleType());
    assertEquals("Test nan Float", (Float) Float.NaN, result);

    result =
        AvroToValueMapper.avroFieldToFloat32(
            Float.POSITIVE_INFINITY, SchemaBuilder.builder().doubleType());
    assertEquals("Test positive infinity Float", (Float) Float.POSITIVE_INFINITY, result);

    result =
        AvroToValueMapper.avroFieldToFloat32(
            Float.NEGATIVE_INFINITY, SchemaBuilder.builder().doubleType());
    assertEquals("Test negetive infinity Float", (Float) Float.NEGATIVE_INFINITY, result);

    result = AvroToValueMapper.avroFieldToFloat32(10, SchemaBuilder.builder().intType());
    assertEquals("Test int input", Float.valueOf(10), result);
  }

  @Test
  public void testAvroFieldToFloat32_NullInput() {
    Object inputValue = null;
    Float result =
        AvroToValueMapper.avroFieldToFloat32(inputValue, SchemaBuilder.builder().floatType());
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToFloat32_UnsupportedType() {
    Boolean inputValue = true;
    AvroToValueMapper.avroFieldToFloat32(inputValue, SchemaBuilder.builder().booleanType());
  }

  @Test
  public void testAvroFieldToString_valid() {
    String result =
        AvroToValueMapper.avroFieldToString("Hello", SchemaBuilder.builder().stringType());
    assertEquals("Hello", result);

    Value value =
        getGsqlMap().get(Type.string()).apply("Hello", SchemaBuilder.builder().stringType());
    assertEquals("Test String input", Value.string("Hello"), value);

    final String fruitJson =
        "{\n"
            + "    \"fruit\": \"Apple\",\n"
            + "    \"size\": \"Large\",\n"
            + "    \"color\": \"Red\"\n"
            + "}";
    Value valueJson =
        getGsqlMap().get(Type.json()).apply(fruitJson, SchemaBuilder.builder().stringType());
    assertEquals("Test json input", Value.string(fruitJson), valueJson);

    result = AvroToValueMapper.avroFieldToString("", SchemaBuilder.builder().stringType());
    assertEquals("", result);

    result = AvroToValueMapper.avroFieldToString(14, SchemaBuilder.builder().intType());
    assertEquals("14", result);

    result = AvroToValueMapper.avroFieldToString(513148134L, SchemaBuilder.builder().longType());
    assertEquals("513148134", result);

    result = AvroToValueMapper.avroFieldToString(325.532, SchemaBuilder.builder().doubleType());
    assertEquals("325.532", result);
  }

  @Test
  public void testAvroFieldToString_NullInput() {
    assertNull(AvroToValueMapper.avroFieldToString(null, SchemaBuilder.builder().nullType()));
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToString_Exception() {
    class ThrowObject {
      public String toString() {
        throw new RuntimeException("explicit exception");
      }
    }
    AvroToValueMapper.avroFieldToString(new ThrowObject(), SchemaBuilder.builder().nullType());
  }

  @Test
  public void testAvroFieldToNumericBigDecimal_StringInput() {
    Map<String, String> testCases = new HashMap<>();
    testCases.put("1.2334567890345654542E10", "12334567890.345654542");
    testCases.put("-1.2334567890345654542E10", "-12334567890.345654542");
    testCases.put(
        "1233456789034565454223463732234502384848374579495483732758539938558",
        "1233456789034565454223463732234502384848374579495483732758539938558.000000000");
    testCases.put(
        "-1233456789034565454223463732234502384848374579495483732758539938558",
        "-1233456789034565454223463732234502384848374579495483732758539938558.000000000");
    testCases.put("123456789.0123456789", "123456789.012345679");
    testCases.put("-123456789.0123456789", "-123456789.012345679");
    testCases.put(
        "123345678903456545422346373223.903495832", "123345678903456545422346373223.903495832");
    testCases.put("123345.678903456545422346373223903495832", "123345.678903457");
    testCases.put("9223372036854775807", "9223372036854775807.000000000");
    testCases.put("-9223372036854775807", "-9223372036854775807.000000000");
    testCases.put(
        ((Long) Long.MIN_VALUE).toString(), String.format("%s.000000000", Long.MIN_VALUE));
    testCases.put(
        ((Long) Long.MAX_VALUE).toString(), String.format("%s.000000000", Long.MAX_VALUE));
    for (String input : testCases.keySet()) {
      BigDecimal result =
          AvroToValueMapper.avroFieldToNumericBigDecimal(
              input, SchemaBuilder.builder().stringType());
      assertEquals(
          String.format("Test case input : %s", input), testCases.get(input), result.toString());
    }
  }

  @Test
  public void testAvroFieldToNumericBigDecimal_DoubleInput() {
    Double inputValue = 3.14159;
    BigDecimal expectedResult = new BigDecimal(inputValue).setScale(9, RoundingMode.HALF_UP);
    BigDecimal result =
        AvroToValueMapper.avroFieldToNumericBigDecimal(
            inputValue, SchemaBuilder.builder().doubleType());
    assertEquals(expectedResult, result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToNumericBigDecimal_InvalidInput_string() {
    String inputValue = "123456.789asd";
    AvroToValueMapper.avroFieldToNumericBigDecimal(
        inputValue, SchemaBuilder.builder().stringType());
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToNumericBigDecimal_InvalidInput_boolean() {
    Boolean inputValue = true;
    AvroToValueMapper.avroFieldToNumericBigDecimal(
        inputValue, SchemaBuilder.builder().booleanType());
  }

  @Test
  public void testAvroFieldToNumericBigDecimal_NullInput() {
    Object inputValue = null;
    BigDecimal result =
        AvroToValueMapper.avroFieldToNumericBigDecimal(
            inputValue, SchemaBuilder.builder().stringType());
    assertNull(result);
  }

  @Test
  public void testAvroFieldToByteArray_LongInput() {
    Long inputValue = 7L;
    ByteArray expectedResult = ByteArray.copyFrom(new byte[] {07});
    ByteArray result =
        AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().longType());
    assertEquals("Test long input", expectedResult, result);

    inputValue = Long.MAX_VALUE;
    expectedResult = ByteArray.copyFrom(new byte[] {127, -1, -1, -1, -1, -1, -1, -1});
    result = AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().longType());
    assertEquals("Test long.MAX  input", expectedResult, result);

    inputValue = 0L;
    expectedResult = ByteArray.copyFrom(new byte[] {00});
    result = AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().longType());
    assertEquals("Test 0 input", expectedResult, result);
  }

  @Test
  public void testAvroFieldToByteArray_StringInput() throws Exception {
    // Test even length string.
    String inputValue = "68656c6c6f20686f772061722065796f75";
    ByteArray expectedResult =
        ByteArray.copyFrom(
            new byte[] {
              104, 101, 108, 108, 111, 32, 104, 111, 119, 32, 97, 114, 32, 101, 121, 111, 117
            });
    ByteArray result =
        AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().stringType());
    assertEquals("Test even length input", expectedResult, result);

    // Test odd length string.
    inputValue = "8656c6c6f20686f772061722065796f75";
    expectedResult =
        ByteArray.copyFrom(
            new byte[] {
              8, 101, 108, 108, 111, 32, 104, 111, 119, 32, 97, 114, 32, 101, 121, 111, 117
            });
    result =
        AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().stringType());
    assertEquals("Test odd length input", expectedResult, result);
  }

  @Test
  public void testAvroFieldToByteArray_ValidByteArrayInput() {
    byte[] bytes = {10, 20, 30};
    ByteBuffer inputValue = ByteBuffer.wrap(bytes);
    ByteArray result =
        AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().bytesType());
    assertEquals(ByteArray.copyFrom(bytes), result);
  }

  @Test
  public void testAvroFieldToByteArray_NullInput() {
    ByteArray result =
        AvroToValueMapper.avroFieldToByteArray(null, SchemaBuilder.builder().bytesType());
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToByteArray_UnsupportedType() {
    Integer inputValue = 5;
    AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().intType());
  }

  @Test
  public void testAvroFieldToTimestamp_valid() {
    Map<String, String> testCases = new HashMap<>();
    testCases.put("2020-12-30T12:12:12Z", "2020-12-30T12:12:12Z");
    testCases.put("2020-12-30T12:12:12.1Z", "2020-12-30T12:12:12.1Z");
    testCases.put("2020-12-30T12:12:12.123Z", "2020-12-30T12:12:12.123Z");
    testCases.put("2020-12-30T12:12:12", "2020-12-30T12:12:12Z");
    testCases.put("2020-12-30T12:12:12.1", "2020-12-30T12:12:12.1Z");
    testCases.put("2020-12-30T12:12:12.12345", "2020-12-30T12:12:12.12345Z");
    testCases.put("2023-12-22T15:26:01.769602", "2023-12-22T15:26:01.769602");
    Schema schema = SchemaBuilder.builder().stringType();
    for (String input : testCases.keySet()) {
      Timestamp result = AvroToValueMapper.avroFieldToTimestamp(input, schema);
      assertEquals(
          String.format("Test case input : %s", input),
          Timestamp.parseTimestamp(testCases.get(input)),
          result);
    }
  }

  @Test
  public void testAvroToTimestamp_null() {
    assertNull(AvroToValueMapper.avroFieldToTimestamp(null, SchemaBuilder.builder().stringType()));
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToTimestamp() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToTimestamp("asd123456.789", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertLongToTimestamp() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToTimestamp(1234523342, schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertBooleanToTimestamp() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToTimestamp(true, schema);
  }

  @Test
  public void testAvroFieldToDate_valid() {
    Map<String, String> testCases = new HashMap<>();
    testCases.put("2020-12-30T00:00:00Z", "2020-12-30");
    testCases.put("2020-12-30", "2020-12-30");
    testCases.put("2020-12-30T12:12:12Z", "2020-12-30");
    testCases.put("2020-12-30T00:00:00", "2020-12-30");
    testCases.put("2020-12-30T12:12:12Z", "2020-12-30");
    Schema schema = SchemaBuilder.builder().stringType();
    for (String input : testCases.keySet()) {
      Date result = AvroToValueMapper.avroFieldToDate(input, schema);
      assertEquals(
          String.format("Test case input : %s", input),
          com.google.cloud.Date.parseDate(testCases.get(input)),
          result);
    }
  }

  @Test
  public void testAvroToDate_null() {
    assertNull(AvroToValueMapper.avroFieldToDate(null, SchemaBuilder.builder().stringType()));
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToDate("asd123456.789", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToDateEndingWithZ() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToDate("asd123456.789Z", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertLongToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToDate(1234523342, schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertBooleanToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    AvroToValueMapper.avroFieldToDate(true, schema);
  }

  @Test
  public void testAvroValueToArrayBasic() {
    long[] values = {
      Long.MIN_VALUE, 0L, 42L, Long.MAX_VALUE,
    };
    Schema schema = SchemaBuilder.array().items(SchemaBuilder.builder().longType());
    GenericRecord genericRecord =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schema)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    /* Test equivalent type mapping */
    assertThat(
            Value.int64Array(
                avroArrayFieldToSpannerArray(
                    genericRecord.get("arrayField"), schema, AvroToValueMapper::avroFieldToLong)))
        .isEqualTo(Value.int64Array(values));

    assertThat(avroArrayFieldToSpannerArray(null, schema, AvroToValueMapper::avroFieldToLong))
        .isEqualTo(null);

    /*
     * Test cross type mapping.
     */
    assertThat(
            Value.stringArray(
                avroArrayFieldToSpannerArray(
                    genericRecord.get("arrayField"), schema, AvroToValueMapper::avroFieldToString)))
        .isEqualTo(
            Value.stringArray(
                Arrays.stream(values).boxed().map(Object::toString).collect(Collectors.toList())));
    /*
     * Test that every spanner primitive has an array defined.
     */
    assertThat(
            AvroToValueMapper.getGsqlMap().keySet().stream()
                .filter(t -> !t.getCode().equals(Code.ARRAY))
                .map(t -> t.toString())
                .sorted()
                .collect(Collectors.toList()))
        .isEqualTo(
            AvroToValueMapper.getGsqlMap().keySet().stream()
                .filter(t -> t.getCode().equals(Code.ARRAY))
                .map(t -> t.getArrayElementType())
                .map(t -> t.toString())
                .sorted()
                .collect(Collectors.toList()));
  }

  @Test
  public void testAvroValueToArrayTypeBool() {
    boolean[] values = {true, false, true};
    Schema schemaBoolArray = SchemaBuilder.array().items(SchemaBuilder.builder().booleanType());
    GenericRecord genericRecordBool =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaBoolArray)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.bool()))
                .apply(
                    genericRecordBool.get("arrayField"),
                    genericRecordBool.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.boolArray(values));
  }

  @Test
  public void testAvroValueToArrayTypeFloat32() {
    float[] values = {Float.MIN_VALUE, 0.0f, 3.14f, Float.MAX_VALUE};
    Schema schemaFloat32Array = SchemaBuilder.array().items(SchemaBuilder.builder().floatType());
    GenericRecord genericRecordFloat32 =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaFloat32Array)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.float32()))
                .apply(
                    genericRecordFloat32.get("arrayField"),
                    genericRecordFloat32.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.float32Array(values));
  }

  @Test
  public void testAvroValueToArrayTypeFloat64() {
    double[] values = {Double.MIN_VALUE, 0.0, 3.14, Double.MAX_VALUE};
    Schema schemaFloat64Array = SchemaBuilder.array().items(SchemaBuilder.builder().doubleType());
    GenericRecord genericRecordFloat64 =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaFloat64Array)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.float64()))
                .apply(
                    genericRecordFloat64.get("arrayField"),
                    genericRecordFloat64.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.float64Array(values));
  }

  @Test
  public void testAvroValueToArrayTypeString() {
    String[] values = {"hello", "world", "spanner", null};
    Schema schemaStringArray = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
    GenericRecord genericRecordString =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaStringArray)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.string()))
                .apply(
                    genericRecordString.get("arrayField"),
                    genericRecordString.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.stringArray(java.util.Arrays.asList(values)));
  }

  @Test
  public void testAvroValueToJsonArrayOfMaps() {
    JSONArray jsonArray = new JSONArray();

    JSONObject map1 = new JSONObject();
    map1.put("key1", "value1");
    map1.put("key2", "value2");
    jsonArray.put(map1);

    JSONObject map2 = new JSONObject();
    map2.put("keyA", "valueA");
    map2.put("keyB", "valueB");
    jsonArray.put(map2);

    String[] values = new String[] {map1.toString(), map2.toString()};

    Schema schemaStringArray = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
    GenericRecord genericRecordJsonArray =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaStringArray)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();

    java.util.List<String> expectedJsonStrings = new java.util.ArrayList<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      expectedJsonStrings.add(jsonArray.get(i).toString());
    }

    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.json()))
                .apply(
                    genericRecordJsonArray.get("arrayField"),
                    genericRecordJsonArray.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.jsonArray(expectedJsonStrings));
  }

  @Test
  public void testAvroValueToArrayTypeBytes() {
    java.nio.ByteBuffer[] values = {
      java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3}), java.nio.ByteBuffer.wrap(new byte[] {4, 5, 6})
    };
    Schema schemaBytes = SchemaBuilder.array().items(SchemaBuilder.builder().bytesType());
    GenericRecord genericRecordBytes =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaBytes)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    var converted =
        AvroToValueMapper.getGsqlMap()
            .get(Type.array(Type.bytes()))
            .apply(
                genericRecordBytes.get("arrayField"),
                genericRecordBytes.getSchema().getField("arrayField").schema());
    var expected =
        Value.bytesArray(
            Arrays.stream(values).map(b -> ByteArray.copyFrom(b)).collect(Collectors.toList()));
    assertThat(converted).isEqualTo(expected);
  }

  @Test
  public void testAvroValueToArrayTypeDate() {
    java.time.LocalDate[] values = {
      java.time.LocalDate.of(2023, 10, 26), java.time.LocalDate.of(2024, 1, 1)
    };
    Schema schemaDate = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
    GenericRecord genericRecordDate =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaDate)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.date()))
                .apply(
                    genericRecordDate.get("arrayField"),
                    genericRecordDate.getSchema().getField("arrayField").schema()))
        .isEqualTo(
            Value.dateArray(
                Arrays.stream(values)
                    .map(
                        d ->
                            Date.fromYearMonthDay(
                                d.getYear(), d.getMonthValue(), d.getDayOfMonth()))
                    .collect(Collectors.toList())));
  }

  @Test
  public void testAvroValueToNumericArray() {
    BigDecimal[] numericValues =
        new BigDecimal[] {
          new BigDecimal("123.456000000"), new BigDecimal("-789.012000000"), new BigDecimal("0E-9"),
        };

    Schema schemaNumericArray = SchemaBuilder.array().items(SchemaBuilder.builder().bytesType());

    GenericRecord genericRecordNumericArray =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaNumericArray)
                    .noDefault()
                    .endRecord())
            .set("arrayField", numericValues)
            .build();

    List<BigDecimal> expectedNumericList = Arrays.asList(numericValues);

    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.numeric()))
                .apply(
                    genericRecordNumericArray.get("arrayField"),
                    genericRecordNumericArray.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.numericArray(expectedNumericList));
  }

  @Test
  public void testAvroValueToArrayTypeLong() {
    long[] values = {Long.MIN_VALUE, 0L, 42L, Long.MAX_VALUE};
    Schema schemaLong = SchemaBuilder.array().items(SchemaBuilder.builder().longType());
    GenericRecord genericRecordLong =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaLong)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    /* Test equivalent type mapping */
    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.int64()))
                .apply(
                    genericRecordLong.get("arrayField"),
                    genericRecordLong.getSchema().getField("arrayField").schema()))
        .isEqualTo(Value.int64Array(values));
  }

  @Test
  public void testAvroValueToTimestampArray() {
    Instant[] timestampValues =
        new Instant[] {
          Instant.parse("2023-10-26T10:00:00Z"),
          Instant.parse("2024-01-01T00:00:00Z"),
          Instant.parse("2023-11-15T15:30:45Z")
        };

    Schema schemaTimestampArray = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());

    GenericRecord genericRecordTimestampArray =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schemaTimestampArray)
                    .noDefault()
                    .endRecord())
            .set("arrayField", timestampValues)
            .build();

    assertThat(
            AvroToValueMapper.getGsqlMap()
                .get(Type.array(Type.timestamp()))
                .apply(
                    genericRecordTimestampArray.get("arrayField"),
                    genericRecordTimestampArray.getSchema().getField("arrayField").schema()))
        .isEqualTo(
            Value.timestampArray(
                Arrays.stream(timestampValues)
                    .map(i -> Timestamp.ofTimeSecondsAndNanos(i.getEpochSecond(), i.getNano()))
                    .collect(Collectors.toList())));
  }

  @Test
  public void testAvroValueToArrayException() {
    String[] values = {"ABC"};
    Schema schema = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
    GenericRecord genericRecord =
        new GenericRecordBuilder(
                SchemaBuilder.record("payload")
                    .fields()
                    .name("arrayField")
                    .type(schema)
                    .noDefault()
                    .endRecord())
            .set("arrayField", values)
            .build();
    assertThrows(
        AvroTypeConvertorException.class,
        () ->
            avroArrayFieldToSpannerArray(
                genericRecord.get("arrayField"), schema, AvroToValueMapper::avroFieldToLong));
  }
}
