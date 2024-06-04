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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

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
  public void testAvroFieldToString_valid() {
    String result =
        AvroToValueMapper.avroFieldToString("Hello", SchemaBuilder.builder().stringType());
    assertEquals("Hello", result);

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
    byte[] inputValue = {10, 20, 30};
    ByteArray result =
        AvroToValueMapper.avroFieldToByteArray(inputValue, SchemaBuilder.builder().bytesType());
    assertEquals(ByteArray.copyFrom(inputValue), result);
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
}
