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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.*;
import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.handleCassandraTimestampType;
import static org.junit.Assert.*;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.*;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CassandraTypeHandlerTest {

  @Test
  public void convertSpannerValueJsonToBooleanType() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":\"true\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "isAdmin";
    Boolean convertedValue = handleCassandraBoolType(colKey, newValuesJson);
    assertTrue(convertedValue);
  }

  @Test
  public void convertSpannerValueJsonToBooleanType_False() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":\"false\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "isAdmin";
    Boolean convertedValue = handleCassandraBoolType(colKey, newValuesJson);
    Assert.assertFalse(convertedValue);
  }

  @Test
  public void convertSpannerValueJsonToFloatType() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"age\":23.5}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    Float convertedValue = handleCassandraFloatType(colKey, newValuesJson);
    assertEquals(23.5f, convertedValue, 0.01f);
  }

  @Test
  public void convertSpannerValueJsonToDoubleType() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"salary\":100000.75}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "salary";
    Double convertedValue = handleCassandraDoubleType(colKey, newValuesJson);
    assertEquals(100000.75, convertedValue, 0.01);
  }

  @Test
  public void convertSpannerValueJsonToBlobType_FromByteArray() {
    String newValuesString =
        "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"QUJDQDEyMzQ=\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
    byte[] expectedBytes = java.util.Base64.getDecoder().decode("QUJDQDEyMzQ=");
    byte[] actualBytes = new byte[convertedValue.remaining()];
    convertedValue.get(actualBytes);
    Assert.assertArrayEquals(expectedBytes, actualBytes);
  }

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testHandleNullBooleanType() {
    String newValuesString = "{\"isAdmin\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "isAdmin";
      assertEquals(false, handleCassandraBoolType(colKey, newValuesJson));
  }

  @Test
  public void testHandleNullFloatType() {
    String newValuesString = "{\"age\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    assertNull(handleCassandraFloatType(colKey, newValuesJson));
  }

  @Test
  public void testHandleNullDoubleType() {
    String newValuesString = "{\"salary\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "salary";
    Double value = handleCassandraDoubleType(colKey, newValuesJson);
    assertNull(value);
  }

  @Test
  public void testHandleMaxInteger() {
    String newValuesString = "{\"age\":2147483647}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    Integer value = handleCassandraIntType(colKey, newValuesJson);
    assertEquals(Integer.MAX_VALUE, value.longValue());
  }

  @Test
  public void testHandleMinInteger() {
    String newValuesString = "{\"age\":-2147483648}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    Integer value = handleCassandraIntType(colKey, newValuesJson);
    assertEquals(Integer.MIN_VALUE, value.longValue());
  }

  @Test
  public void testHandleMaxLong() {
    String newValuesString = "{\"age\":9223372036854775807}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    Long value = handleCassandraBigintType(colKey, newValuesJson);
    assertEquals(Long.MAX_VALUE, value.longValue());
  }

  @Test
  public void testHandleMinLong() {
    String newValuesString = "{\"age\":-9223372036854775808}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    Long value = handleCassandraBigintType(colKey, newValuesJson);
    assertEquals(Long.MIN_VALUE, value.longValue());
  }

  @Test
  public void testHandleMaxFloat() {
    String newValuesString = "{\"value\":3.4028235E38}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    Float value = handleCassandraFloatType(colKey, newValuesJson);
    assertEquals(Float.MAX_VALUE, value, 0.01f);
  }

  @Test
  public void testHandleMinFloat() {
    String newValuesString = "{\"value\":-3.4028235E38}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    Float value = handleCassandraFloatType(colKey, newValuesJson);
    assertEquals(-Float.MAX_VALUE, value, 0.01f);
  }

  @Test
  public void testHandleMaxDouble() {
    String newValuesString = "{\"value\":1.7976931348623157E308}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    Double value = handleCassandraDoubleType(colKey, newValuesJson);
    assertEquals(Double.MAX_VALUE, value, 0.01);
  }

  @Test
  public void testHandleMinDouble() {
    String newValuesString = "{\"value\":-1.7976931348623157E308}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    Double value = handleCassandraDoubleType(colKey, newValuesJson);
    assertEquals(-Double.MAX_VALUE, value, 0.01);
  }

  @Test
  public void testHandleInvalidIntegerFormat() {
    String newValuesString = "{\"age\":\"invalid_integer\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    handleCassandraIntType(colKey, newValuesJson);
  }

  @Test
  public void testHandleInvalidLongFormat() {
    String newValuesString = "{\"age\":\"invalid_long\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    handleCassandraBigintType(colKey, newValuesJson);
  }

  @Test
  public void testHandleInvalidFloatFormat() {
    String newValuesString = "{\"value\":\"invalid_float\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    handleCassandraFloatType(colKey, newValuesJson);
  }

  @Test
  public void testHandleInvalidDoubleFormat() {
    String newValuesString = "{\"value\":\"invalid_double\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "value";
    handleCassandraDoubleType(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleInvalidBlobFormat() {
    String newValuesString = "{\"data\":\"not_base64\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    handleCassandraBlobType(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleInvalidDateFormat() {
    String newValuesString = "{\"birthdate\":\"invalid_date_format\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "birthdate";
    handleCassandraDateType(colKey, newValuesJson);
  }

  @Test
  public void testHandleNullTextType() {
    String newValuesString = "{\"name\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "name";
    String value = handleCassandraTextType(colKey, newValuesJson);
    assertNull(value);
  }

  @Test
  public void testHandleUnsupportedBooleanType() {
    String newValuesString = "{\"values\":[true, false]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unsupported type for column values");

    handleFloatSetType("values", newValuesJson);
  }

  @Test
  public void testHandleUnsupportedListType() {
    String newValuesString = "{\"values\":[[1, 2], [3, 4]]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unsupported type for column values");

    handleFloatSetType("values", newValuesJson);
  }

  @Test
  public void testHandleUnsupportedMapType() {
    String newValuesString = "{\"values\":[{\"key1\":\"value1\"}, {\"key2\":\"value2\"}]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unsupported type for column values");

    handleFloatSetType("values", newValuesJson);
  }

  @Test
  public void testHandleUnsupportedType() {
    String newValuesString = "{\"values\":[true, false]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unsupported type for column values");

    handleFloatSetType("values", newValuesJson);
  }

  @Test
  public void convertSpannerValueJsonToBlobType_FromBase64() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"QUJDRA==\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
    byte[] expectedBytes = Base64.getDecoder().decode("QUJDRA==");
    byte[] actualBytes = new byte[convertedValue.remaining()];
    convertedValue.get(actualBytes);
    Assert.assertArrayEquals(expectedBytes, actualBytes);
  }

  @Test
  public void convertSpannerValueJsonToBlobType_EmptyString() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
    Assert.assertNotNull(convertedValue);
    assertEquals(0, convertedValue.remaining());
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertSpannerValueJsonToBlobType_InvalidType() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":12345}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    handleCassandraBlobType(colKey, newValuesJson);
  }

  @Test
  public void convertSpannerValueJsonToInvalidFloatType() {
    String newValuesString =
        "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"age\":\"invalid_value\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "age";
    handleCassandraFloatType(colKey, newValuesJson);
  }

    @Test
    public void convertSpannerValueJsonToInvalidDoubleType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"salary\":\"invalid_value\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "salary";
        handleCassandraDoubleType(colKey, newValuesJson);
    }

  @Test
  public void convertSpannerValueJsonToBlobType_MissingColumn() {
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "data";
    ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
    Assert.assertNull(convertedValue);
  }

  @Test
  public void testHandleByteArrayType() {
    String newValuesString = "{\"data\":[\"QUJDRA==\", \"RkZIRg==\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    List<ByteBuffer> value = handleByteArrayType("data", newValuesJson);

    List<ByteBuffer> expected =
        Arrays.asList(
            ByteBuffer.wrap(Base64.getDecoder().decode("QUJDRA==")),
            ByteBuffer.wrap(Base64.getDecoder().decode("RkZIRg==")));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleByteSetType() {
    String newValuesString = "{\"data\":[\"QUJDRA==\", \"RkZIRg==\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<ByteBuffer> value = handleByteSetType("data", newValuesJson);

    Set<ByteBuffer> expected =
        new HashSet<>(
            Arrays.asList(
                ByteBuffer.wrap(Base64.getDecoder().decode("QUJDRA==")),
                ByteBuffer.wrap(Base64.getDecoder().decode("RkZIRg=="))));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleStringArrayType() {
    String newValuesString = "{\"names\":[\"Alice\", \"Bob\", \"Charlie\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    List<String> value = handleStringArrayType("names", newValuesJson);

    List<String> expected = Arrays.asList("Alice", "Bob", "Charlie");
    assertEquals(expected, value);
  }

  @Test
  public void testHandleStringSetType() {
    String newValuesString = "{\"names\":[\"Alice\", \"Bob\", \"Alice\", \"Charlie\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<String> valueList = handleStringSetType("names", newValuesJson);
    HashSet<String> value = new HashSet<>(valueList);
    HashSet<String> expected = new HashSet<>(Arrays.asList("Alice", "Bob", "Charlie"));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleBoolSetTypeString() {
    String newValuesString = "{\"flags\":[\"true\", \"false\", \"true\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<Boolean> value = handleBoolSetTypeString("flags", newValuesJson);

    Set<Boolean> expected = new HashSet<>(Arrays.asList(true, false));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleFloatArrayType() {
    String newValuesString = "{\"values\":[1.1, 2.2, 3.3]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    List<Float> value = handleFloatArrayType("values", newValuesJson);

    List<Float> expected = Arrays.asList(1.1f, 2.2f, 3.3f);
    assertEquals(expected, value);
  }

  @Test
  public void testHandleFloatSetType() {
    String newValuesString = "{\"values\":[1.1, 2.2, 3.3, 2.2]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<Float> value = handleFloatSetType("values", newValuesJson);

    Set<Float> expected = new HashSet<>(Arrays.asList(1.1f, 2.2f, 3.3f));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleFloatSetType_InvalidString() {
    String newValuesString = "{\"values\":[\"1.1\", \"2.2\", \"abc\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    try {
      handleFloatSetType("values", newValuesJson);
      fail("Expected IllegalArgumentException for invalid number format");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid number format for column values"));
    }
  }

  @Test
  public void testHandleFloat64ArrayType() {
    String newValuesString = "{\"values\":[1.1, \"2.2\", 3.3]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    List<Double> value = handleFloat64ArrayType("values", newValuesJson);

    List<Double> expected = Arrays.asList(1.1, 2.2, 3.3);
    assertEquals(expected, value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleFloat64ArrayTypeInvalid() {
    String newValuesString = "{\"values\":[\"1.1\", \"abc\", \"3.3\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    handleFloat64ArrayType("values", newValuesJson);
  }

  @Test
  public void testHandleDateSetType() {
    String newValuesString = "{\"dates\":[\"2024-12-05\", \"2024-12-06\"]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<LocalDate> value = handleDateSetType("dates", newValuesJson);
    Set<LocalDate> expected =
        new HashSet<>(Arrays.asList(LocalDate.of(2024, 12, 5), LocalDate.of(2024, 12, 6)));
    assertEquals(expected, value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleFloat64ArrayType_WithUnsupportedList() {
    String jsonStr = "{\"colName\": [[1, 2, 3], [4, 5, 6]]}";
    JSONObject valuesJson = new JSONObject(jsonStr);
    CassandraTypeHandler.handleFloat64ArrayType("colName", valuesJson);
  }

  @Test
  public void testHandleInt64SetType_ValidLongValues() {
    String newValuesString = "{\"numbers\":[1, 2, 3, 4]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<Long> result = handleInt64SetType("numbers", newValuesJson);
    Set<Long> expected = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L));
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraIntType_ValidInteger() {
    String newValuesString = "{\"age\":1234}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Integer result = handleCassandraIntType("age", newValuesJson);
    Integer expected = 1234;
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraBigintType_ValidConversion() {
    String newValuesString = "{\"age\":1234567890123}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Long result = handleCassandraBigintType("age", newValuesJson);
    Long expected = 1234567890123L;
    assertEquals(expected, result);
  }

  @Test
  public void testHandleInt64ArrayAsInt32Array() {
    String newValuesString = "{\"values\":[1, 2, 3, 4]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    List<Integer> value = handleInt64ArrayAsInt32Array("values", newValuesJson);

    List<Integer> expected = Arrays.asList(1, 2, 3, 4);
    assertEquals(expected, value);
  }

  @Test
  public void testHandleInt64ArrayAsInt32Set() {
    String newValuesString = "{\"values\":[1, 2, 3, 2]}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    Set<Integer> value = handleInt64ArrayAsInt32Set("values", newValuesJson);

    Set<Integer> expected = new HashSet<>(Arrays.asList(1, 2, 3));
    assertEquals(expected, value);
  }

  @Test
  public void testHandleCassandraUuidTypeNull() {
    String newValuesString = "{\"uuid\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    UUID value = handleCassandraUuidType("uuid", newValuesJson);
    Assert.assertNull(value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleCassandraTimestampInvalidFormat() {
    String newValuesString = "{\"createdAt\":\"2024-12-05 10:15:30.123\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    handleCassandraTimestampType("createdAt", newValuesJson);
  }

  @Test
  public void testHandleCassandraTimestampInvalidFormatColNull() {
    String newValuesString = "{\"createdAt\":\"2024-12-05 10:15:30.123\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    handleCassandraTimestampType(null, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleCassandraDateInvalidFormat() {
    String newValuesString = "{\"birthdate\":\"2024/12/05\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    handleCassandraDateType("birthdate", newValuesJson);
  }

  @Test
  public void testHandleCassandraTextTypeNull() {
    String newValuesString = "{\"name\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String value = handleCassandraTextType("name", newValuesJson);
    Assert.assertNull(value);
  }

  @Test
  public void testHandleBoolArrayType_ValidBooleanStrings() {
    String jsonStr = "{\"colName\": [\"true\", \"false\", \"true\"]}";
    JSONObject valuesJson = new JSONObject(jsonStr);
    List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
    assertEquals(3, result.size());
    assertTrue(result.get(0));
    assertFalse(result.get(1));
    assertTrue(result.get(2));
  }

  @Test
  public void testHandleBoolArrayType_InvalidBooleanStrings() {
    String jsonStr = "{\"colName\": [\"yes\", \"no\", \"true\"]}";
    JSONObject valuesJson = new JSONObject(jsonStr);
    List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
    assertEquals(3, result.size());
    assertFalse(result.get(0));
    assertFalse(result.get(1));
    assertTrue(result.get(2));
  }

  @Test
  public void testHandleBoolArrayType_EmptyArray() {
    String jsonStr = "{\"colName\": []}";
    JSONObject valuesJson = new JSONObject(jsonStr);
    List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testHandleTimestampSetType_validArray() {
    String jsonString =
        "{\"timestamps\": [\"2024-12-04T12:34:56.123Z\", \"2024-12-05T13:45:00.000Z\"]}";
    JSONObject valuesJson = new JSONObject(jsonString);

    Set<Timestamp> result = CassandraTypeHandler.handleTimestampSetType("timestamps", valuesJson);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.contains(Timestamp.valueOf("2024-12-04 00:00:00.0")));
    assertTrue(result.contains(Timestamp.valueOf("2024-12-05 00:00:00.0")));
  }

  @Test
  public void testHandleValidAsciiString() {
    String newValuesString = "{\"name\":\"JohnDoe\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "name";
    assertEquals("JohnDoe", handleCassandraAsciiType(colKey, newValuesJson));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleNonAsciiString() {
    String newValuesString = "{\"name\":\"JoãoDoe\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "name";
    handleCassandraAsciiType(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleNullForAsciiColumn() {
    String newValuesString = "{\"name\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "name";
    handleCassandraAsciiType(colKey, newValuesJson);
  }

  @Test
  public void testHandleValidStringVarint() {
    String newValuesString = "{\"amount\":\"123456789123456789\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "amount";
    BigInteger expected = new BigInteger("123456789123456789");
    assertEquals(expected, handleCassandraVarintType(colKey, newValuesJson));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleInvalidStringVarint() {
    String newValuesString = "{\"amount\":\"abcxyz\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "amount";
    handleCassandraVarintType(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleInvalidTypeVarint() {
    String newValuesString = "{\"amount\":12345}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "amount";
    handleCassandraVarintType(colKey, newValuesJson);
  }

  @Test
  public void testHandleValidDuration() {
    String newValuesString = "{\"duration\":\"P1DT1H\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "duration";
    Duration expected = Duration.parse("P1DT1H");
    assertEquals(expected, handleCassandraDurationType(colKey, newValuesJson));
  }

  @Test
  public void testHandleNullDuration() {
    String newValuesString = "{\"duration\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "duration";
    assertNull(handleCassandraDurationType(colKey, newValuesJson));
  }

  @Test
  public void testHandleMissingColumnKey() {
    String newValuesString = "{\"otherColumn\":\"P1DT1H\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "duration";
    assertNull(handleCassandraDurationType(colKey, newValuesJson));
  }

  @Test
  public void testHandleValidIPv4Address() throws UnknownHostException {
    String newValuesString = "{\"ipAddress\":\"192.168.0.1\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "ipAddress";
    InetAddress expected = InetAddress.getByName("192.168.0.1");
    assertEquals(expected, handleCassandraInetAddressType(colKey, newValuesJson));
  }

  @Test
  public void testHandleValidIPv6Address() throws UnknownHostException {
    String newValuesString = "{\"ipAddress\":\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "ipAddress";
    InetAddress expected = InetAddress.getByName("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
    assertEquals(expected, handleCassandraInetAddressType(colKey, newValuesJson));
  }

  @Test(expected = UnknownHostException.class)
  public void testHandleInvalidIPAddressFormat() throws UnknownHostException {
    String newValuesString = "{\"ipAddress\":\"invalid-ip-address\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "ipAddress";
    handleCassandraInetAddressType(colKey, newValuesJson);
  }

  @Test
  public void testHandleEmptyStringIPAddress() throws UnknownHostException {
    String newValuesString = "{\"ipAddress\":\"\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String colKey = "ipAddress";
    handleCassandraInetAddressType(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleInvalidStringifiedJson() {
    String newValuesString = "{\"user\":{\"name\":\"John\", \"age\":30";
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("data", newValuesString);
    String colKey = "data";
    handleStringifiedJsonToMap(colKey, newValuesJson);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHandleNonStringValue() {
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("data", 12345);
    String colKey = "data";
    handleStringifiedJsonToMap(colKey, newValuesJson);
  }

  @Test
  public void testHandleValidStringifiedJsonArray() {
    String newValuesString = "[\"apple\", \"banana\", \"cherry\"]";
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("data", newValuesString);
    String colKey = "data";

    Set<Object> expected = new HashSet<>();
    expected.add("apple");
    expected.add("banana");
    expected.add("cherry");
    assertEquals(expected, handleStringifiedJsonToSet(colKey, newValuesJson));
  }

  @Test
  public void testHandleEmptyStringifiedJsonArray() {
    String newValuesString = "[]";
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("data", newValuesString);
    String colKey = "data";
    Set<Object> expected = new HashSet<>();
    assertEquals(expected, handleStringifiedJsonToSet(colKey, newValuesJson));
  }

  @Test
  public void testHandleNonArrayValue() {
    String newValuesString = "\"apple\"";
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("data", newValuesString);
    String colKey = "data";
    assertThrows(IllegalArgumentException.class, () -> handleStringifiedJsonToSet(colKey, newValuesJson));
  }

  @Test
  public void testConvertToSmallIntValidInput() {
    Integer validValue = 100;
    short result = convertToSmallInt(validValue);
    assertEquals(100, result);
  }

  @Test
  public void testConvertToSmallIntBelowMinValue() {
    Integer invalidValue = Short.MIN_VALUE - 1;
    assertThrows(IllegalArgumentException.class, () -> convertToSmallInt(invalidValue));
  }

  @Test
  public void testConvertToSmallIntAboveMaxValue() {
    Integer invalidValue = Short.MAX_VALUE + 1;
    assertThrows(IllegalArgumentException.class, () -> convertToSmallInt(invalidValue));
  }

  @Test
  public void testConvertToTinyIntValidInput() {
    Integer validValue = 100;
    byte result = convertToTinyInt(validValue);
    assertEquals(100, result);
  }

  @Test
  public void testConvertToTinyIntBelowMinValue() {
    Integer invalidValue = Byte.MIN_VALUE - 1;
    assertThrows(IllegalArgumentException.class, () -> convertToTinyInt(invalidValue));
  }

  @Test
  public void testConvertToTinyIntAboveMaxValue() {
    Integer invalidValue = Byte.MAX_VALUE + 1;
    assertThrows(IllegalArgumentException.class, () -> convertToTinyInt(invalidValue));
  }

  @Test
  public void testEscapeCassandraStringNoQuotes() {
    String input = "Hello World";
    String expected = "Hello World";
    String result = escapeCassandraString(input);
    assertEquals(expected, result);
  }

  @Test
  public void testEscapeCassandraStringWithSingleQuote() {
    String input = "O'Reilly";
    String expected = "O''Reilly";
    String result = escapeCassandraString(input);
    assertEquals(expected, result);
  }

  @Test
  public void testEscapeCassandraStringEmpty() {
    String input = "";
    String expected = "";
    String result = escapeCassandraString(input);
    assertEquals(expected, result);
  }

  @Test
  public void testEscapeCassandraStringWithMultipleQuotes() {
    String input = "It's John's book.";
    String expected = "It''s John''s book.";
    String result = escapeCassandraString(input);
    assertEquals(expected, result);
  }

  @Test
  public void testConvertToCassandraTimestampWithValidOffset() {
    String value = "2024-12-12T10:15:30+02:00";
    String timezoneOffset = "+00:00";
    String expected = "'2024-12-12T08:15:30Z'";
    String result = convertToCassandraTimestamp(value, timezoneOffset);
    assertEquals(expected, result);
  }

  @Test(expected = RuntimeException.class)
  public void testConvertToCassandraTimestampWithInvalidFormat() {
    String value = "2024-12-12T25:15:30+02:00";
    String timezoneOffset = "+00:00";
    convertToCassandraTimestamp(value, timezoneOffset);
  }

  @Test
  public void testConvertToCassandraTimestampWithoutTimezone() {
    String value = "2024-12-12T10:15:30Z";
    String timezoneOffset = "+00:00";
    String expected = "'2024-12-12T10:15:30Z'";
    String result = convertToCassandraTimestamp(value, timezoneOffset);
    assertEquals(expected, result);
  }
  @Test
  public void testConvertToCassandraDateWithValidDate() {
    String dateString = "2024-12-12T10:15:30Z";
    LocalDate result = convertToCassandraDate(dateString);
    LocalDate expected = LocalDate.of(2024, 12, 12);
    assertEquals(expected, result);
  }

  @Test
  public void testConvertToCassandraDateLeapYear() {
    String dateString = "2024-02-29T00:00:00Z";
    LocalDate result = convertToCassandraDate(dateString);
    LocalDate expected = LocalDate.of(2024, 2, 29);
    assertEquals(expected, result);
  }

  @Test
  public void testConvertToCassandraDateWithDifferentTimeZone() {
    String dateString = "2024-12-12T10:15:30+02:00";
    LocalDate result = convertToCassandraDate(dateString);
    LocalDate expected = LocalDate.of(2024, 12, 12);
    assertEquals(expected, result);
  }

  @Test(expected = DateTimeParseException.class)
  public void testConvertToCassandraDateWithInvalidDate() {
    String dateString = "2024-13-12T10:15:30Z";
    convertToCassandraDate(dateString);
  }

  @Test
  public void testConvertToCassandraTimestampWithValidDate() {
    String dateString = "2024-12-12T10:15:30Z";
    Instant result = convertToCassandraTimestamp(dateString);
    Instant expected = Instant.parse(dateString);
    assertEquals(expected, result);
  }

  @Test
  public void testConvertToCassandraTimestampWithTimezoneOffset() {
    String dateString = "2024-12-12T10:15:30+02:00";
    Instant result = convertToCassandraTimestamp(dateString);
    Instant expected = Instant.parse("2024-12-12T08:15:30Z");
    assertEquals(expected, result);
  }

  @Test
  public void testConvertToCassandraTimestampLeapYear() {
    String dateString = "2024-02-29T00:00:00Z";
    Instant result = convertToCassandraTimestamp(dateString);
    Instant expected = Instant.parse(dateString);
    assertEquals(expected, result);
  }

  @Test(expected = DateTimeParseException.class)
  public void testConvertToCassandraTimestampWithInvalidDate() {
    String dateString = "2024-13-12T10:15:30Z";
    convertToCassandraTimestamp(dateString);
  }

  @Test
  public void testIsValidUUIDWithValidUUID() {
    String validUUID = "123e4567-e89b-12d3-a456-426614174000";
    boolean result = isValidUUID(validUUID);
    assertTrue(result);
  }

  @Test
  public void testIsValidUUIDWithInvalidUUID() {
    String invalidUUID = "123e4567-e89b-12d3-a456-426614174000Z";
    boolean result = isValidUUID(invalidUUID);
    assertFalse(result);
  }

  @Test
  public void testIsValidUUIDWithEmptyString() {
    String emptyString = "";
    boolean result = isValidUUID(emptyString);
    assertFalse(result);
  }

  @Test
  public void testIsValidIPAddressWithValidIPv4() {
    String validIPv4 = "192.168.1.1";
    boolean result = isValidIPAddress(validIPv4);
    assertTrue(result);
  }

  @Test
  public void testIsValidIPAddressWithValidIPv6() {
    String validIPv6 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
    boolean result = isValidIPAddress(validIPv6);
    assertTrue(result);
  }

  @Test
  public void testIsValidIPAddressWithInvalidFormat() {
    String invalidIP = "999.999.999.999";
    boolean result = isValidIPAddress(invalidIP);
    assertFalse(result);
  }

  @Test
  public void testIsValidJSONWithValidJSON() {
    String validJson = "{\"name\":\"John\", \"age\":30}";
    boolean result = isValidJSON(validJson);
    assertTrue(result);
  }

  @Test
  public void testIsValidJSONWithInvalidJSON() {
    String invalidJson = "{\"name\":\"John\", \"age\":30";
    boolean result = isValidJSON(invalidJson);
    assertFalse(result);
  }

  @Test
  public void testIsValidJSONWithEmptyString() {
    String emptyString = "";
    boolean result = isValidJSON(emptyString);
    assertFalse(result);
  }

  @Test
  public void testIsValidJSONWithNull() {
    String nullString = null;
    boolean result = isValidJSON(nullString);
    assertFalse(result);
  }

}
