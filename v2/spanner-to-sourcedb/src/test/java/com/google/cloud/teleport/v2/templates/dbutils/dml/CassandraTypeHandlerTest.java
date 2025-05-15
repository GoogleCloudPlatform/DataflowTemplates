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

import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.castToExpectedType;
import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.getColumnValueByType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.common.net.InetAddresses;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CassandraTypeHandlerTest {

  @Test
  public void testGetColumnValueByTypeForString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("varchar", null, null);
    String columnName = "test_column";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "test_value");
    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("test_value", castResult);
  }

  @Test
  public void testGetColumnValueByType() {
    String spannerColumnType = "string";
    String sourceType = "varchar";
    SpannerColumnType spannerType = new SpannerColumnType(spannerColumnType, false);
    SourceColumnType sourceColumnType = new SourceColumnType(sourceType, null, null);
    String columnValue = "é";
    String columnName = "LastName";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("é", castResult);
  }

  @Test
  public void testGetColumnValueByTypeForNumericToInt() {
    String spannerColumnName = "NUMERIC";
    String sourceColumnName = "int";
    SpannerColumnType spannerType = new SpannerColumnType(spannerColumnName, false);
    SourceColumnType sourceColumnType = new SourceColumnType(sourceColumnName, null, null);
    String columnName = "Salary";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 12345);
    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(12345, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringUUID() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("uuid", null, null);
    String columnName = "id";
    String columnValue = "123e4567-e89b-12d3-a456-426614174000";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(UUID.fromString(columnValue), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringIpAddress() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("inet", null, null);
    String columnValue = "192.168.1.1";
    String columnName = "ipAddress";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(InetAddresses.forString(columnValue), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonArray() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("set<text>", null, null);
    String columnValue = "[\"apple\", \"banana\", \"cherry\"]";
    String columnName = "fruits";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    Set<String> expectedSet = new HashSet<>(Arrays.asList("apple", "banana", "cherry"));
    assertEquals(expectedSet, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonObject() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("map<text, text>", null, null);
    String columnName = "user";
    String columnValue = "{\"name\": \"John\", \"age\": \"30\"}";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("name", "John");
    expectedMap.put("age", "30");
    assertEquals(expectedMap, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringHex() {
    SpannerColumnType spannerType = new SpannerColumnType("bytes", false);
    SourceColumnType sourceColumnType = new SourceColumnType("blob", null, null);
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    String columnValue = Base64.getEncoder().encodeToString(expectedBytes);
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    byte[] actualBytes;
    if (castResult instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) castResult;
      actualBytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(actualBytes);
    } else if (castResult instanceof byte[]) {
      actualBytes = (byte[]) castResult;
    } else {
      throw new AssertionError("Unexpected type for castResult");
    }
    assertArrayEquals(expectedBytes, actualBytes);
  }

  @Test
  public void testColumnKeyNotPresent() {
    SpannerColumnType spannerType = new SpannerColumnType("bytes", false);
    SourceColumnType sourceColumnType = new SourceColumnType("blob", null, null);
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    StringBuilder binaryString = new StringBuilder();
    for (byte b : expectedBytes) {
      binaryString.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
    }
    String columnValue = binaryString.toString();
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put("random", columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertEquals("blob", result.dataType());
    assertEquals(CassandraTypeHandler.NullClass.INSTANCE, result.value());
  }

  @Test
  public void testGetColumnValueByTypeForStringByteArrayBase64Encode() {
    SpannerColumnType spannerType = new SpannerColumnType("bytes", false);
    SourceColumnType sourceColumnType = new SourceColumnType("blob", null, null);
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};

    String columnValue = java.util.Base64.getEncoder().encodeToString(expectedBytes);
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    byte[] actualBytes;
    if (castResult instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) castResult;
      actualBytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(actualBytes);
    } else if (castResult instanceof byte[]) {
      actualBytes = (byte[]) castResult;
    } else {
      throw new AssertionError("Unexpected type for castResult");
    }
    assertArrayEquals(expectedBytes, actualBytes);
  }

  @Test
  public void testGetColumnValueByTypeForBlobEncodeInStringHexToBlob() {
    SpannerColumnType spannerType = new SpannerColumnType("bytes", false);
    SourceColumnType sourceColumnType = new SourceColumnType("blob", null, null);
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    String columnValue = Base64.getEncoder().encodeToString(expectedBytes);
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    byte[] actualBytes;
    if (castResult instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) castResult;
      actualBytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(actualBytes);
    } else if (castResult instanceof byte[]) {
      actualBytes = (byte[]) castResult;
    } else {
      throw new AssertionError("Unexpected type for castResult");
    }
    assertArrayEquals(expectedBytes, actualBytes);
  }

  @Test
  public void testGetColumnValueByTypeForStringDuration() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColumnType = new SourceColumnType("duration", null, null);
    String columnValue = "P4DT1H";
    String columnName = "total_time";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(CqlDuration.from("P4DT1H"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDates() {
    SpannerColumnType spannerType = new SpannerColumnType("date", false);
    SourceColumnType sourceColumnType = new SourceColumnType("timestamp", null, null);
    String columnValue = "2025-01-01T00:00:00Z";
    String columnName = "created_on";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    ZonedDateTime expectedDate = ZonedDateTime.parse(columnValue).withSecond(0).withNano(0);
    Instant instant = (Instant) castResult;
    ZonedDateTime actualDate = instant.atZone(ZoneOffset.UTC).withSecond(0).withNano(0);
    assertEquals(expectedDate, actualDate);
  }

  @Test
  public void testGetColumnValueByTypeForBigInt() {
    SpannerColumnType spannerType = new SpannerColumnType("bigint", false);
    SourceColumnType sourceColumnType = new SourceColumnType("bigint", null, null);
    String columnName = "Salary";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(123456789L));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    Long expectedBigInt = 123456789L;

    assertEquals(expectedBigInt, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBytesForHexString() {
    SpannerColumnType spannerType = new SpannerColumnType("String", false);
    SourceColumnType sourceColumnType = new SourceColumnType("bytes", null, null);
    String columnName = "Name";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "48656c6c6f20576f726c64");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("48656c6c6f20576f726c64", castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBigIntForString() {
    SpannerColumnType spannerType = new SpannerColumnType("String", false);
    SourceColumnType sourceColumnType = new SourceColumnType("bigint", null, null);
    String columnName = "Salary";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "123456789");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    long expectedValue = 123456789L;
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBoolentForString() {
    SpannerColumnType spannerType = new SpannerColumnType("String", false);
    SourceColumnType sourceColumnType = new SourceColumnType("boolean", null, null);
    String columnName = "Male";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "1");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(true, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBoolent() {
    SpannerColumnType spannerType = new SpannerColumnType("Boolean", false);
    SourceColumnType sourceColumnType = new SourceColumnType("boolean", null, null);
    String columnName = "Male";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, false);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(false, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForIntegerValue() {
    SpannerColumnType spannerType = new SpannerColumnType("Integer", false);
    SourceColumnType sourceColumnType = new SourceColumnType("bigint", null, null);
    String columnName = "Salary";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 225000);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    long expectedValue = 225000L;
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBoolentSamllCaseForString() {
    SpannerColumnType spannerType = new SpannerColumnType("String", false);
    SourceColumnType sourceColumnType = new SourceColumnType("boolean", null, null);
    String columnName = "Male";
    String sourceDbTimezoneOffset = null;

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColumnType);

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "f");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(false, castResult);
  }

  // Revised and Improved Tests

  @Test
  public void testGetColumnValueByTypeForInteger() {
    SpannerColumnType spannerType = new SpannerColumnType("NUMERIC", false);
    SourceColumnType sourceColType = new SourceColumnType("integer", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(5));

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(BigInteger.valueOf(5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForValidBigInteger() {
    SpannerColumnType spannerType = new SpannerColumnType("integer", false);
    SourceColumnType sourceColType = new SourceColumnType("int64", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(5));

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(BigInteger.valueOf(5), castResult);
  }

  @Test
  public void testConvertToCassandraTimestampWithISOInstant() {
    String timestamp = "2025-01-15T10:15:30Z";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    LocalDate expectedValue = Instant.parse(timestamp).atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testConvertToCassandraTimestampWithISODateTime() {
    String timestamp = "2025-01-15T10:15:30";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("datetime", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15T00:00:00Z", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithISODate() {
    String timestamp = "2025-01-15";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(timestamp, castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat1() {
    String timestamp = "01/15/2025";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat2() {
    String timestamp = "2025/01/15";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat3() {
    String timestamp = "15-01-2025";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat4() {
    String timestamp = "15/01/2025";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat5() {
    String timestamp = "2025-01-15 10:15:30";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithInvalidFormat() {
    String timestamp = "invalid-timestamp";
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PreparedStatementValueObject result =
              getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);
          CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
        });
  }

  @Test
  public void testConvertToCassandraTimestampWithNull() {
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, " ");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              PreparedStatementValueObject result =
                  getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);
              CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testConvertToCassandraTimestampWithWhitespaceString() {
    SpannerColumnType spannerType = new SpannerColumnType("timestamp", false);
    SourceColumnType sourceColType = new SourceColumnType("date", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "   ");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              PreparedStatementValueObject result =
                  getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);
              CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testGetColumnValueByTypeForFloat() {
    SpannerColumnType spannerType = new SpannerColumnType("float", false);
    SourceColumnType sourceColType = new SourceColumnType("float", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5f);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object actualValue = ((PreparedStatementValueObject<?>) result).value();
    assertEquals(5.5f, actualValue);
  }

  @Test
  public void testGetColumnValueByTypeForFloatIllegalArgumentException() {
    SpannerColumnType spannerType = new SpannerColumnType("date", false);
    SourceColumnType sourceColType = new SourceColumnType("float", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "2024-12-12");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PreparedStatementValueObject<?> preparedStatementValueObject =
              getColumnValueByType(spannerColDef, sourceColDef, valuesJson, null);
          castToExpectedType(
              preparedStatementValueObject.dataType(), preparedStatementValueObject.value());
        });
  }

  @Test
  public void testGetColumnValueByTypeForFloat64() {
    SpannerColumnType spannerType = new SpannerColumnType("float64", false);
    SourceColumnType sourceColType = new SourceColumnType("double", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, new BigDecimal("5.5"));

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(5.5, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForFloat64FromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("double", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(5.5, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("decimal", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromFloat() {
    SpannerColumnType spannerType = new SpannerColumnType("float", false);
    SourceColumnType sourceColType = new SourceColumnType("decimal", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromFloat64() {
    SpannerColumnType spannerType = new SpannerColumnType("float64", false);
    SourceColumnType sourceColType = new SourceColumnType("decimal", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5);

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForFloatFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("float", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(5.5f, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBigIntFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("bigint", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(Long.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForIntFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("int", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(Integer.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForSmallIntFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("smallint", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(Integer.valueOf("5").shortValue(), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForTinyIntFromString() {
    SpannerColumnType spannerType = new SpannerColumnType("string", false);
    SourceColumnType sourceColType = new SourceColumnType("tinyint", null, null);
    String columnName = "test_column";

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(Byte.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBytes() {
    SpannerColumnType spannerType = new SpannerColumnType("bytes", false);
    SourceColumnType sourceColType = new SourceColumnType("bytes", null, null);
    String columnName = "test_column";

    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, Base64.getEncoder().encodeToString(expectedBytes));

    SpannerColumnDefinition spannerColDef = new SpannerColumnDefinition(columnName, spannerType);
    SourceColumnDefinition sourceColDef = new SourceColumnDefinition(columnName, sourceColType);

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertTrue(result instanceof PreparedStatementValueObject);

    Object actualValue = ((PreparedStatementValueObject<?>) result).value();
    assertEquals(ByteBuffer.wrap(expectedBytes), actualValue);
  }

  @Test
  public void testCastToExpectedTypeForVariousTypes() throws UnknownHostException {
    assertEquals("Test String", castToExpectedType("text", "Test String"));
    assertEquals(123L, castToExpectedType("bigint", "123"));
    assertEquals(true, castToExpectedType("boolean", "true"));
    assertEquals(
        new BigDecimal("123.456"),
        castToExpectedType("decimal", new BigDecimal("123.456").toString()));
    assertEquals(123.456, castToExpectedType("double", "123.456"));
    assertEquals(123.45f, castToExpectedType("float", "123.45"));
    assertEquals(InetAddress.getByName("127.0.0.1"), castToExpectedType("inet", "127.0.0.1"));
    assertEquals(123, castToExpectedType("int", "123"));
    assertEquals((short) 123, castToExpectedType("smallint", "123"));
    assertEquals(
        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
        castToExpectedType("uuid", "123e4567-e89b-12d3-a456-426614174000"));
    assertEquals((byte) 100, castToExpectedType("tinyint", "100"));
    assertEquals(
        new BigInteger("123456789123456789123456789"),
        castToExpectedType("varint", "123456789123456789123456789"));
    String timeString = "14:30:45";
    Object localTime1 = castToExpectedType("time", "14:30:45");
    assertTrue(localTime1 instanceof LocalTime);
    assertEquals(
        CqlDuration.from("5h"), castToExpectedType("duration", CqlDuration.from("5h").toString()));
  }

  @Test
  public void testCastToExpectedTypeForJSONArrayStringifyToSet() {
    String cassandraType = "set<int>";
    String columnValue = "[1, 2, 3]";
    Object result = castToExpectedType(cassandraType, columnValue);
    assertTrue(result instanceof Set);
    assertEquals(3, ((Set<?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForJSONObjectStringifyToMap() {
    String cassandraType = "map<int, text>";
    String columnValue = "{\"2024-12-12\": \"One\", \"2\": \"Two\"}";
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          castToExpectedType(cassandraType, columnValue);
        });
  }

  @Test
  public void testCastToExpectedTypeForJSONObjectStringifyToMFrozenMap() {
    String cassandraType = "frozen<map<int, text>>";
    String columnValue = "{\"1\": \"One\", \"2\": \"Two\"}";
    Object castResult = castToExpectedType(cassandraType, columnValue);
    assertTrue(castResult instanceof Map);
    assertTrue(((Map<?, ?>) castResult).containsKey(1));
    assertTrue(((Map<?, ?>) castResult).containsKey(2));
    assertEquals("One", ((Map<?, ?>) castResult).get(1));
    assertEquals("Two", ((Map<?, ?>) castResult).get(2));
  }

  @Test
  public void testCastToExpectedTypeForExceptionScenario() {
    String cassandraType = "int";
    String columnValue = "InvalidInt";
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          castToExpectedType(cassandraType, columnValue);
        });
  }

  @Test
  public void testGetColumnValueByTypeForNullBothColumnDefs() {
    JSONObject valuesJson = mock(JSONObject.class);
    String sourceDbTimezoneOffset = null;
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          getColumnValueByType(null, null, valuesJson, sourceDbTimezoneOffset);
        });
  }

  @Test
  public void testCastToExpectedTypeForAscii() {
    String expected = "test string";
    Object result = CassandraTypeHandler.castToExpectedType("ascii", expected);
    assertEquals(expected, result);
  }

  @Test
  public void testCastToExpectedTypeForVarchar() {
    String expected = "test varchar";
    Object result = CassandraTypeHandler.castToExpectedType("varchar", expected);
    assertEquals(expected, result);
  }

  @Test
  public void testCastToExpectedTypeForList() {
    JSONArray listValue = new JSONArray(Arrays.asList("value1", "value2"));
    Object result = CassandraTypeHandler.castToExpectedType("list<text>", listValue.toString());
    assertTrue(result instanceof List);
    assertEquals(2, ((List<?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForSet() {
    JSONArray setValue = new JSONArray(Arrays.asList("value1", "value2"));
    Object result = CassandraTypeHandler.castToExpectedType("set<text>", setValue.toString());
    assertTrue(result instanceof Set);
    assertEquals(2, ((Set<?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForInvalidType() {
    Object object = CassandraTypeHandler.castToExpectedType("unknownType", new Object());
    assertNotNull(object);
  }

  @Test
  public void testCastToExpectedTypeForDate_String() {
    String dateString = "2025-01-09"; // Format: yyyy-MM-dd
    Object result = CassandraTypeHandler.castToExpectedType("date", dateString);
    LocalDate expected = LocalDate.parse(dateString);
    assertEquals(expected, result);
  }

  @Test
  public void testCastToExpectedTypeForDate_InvalidString() {
    String invalidDateString = "invalid-date";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              CassandraTypeHandler.castToExpectedType("date", invalidDateString);
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testCastToExpectedTypeForDate_UnsupportedType() {
    Integer unsupportedType = 123;
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              CassandraTypeHandler.castToExpectedType("date", unsupportedType);
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testHandleCassandraVarintType_String() {
    String validString = "12345678901234567890";
    Object result = CassandraTypeHandler.castToExpectedType("varint", validString);
    BigInteger expected = new BigInteger(validString);
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraVarintType_ForBytesArray() {
    byte[] byteArray = new byte[] {0, 0, 0, 0, 0, 0, 0, 10};
    BigInteger expected = new BigInteger(byteArray);
    Object result = CassandraTypeHandler.castToExpectedType("varint", byteArray);
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraVarintType_ForByteBuffer() {
    byte[] byteArray = new byte[] {0, 0, 0, 0, 0, 0, 0, 10};
    BigInteger expected = new BigInteger(byteArray);
    Object result = CassandraTypeHandler.castToExpectedType("varint", ByteBuffer.wrap(byteArray));
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraVarintType_ForInteger() {
    Long inputValue = 123456789L;
    Object result = CassandraTypeHandler.castToExpectedType("varint", inputValue);
    assertEquals(BigInteger.valueOf(inputValue), result);
  }

  @Test
  public void testHandleCassandraVarintType_InvalidString() {
    String invalidString = "invalid-number";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              CassandraTypeHandler.castToExpectedType("varint", invalidString);
            });
    assertEquals("Error converting value for cassandraType: varint", exception.getMessage());
  }

  @Test
  public void testHandleCassandraVarintType_UnsupportedType() {
    String unsupportedType = "dsdsdd";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              CassandraTypeHandler.castToExpectedType("varint", unsupportedType);
            });
    assertEquals("Error converting value for cassandraType: varint", exception.getMessage());
  }

  @Test
  public void testCastToExpectedTypeForNullDate() {
    Object result = CassandraTypeHandler.castToExpectedType("date", null);
    assertNull(result);
  }

  @Test
  public void testCastToExpectedTypeForNullList() {
    Object result = CassandraTypeHandler.castToExpectedType("list<text>", null);
    assertNull(result);
  }

  @Test
  public void testCastToExpectedTypeForNullSet() {
    Object result = CassandraTypeHandler.castToExpectedType("set<text>", null);
    assertNull(result);
  }

  @Test
  public void testCastToExpectedTypeForNullMap() {
    Object result = CassandraTypeHandler.castToExpectedType("map<text, frozen<list<text>>>", null);
    assertNull(result);
  }

  @Test
  public void testCastToExpectedTypeForEmptyList() {
    Object result = CassandraTypeHandler.castToExpectedType("list<text>", "[]");
    assertNotNull(result);
    assertTrue(result instanceof List);
    assertEquals(Collections.emptyList(), result);
  }

  @Test
  public void testCastToExpectedTypeForEmptySet() {
    Object result = CassandraTypeHandler.castToExpectedType("set<text>", "[]");
    assertNotNull(result);
    assertTrue(result instanceof Set);
    assertEquals(Collections.emptySet(), result);
  }

  @Test
  public void testCastToExpectedTypeForEmptyMap() {
    Object result = CassandraTypeHandler.castToExpectedType("map<text, frozen<list<text>>>", "{}");
    assertNotNull(result);
    assertTrue(result instanceof Map);
    assertEquals(Collections.emptyMap(), result);
  }
}
