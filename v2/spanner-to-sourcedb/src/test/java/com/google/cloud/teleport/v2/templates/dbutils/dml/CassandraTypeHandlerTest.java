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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.slf4j.Logger;

@RunWith(JUnit4.class)
public class CassandraTypeHandlerTest {

  private SpannerColumnDefinition spannerColDef;

  private SourceColumnDefinition sourceColDef;

  private JSONObject valuesJson;

  private static final Logger LOG = mock(Logger.class);

  private void mockLogging(ClassCastException e) {
    Mockito.doNothing().when(LOG).error(Mockito.anyString(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testGetColumnValueByTypeForString() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "test_column";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByType() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "é";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForNonString() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "DEL";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForStringUUID() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "123e4567-e89b-12d3-a456-426614174000";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForStringIpAddress() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "192.168.1.1";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonArray() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "[\"apple\", \"banana\", \"cherry\"]";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonObject() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "{\"name\": \"John\", \"age\": 30}";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForStringHex() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);

    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "a3f5b7";
    String sourceDbTimezoneOffset = "UTC";

    when(valuesJson.optString(columnName, null))
        .thenReturn(columnName); // Mock string value for column
    when(valuesJson.get(columnName)).thenReturn(columnName); // Mock getting column value

    when(spannerColDef.getType()).thenReturn(spannerType); // Spanner column type
    when(spannerColDef.getName()).thenReturn(columnName); // Column name in Spanner
    when(sourceColDef.getType())
        .thenReturn(new SourceColumnType("sourceType", null, null)); // Source column type

    PreparedStatementValueObject preparedStatementValueObject =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertEquals(CassandraTypeHandler.NullClass.INSTANCE, preparedStatementValueObject.value());
  }

  @Test
  public void testGetColumnValueByTypeForStringDuration() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("string", true);
    String columnName = "P4DT1H";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn(columnName);
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForDates() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("date", true);
    String columnName = "timestampColumn";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.optString(columnName, null)).thenReturn("2025-01-01T00:00:00Z");
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForBigInt() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("bigint", true);
    String columnName = "test_column";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.getBigInteger(columnName)).thenReturn(BigInteger.valueOf(5));
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result =
        getColumnValueByType(spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset);

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForInteger() {
    SpannerColumnDefinition spannerColDef = mock(SpannerColumnDefinition.class);
    SourceColumnDefinition sourceColDef = mock(SourceColumnDefinition.class);
    JSONObject valuesJson = mock(JSONObject.class);

    String columnName = "test_column";
    SpannerColumnType spannerType = new SpannerColumnType("integer", true);
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.getBigInteger(columnName)).thenReturn(BigInteger.valueOf(5));

    when(valuesJson.getInt(columnName)).thenReturn(5);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));
    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");
    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeForValidBigInteger() {
    SpannerColumnDefinition spannerColDef = mock(SpannerColumnDefinition.class);
    SourceColumnDefinition sourceColDef = mock(SourceColumnDefinition.class);
    JSONObject valuesJson = mock(JSONObject.class);

    String columnName = "test_column";
    SpannerColumnType spannerType = new SpannerColumnType("boolean", true);
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.getBigInteger(columnName)).thenReturn(BigInteger.valueOf(5));

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertNotNull(result);
  }

  @Test
  public void testGetColumnValueByTypeFor() {
    spannerColDef = mock(SpannerColumnDefinition.class);
    sourceColDef = mock(SourceColumnDefinition.class);
    valuesJson = mock(JSONObject.class);
    SpannerColumnType spannerType = new SpannerColumnType("float", true);
    String columnName = "test_column";
    String sourceDbTimezoneOffset = "UTC";
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.getBigDecimal(columnName)).thenReturn(new BigDecimal("5.5"));
    when(valuesJson.get(columnName)).thenReturn(columnName);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertNotNull(result);

    assertTrue(result instanceof PreparedStatementValueObject);

    Object actualValue = ((PreparedStatementValueObject<?>) result).value();

    assertEquals(5.5f, actualValue);
  }

  @Test
  public void testGetColumnValueByTypeForFloat64() {
    SpannerColumnDefinition spannerColDef = mock(SpannerColumnDefinition.class);
    SourceColumnDefinition sourceColDef = mock(SourceColumnDefinition.class);
    JSONObject valuesJson = mock(JSONObject.class);

    String columnName = "test_column";
    SpannerColumnType spannerType = new SpannerColumnType("float64", true);
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    when(valuesJson.getBigDecimal(columnName)).thenReturn(new BigDecimal("5.5"));

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertNotNull(result);

    assertTrue(result instanceof PreparedStatementValueObject);

    Object actualValue = ((PreparedStatementValueObject<?>) result).value();

    assertEquals(5.5, actualValue);
  }

  @Test
  public void testGetColumnValueByTypeForBytes() {
    SpannerColumnDefinition spannerColDef = mock(SpannerColumnDefinition.class);
    SourceColumnDefinition sourceColDef = mock(SourceColumnDefinition.class);
    JSONObject valuesJson = mock(JSONObject.class);

    String columnName = "test_column";
    SpannerColumnType spannerType = new SpannerColumnType("bytes", true);
    Long[] myArray = new Long[5];
    myArray[0] = 10L;
    myArray[1] = 20L;

    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    when(valuesJson.opt(columnName)).thenReturn(expectedBytes);

    when(spannerColDef.getType()).thenReturn(spannerType);
    when(spannerColDef.getName()).thenReturn(columnName);
    when(sourceColDef.getType()).thenReturn(new SourceColumnType("sourceType", myArray, myArray));

    Object result = getColumnValueByType(spannerColDef, sourceColDef, valuesJson, "UTC");

    assertNotNull(result);

    assertTrue(result instanceof PreparedStatementValueObject);

    Object actualValue = ((PreparedStatementValueObject<?>) result).value();

    byte[] actualBytes = ((ByteBuffer) actualValue).array();

    assertArrayEquals(expectedBytes, actualBytes);
  }

  @Test
  public void testCastToExpectedTypeForString() {
    String cassandraType = "text";
    String columnValue = "Test String";

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForBigInt() {
    String cassandraType = "bigint";
    Long columnValue = 123L;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForBoolean() {
    String cassandraType = "boolean";
    Boolean columnValue = true;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForDecimal() {
    String cassandraType = "decimal";
    BigDecimal columnValue = new BigDecimal("123.456");

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForDouble() {
    String cassandraType = "double";
    Double columnValue = 123.456;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForFloat() {
    String cassandraType = "float";
    Float columnValue = 123.45f;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForInet() throws Exception {
    String cassandraType = "inet";
    InetAddress columnValue = InetAddress.getByName("127.0.0.1");

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForInt() {
    String cassandraType = "int";
    Integer columnValue = 123;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForSmallInt() {
    String cassandraType = "smallint";
    Integer columnValue = 123;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals((short) 123, result);
  }

  @Test
  public void testCastToExpectedTypeForTimestamp() {
    String cassandraType = "timestamp";
    Instant columnValue = Instant.now();

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForDate() {
    String cassandraType = "date";
    LocalDate columnValue = LocalDate.now();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          castToExpectedType(cassandraType, columnValue);
        });
  }

  @Test
  public void testCastToExpectedTypeForUUID() {
    String cassandraType = "uuid";
    UUID columnValue = UUID.randomUUID();

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForTinyInt() {
    String cassandraType = "tinyint";
    Integer columnValue = 100;

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals((byte) 100, result);
  }

  @Test
  public void testCastToExpectedTypeForVarint() {
    String cassandraType = "varint";
    ByteBuffer columnValue = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(new BigInteger(columnValue.array()), result);
  }

  @Test
  public void testCastToExpectedTypeForDuration() {
    String cassandraType = "duration";
    Duration columnValue = Duration.ofHours(5);

    Object result = castToExpectedType(cassandraType, columnValue);

    assertEquals(columnValue, result);
  }

  @Test
  public void testCastToExpectedTypeForJSONArrayToList() {
    String cassandraType = "list<int>";
    JSONArray columnValue = new JSONArray(Arrays.asList(1, 2, 3));

    Object result = castToExpectedType(cassandraType, columnValue);

    assertTrue(result instanceof List);
  }

  @Test
  public void testCastToExpectedTypeForJSONArrayToSet() {
    String cassandraType = "set<int>";
    JSONArray columnValue = new JSONArray(Arrays.asList(1, 2, 3));

    Object result = castToExpectedType(cassandraType, columnValue);

    assertTrue(result instanceof Set);
  }

  @Test
  public void testCastToExpectedTypeForJSONObjectToMap() {
    String cassandraType = "map<int, text>";
    JSONObject columnValue = new JSONObject();
    columnValue.put(String.valueOf(1), "One");
    columnValue.put(String.valueOf(2), "Two");

    assertThrows(
        ClassCastException.class,
        () -> {
          castToExpectedType(cassandraType, columnValue);
        });
  }

  @Test
  public void testCastToExpectedTypeForExceptionScenario() {
    String cassandraType = "int";
    String columnValue = "InvalidInt";

    mockLogging(new ClassCastException("Invalid cast"));

    assertThrows(
        ClassCastException.class,
        () -> {
          castToExpectedType(cassandraType, columnValue);
        });
  }

  @Test
  public void testGetColumnValueByTypeForNullBothColumnDefs() {
    JSONObject valuesJson = mock(JSONObject.class);
    String sourceDbTimezoneOffset = "UTC";

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
    JSONArray listValue = new JSONArray();
    listValue.put("value1");
    listValue.put("value2");
    Object result = CassandraTypeHandler.castToExpectedType("list<text>", listValue);
    assertTrue(result instanceof List);
    assertEquals(2, ((List<?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForSet() {
    JSONArray setValue = new JSONArray();
    setValue.put("value1");
    setValue.put("value2");
    Object result = CassandraTypeHandler.castToExpectedType("set<text>", setValue);
    assertTrue(result instanceof Set);
    assertEquals(2, ((Set<?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForMap() {
    JSONObject mapValue = new JSONObject();
    mapValue.put("key1", "value1");
    mapValue.put("key2", "value2");
    Object result = CassandraTypeHandler.castToExpectedType("map<text, text>", mapValue);
    assertTrue(result instanceof Map);
    assertEquals(2, ((Map<?, ?>) result).size());
  }

  @Test
  public void testCastToExpectedTypeForInvalidType() {
    Object object = CassandraTypeHandler.castToExpectedType("unknownType", new Object());
    assertNotNull(object);
  }

  @Test
  public void testCastToExpectedTypeForNull() {
    assertThrows(
        NullPointerException.class,
        () -> {
          CassandraTypeHandler.castToExpectedType("text", null);
        });
  }

  @Test
  public void testCastToExpectedTypeForDate_String() {
    String dateString = "2025-01-09"; // Format: yyyy-MM-dd
    Object result = CassandraTypeHandler.castToExpectedType("date", dateString);
    LocalDate expected = LocalDate.parse(dateString);
    assertEquals(expected, result);
  }

  @Test
  public void testCastToExpectedTypeForDate_Instant() {
    Instant now = Instant.now();
    Object result = CassandraTypeHandler.castToExpectedType("date", now);
    LocalDate expected = now.atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(expected, result);
  }

  @Test
  public void testCastToExpectedTypeForDate_JavaUtilDate() {
    Date date = new Date();
    Object result = CassandraTypeHandler.castToExpectedType("date", date);
    LocalDate expected = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
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
    assertEquals(
        "Error handling type: Text 'invalid-date' could not be parsed at index 0",
        exception.getMessage());
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
    assertEquals(
        "Error handling type: Unsupported value for date conversion: 123", exception.getMessage());
  }

  @Test
  public void testHandleCassandraVarintType_String() {
    String validString = "12345678901234567890";
    Object result = CassandraTypeHandler.castToExpectedType("varint", validString);
    BigInteger expected = new BigInteger(validString);
    assertEquals(expected, result);
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
    assertEquals(
        "Invalid varint format (string) for value: invalid-number", exception.getMessage());
  }

  @Test
  public void testHandleCassandraVarintType_ByteArray() {
    byte[] validByteArray = new byte[] {0, 0, 0, 0, 0, 0, 0, 10};
    Object result = CassandraTypeHandler.castToExpectedType("varint", validByteArray);
    BigInteger expected = new BigInteger(validByteArray);
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraVarintType_ByteBuffer() {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 20});
    Object result = CassandraTypeHandler.castToExpectedType("varint", byteBuffer);
    BigInteger expected = new BigInteger(new byte[] {0, 0, 0, 0, 0, 0, 0, 20});
    assertEquals(expected, result);
  }

  @Test
  public void testHandleCassandraVarintType_UnsupportedType() {
    Integer unsupportedType = 123;
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              CassandraTypeHandler.castToExpectedType("varint", unsupportedType);
            });
    assertEquals(
        "Invalid value type for varint conversion: class java.lang.Integer",
        exception.getMessage());
  }
}
