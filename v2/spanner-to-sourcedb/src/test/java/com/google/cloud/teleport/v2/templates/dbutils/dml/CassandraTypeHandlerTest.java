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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class CassandraTypeHandlerTest {

  private static final String TEST_TABLE = "TestTable";

  @Test
  public void testGetColumnValueByTypeForString() {
    // Create Ddl and get Column
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column("test_column")
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Table spannerTable = ddl.table(TEST_TABLE);
    Column spannerCol = spannerTable.column("test_column");
    // Create SourceColumn
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name("test_column")
            .type("varchar")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put("test_column", "test_value");
    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("test_value", castResult);
  }

  @Test
  public void testGetColumnValueByType() {
    String columnName = "LastName";
    String columnValue = "é";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("varchar").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals("é", castResult);
  }

  @Test
  public void testGetColumnValueByTypeForNumericToInt() {
    String columnName = "Salary";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .numeric()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("int").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 12345);
    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);

    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    assertEquals(12345, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringUUID() {
    String columnName = "id";
    String columnValue = "123e4567-e89b-12d3-a456-426614174000";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("uuid").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(UUID.fromString(columnValue), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringIpAddress() {
    String columnValue = "192.168.1.1";
    String columnName = "ipAddress";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("inet").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(InetAddresses.forString(columnValue), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonArray() {
    String columnValue = "[\"apple\", \"banana\", \"cherry\"]";
    String columnName = "fruits";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("set<text>")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    Set<String> expectedSet = new HashSet<>(Arrays.asList("apple", "banana", "cherry"));
    assertEquals(expectedSet, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringJsonObject() {
    String columnName = "user";
    String columnValue = "{\"name\": \"John\", \"age\": \"30\"}";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("map<text, text>")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("name", "John");
    expectedMap.put("age", "30");
    assertEquals(expectedMap, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForStringHex() {
    String columnName = "data";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    String columnValue = Base64.getEncoder().encodeToString(expectedBytes);
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bytes()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("blob").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
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
    String columnName = "lastName";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bytes()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("blob").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put("random", "someValue");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    assertEquals("blob", result.dataType());
    assertEquals(CassandraTypeHandler.NullClass.INSTANCE, result.value());
  }

  @Test
  public void testGetColumnValueByTypeForStringByteArrayBase64Encode() {
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    String columnValue = java.util.Base64.getEncoder().encodeToString(expectedBytes);
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bytes()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("blob").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
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
    String columnName = "lastName";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    String columnValue = Base64.getEncoder().encodeToString(expectedBytes);
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bytes()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("blob").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
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
    String columnValue = "P4DT1H";
    String columnName = "total_time";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("duration")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(CqlDuration.from("P4DT1H"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDates() {
    String columnValue = "2025-01-01T00:00:00Z";
    String columnName = "created_on";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .date()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("timestamp")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, columnValue);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());

    ZonedDateTime expectedDate = ZonedDateTime.parse(columnValue).withSecond(0).withNano(0);
    Instant instant = (Instant) castResult;
    ZonedDateTime actualDate = instant.atZone(ZoneOffset.UTC).withSecond(0).withNano(0);
    assertEquals(expectedDate, actualDate);
  }

  @Test
  public void testGetColumnValueByTypeForBigInt() {
    String columnName = "Salary";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .int64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bigint").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(123456789L));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    Long expectedBigInt = 123456789L;
    assertEquals(expectedBigInt, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBytesForHexString() {
    String columnName = "Name";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bytes").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "48656c6c6f20576f726c64");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("48656c6c6f20576f726c64", castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBigIntForString() {
    String columnName = "Salary";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bigint").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "123456789");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    long expectedValue = 123456789L;
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBooleanForString() {
    String columnName = "Male";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("boolean").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "1");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(true, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBoolean() {
    String columnName = "Male";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bool()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("boolean").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, false);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(false, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForIntegerValue() {
    String columnName = "Salary";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .int64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bigint").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 225000);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    long expectedValue = 225000L;
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBooleanSmallCaseForString() {
    String columnName = "Male";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("boolean").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "f");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(false, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForInteger() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .numeric()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("integer").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(5));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(BigInteger.valueOf(5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForValidBigInteger() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .int64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("int64").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, BigInteger.valueOf(5));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(BigInteger.valueOf(5), castResult);
  }

  @Test
  public void testConvertToCassandraTimestampWithISOInstant() {
    String timestamp = "2025-01-15T10:15:30Z";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    LocalDate expectedValue = Instant.parse(timestamp).atZone(ZoneId.systemDefault()).toLocalDate();
    assertEquals(expectedValue, castResult);
  }

  @Test
  public void testConvertToCassandraTimestampWithISODateTime() {
    String timestamp = "2025-01-15T10:15:30";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("datetime")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15T00:00:00Z", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithISODate() {
    String timestamp = "2025-01-15";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(timestamp, castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat1() {
    String timestamp = "01/15/2025";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat2() {
    String timestamp = "2025/01/15";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat3() {
    String timestamp = "15-01-2025";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat4() {
    String timestamp = "15/01/2025";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithCustomFormat5() {
    String timestamp = "2025-01-15 10:15:30";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals("2025-01-15", castResult.toString());
  }

  @Test
  public void testConvertToCassandraTimestampWithInvalidFormat() {
    String timestamp = "invalid-timestamp";
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, timestamp);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PreparedStatementValueObject result =
              getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
          CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
        });
  }

  @Test
  public void testConvertToCassandraTimestampWithNull() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, " ");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              PreparedStatementValueObject result =
                  getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
              CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testConvertToCassandraTimestampWithWhitespaceString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .timestamp()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("date").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "   ");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              PreparedStatementValueObject result =
                  getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
              CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
            });
    assertEquals("Error converting value for cassandraType: date", exception.getMessage());
  }

  @Test
  public void testGetColumnValueByTypeForFloat() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .float64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("float").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5f);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object actualValue = result.value();
    assertEquals(5.5f, actualValue);
  }

  @Test
  public void testGetColumnValueByTypeForFloatIllegalArgumentException() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .date()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("float").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "2024-12-12");

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          PreparedStatementValueObject<?> preparedStatementValueObject =
              getColumnValueByType(spannerCol, sourceCol, valuesJson, null);
          castToExpectedType(
              preparedStatementValueObject.dataType(), preparedStatementValueObject.value());
        });
  }

  @Test
  public void testGetColumnValueByTypeForFloat64() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .float64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("double").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, new BigDecimal("5.5"));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(5.5, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForFloat64FromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("double").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(5.5, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("decimal").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromFloat() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .float64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("decimal").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForDecimalFromFloat64() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .float64()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("decimal").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, 5.5);

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(BigDecimal.valueOf(5.5), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForFloatFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("float").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5.5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(5.5f, castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBigIntFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bigint").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(Long.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForIntFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("int").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(Integer.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForSmallIntFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA)
            .name(columnName)
            .type("smallint")
            .build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(Short.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForTinyIntFromString() {
    String columnName = "test_column";
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .string()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("tinyint").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, "5");

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object castResult = CassandraTypeHandler.castToExpectedType(result.dataType(), result.value());
    assertEquals(Byte.valueOf("5"), castResult);
  }

  @Test
  public void testGetColumnValueByTypeForBytes() {
    String columnName = "test_column";
    byte[] expectedBytes = new byte[] {1, 2, 3, 4, 5};
    Ddl ddl =
        Ddl.builder()
            .createTable(TEST_TABLE)
            .column(columnName)
            .bytes()
            .max()
            .endColumn()
            .endTable()
            .build();
    Column spannerCol = ddl.table(TEST_TABLE).column(columnName);
    SourceColumn sourceCol =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name(columnName).type("bytes").build();

    JSONObject valuesJson = new JSONObject();
    valuesJson.put(columnName, Base64.getEncoder().encodeToString(expectedBytes));

    PreparedStatementValueObject result =
        getColumnValueByType(spannerCol, sourceCol, valuesJson, "UTC");
    assertTrue(result instanceof PreparedStatementValueObject);
    Object actualValue = result.value();
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
  public void testCastToExpectedTypeForJSONObjectStringifyToFrozenMap() {
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
  public void testGetColumnValueByTypeForNullColumnDefs() {
    JSONObject valuesJson = new JSONObject();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          getColumnValueByType(null, null, valuesJson, null);
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
