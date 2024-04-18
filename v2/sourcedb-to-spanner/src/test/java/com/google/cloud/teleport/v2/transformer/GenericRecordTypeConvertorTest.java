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
package com.google.cloud.teleport.v2.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class GenericRecordTypeConvertorTest {

  @Test
  public void testAvroFieldToBoolean() {
    Boolean inputValue = true;
    Boolean result = GenericRecordTypeConvertor.avroFieldToBoolean(inputValue, Schema.Type.BOOLEAN);
    assertEquals("Test true boolean input", inputValue, result);

    inputValue = false;
    result = GenericRecordTypeConvertor.avroFieldToBoolean(inputValue, Schema.Type.BOOLEAN);
    assertEquals("Test false boolean input", inputValue, result);

    result = GenericRecordTypeConvertor.avroFieldToBoolean("true", Schema.Type.STRING);
    assertEquals("Test string input", true, result);

    result = GenericRecordTypeConvertor.avroFieldToBoolean(4, Schema.Type.INT);
    assertEquals("Test int input", false, result);

    result = GenericRecordTypeConvertor.avroFieldToBoolean(1L, Schema.Type.LONG);
    assertEquals("Test long input", false, result);
  }

  @Test
  public void testAvroFieldToBoolean_NullInput() {
    Boolean result = GenericRecordTypeConvertor.avroFieldToBoolean(null, Schema.Type.STRING);
    assertNull(result);
  }

  @Test
  public void testAvroFieldToLong_ValidConversion() {
    Long inputValue = 10L;
    Long result = GenericRecordTypeConvertor.avroFieldToLong(inputValue, Schema.Type.LONG);
    assertEquals(inputValue, result);

    // Test integer values as well.
    Integer intValue = 10;
    result = GenericRecordTypeConvertor.avroFieldToLong(inputValue, Schema.Type.INT);
    assertEquals("Test int input", Long.valueOf(intValue), result);

    // Test string values as well.
    result = GenericRecordTypeConvertor.avroFieldToLong("1536", Schema.Type.INT);
    assertEquals("Test string input", (Long) 1536L, result);
  }

  @Test
  public void testAvroFieldToLong_NullInput() {
    Object inputValue = null;
    Long result = GenericRecordTypeConvertor.avroFieldToLong(inputValue, Schema.Type.LONG);
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToLong_NonLongInput() {
    String inputValue = "test";
    Schema.Type inputType = Schema.Type.STRING;
    GenericRecordTypeConvertor.avroFieldToLong(inputValue, inputType);
  }

  @Test
  public void testAvroFieldToDouble_ValidDoubleInput() {
    Double inputValue = 5.75;
    Double result = GenericRecordTypeConvertor.avroFieldToDouble(inputValue, Schema.Type.DOUBLE);
    assertEquals(inputValue, result);

    result = GenericRecordTypeConvertor.avroFieldToDouble(3.14f, Schema.Type.FLOAT);
    assertEquals("Test float input", (Double) 3.14, result);

    result = GenericRecordTypeConvertor.avroFieldToDouble("456.346", Schema.Type.DOUBLE);
    assertEquals("Test string input", (Double) 456.346, result);

    result = GenericRecordTypeConvertor.avroFieldToDouble(10L, Schema.Type.LONG);
    assertEquals("Test long input", Double.valueOf(10L), result);

    Integer intValue = 10;
    result = GenericRecordTypeConvertor.avroFieldToDouble(10, Schema.Type.INT);
    assertEquals("Test int input", Double.valueOf(10), result);
  }

  @Test
  public void testAvroFieldToDouble_NullInput() {
    Object inputValue = null;
    Double result = GenericRecordTypeConvertor.avroFieldToDouble(inputValue, Schema.Type.DOUBLE);
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToDouble_UnsupportedType() {
    Boolean inputValue = true;
    Schema.Type inputType = Schema.Type.BOOLEAN;
    GenericRecordTypeConvertor.avroFieldToDouble(inputValue, inputType);
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
    for (String input : testCases.keySet()) {
      BigDecimal result =
          GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(input, Schema.Type.STRING);
      assertEquals(
          String.format("Test case input : %s", input), testCases.get(input), result.toString());
    }
  }

  @Test
  public void testAvroFieldToNumericBigDecimal_DoubleInput() {
    Double inputValue = 3.14159;
    BigDecimal expectedResult = new BigDecimal(inputValue).setScale(9, RoundingMode.HALF_UP);
    BigDecimal result =
        GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(inputValue, Schema.Type.DOUBLE);
    assertEquals(expectedResult, result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToNumericBigDecimal_InvalidInput_string() {
    String inputValue = "123456.789asd";
    GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(inputValue, Schema.Type.STRING);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToNumericBigDecimal_InvalidInput_boolean() {
    Boolean inputValue = true;
    GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(inputValue, Schema.Type.BOOLEAN);
  }

  @Test
  public void testAvroFieldToNumericBigDecimal_NullInput() {
    Object inputValue = null;
    BigDecimal result =
        GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(inputValue, Schema.Type.STRING);
    assertNull(result);
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
        GenericRecordTypeConvertor.avroFieldToByteArray(inputValue, Schema.Type.STRING);
    assertEquals("Test even length input", expectedResult, result);

    // Test odd length string.
    inputValue = "8656c6c6f20686f772061722065796f75";
    expectedResult =
        ByteArray.copyFrom(
            new byte[] {
              8, 101, 108, 108, 111, 32, 104, 111, 119, 32, 97, 114, 32, 101, 121, 111, 117
            });
    result = GenericRecordTypeConvertor.avroFieldToByteArray(inputValue, Schema.Type.STRING);
    assertEquals("Test odd length input", expectedResult, result);
  }

  @Test
  public void testAvroFieldToByteArray_ValidByteArrayInput() {
    byte[] inputValue = {10, 20, 30};
    ByteArray result =
        GenericRecordTypeConvertor.avroFieldToByteArray(inputValue, Schema.Type.BYTES);
    assertEquals(ByteArray.copyFrom(inputValue), result);
  }

  @Test
  public void testAvroFieldToByteArray_NullInput() {
    Object inputValue = null;
    ByteArray result =
        GenericRecordTypeConvertor.avroFieldToByteArray(inputValue, Schema.Type.BYTES);
    assertNull(result);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void testAvroFieldToByteArray_UnsupportedType() {
    Integer inputValue = 5;
    GenericRecordTypeConvertor.avroFieldToByteArray(inputValue, Schema.Type.INT);
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
      Timestamp result = GenericRecordTypeConvertor.avroFieldToTimestamp(input, schema);
      assertEquals(
          String.format("Test case input : %s", input),
          Timestamp.parseTimestamp(testCases.get(input)),
          result);
    }
  }

  @Test
  public void testAvroToTimestamp_null() {
    assertNull(
        GenericRecordTypeConvertor.avroFieldToTimestamp(
            null, SchemaBuilder.builder().stringType()));
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToTimestamp() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToTimestamp("asd123456.789", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertLongToTimestamp() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToTimestamp(1234523342, schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertBooleanToTimestamp() throws Exception {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToTimestamp(true, schema);
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
      Date result = GenericRecordTypeConvertor.avroFieldToDate(input, schema);
      assertEquals(
          String.format("Test case input : %s", input),
          com.google.cloud.Date.parseDate(testCases.get(input)),
          result);
    }
  }

  @Test
  public void testAvroToDate_null() {
    assertNull(
        GenericRecordTypeConvertor.avroFieldToDate(null, SchemaBuilder.builder().stringType()));
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToDate("asd123456.789", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertRandomStringToDateEndingWithZ() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToDate("asd123456.789Z", schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertLongToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToDate(1234523342, schema);
  }

  @Test(expected = AvroTypeConvertorException.class)
  public void cannotConvertBooleanToDate() {
    Schema schema = SchemaBuilder.builder().stringType();
    GenericRecordTypeConvertor.avroFieldToDate(true, schema);
  }

  @Test
  public void testHandleLogicalFieldType() {
    Schema avroSchema = SchemaUtils.parseAvroSchema(AvroTestingHelper.LOGICAL_TYPES_SCHEMA_JSON);
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("date_col", 738991);
    genericRecord.put(
        "decimal_col", ByteBuffer.wrap(new BigDecimal("12.34").unscaledValue().toByteArray()));
    genericRecord.put("time_micros_col", 48035000000L);
    genericRecord.put("time_millis_col", 48035000);
    genericRecord.put("timestamp_micros_col", 1602599400056483L);
    genericRecord.put("timestamp_millis_col", 1602599400056L);

    String result = GenericRecordTypeConvertor.handleLogicalFieldType("date_col", genericRecord);
    assertEquals("Test date_col conversion: ", "3993-04-16", result);

    result = GenericRecordTypeConvertor.handleLogicalFieldType("decimal_col", genericRecord);
    assertEquals("Test decimal_col conversion: ", "12.34", result);

    result = GenericRecordTypeConvertor.handleLogicalFieldType("time_micros_col", genericRecord);
    assertEquals("Test time_micros_col conversion: ", "13:20:35", result);

    result = GenericRecordTypeConvertor.handleLogicalFieldType("time_millis_col", genericRecord);
    assertEquals("Test time_millis_col conversion: ", "13:20:35", result);

    result =
        GenericRecordTypeConvertor.handleLogicalFieldType("timestamp_micros_col", genericRecord);
    assertEquals("Test timestamp_micros_col conversion: ", "2020-10-13T14:30:00.056483Z", result);

    result =
        GenericRecordTypeConvertor.handleLogicalFieldType("timestamp_millis_col", genericRecord);
    assertEquals("Test timestamp_millis_col conversion: ", "2020-10-13T14:30:00.056Z", result);
  }

  @Test
  public void testHandleLogicalFieldType_nullInput() {
    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.get(anyString())).thenReturn(null);
    assertNull(GenericRecordTypeConvertor.handleLogicalFieldType("col", mockRecord));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testHandleLogicalFieldType_unsupportedLogicalType() {
    Schema mockFieldSchema = mock(Schema.class);
    when(mockFieldSchema.getLogicalType()).thenReturn(LogicalTypes.uuid());
    GenericRecord mockElement = mock(GenericRecord.class);
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.getField(anyString())).thenReturn(mock(Schema.Field.class));
    when(mockSchema.getField(anyString()).schema()).thenReturn(mockFieldSchema);
    when(mockElement.getSchema()).thenReturn(mockSchema);
    when(mockElement.get("col")).thenReturn("test");

    GenericRecordTypeConvertor.handleLogicalFieldType("col", mockElement);
  }

  @Test
  public void testHandleRecordFieldType() {

    String unsupportedSchemaJson =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"unsupportedName\",\n"
            + "  \"fields\": [\n"
            + "    {\"name\": \"months\",\n"
            + "     \"type\": \"int\"}\n"
            + "  ]}";

    String avroSchemaJson =
        "{\n"
            + "  \"type\" : \"record\",\n"
            + "  \"name\" : \"cart\",\n"
            + "  \"namespace\" : \"com.test.schema\",\n"
            + "  \"fields\" : [\n"
            + "    {\n"
            + "      \"name\": \"timestamp_with_time_zone_column\",\n"
            + "      \"type\": "
            + AvroTestingHelper.TIMESTAMPTZ_SCHEMA_JSON
            + "\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"date_time_column\",\n"
            + "      \"type\": "
            + AvroTestingHelper.DATETIME_SCHEMA_JSON
            + "\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"unsupported_type_column\",\n"
            + "      \"type\": "
            + unsupportedSchemaJson
            + "\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    Schema avroSchema = SchemaUtils.parseAvroSchema(avroSchemaJson);
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put(
        "timestamp_with_time_zone_column",
        AvroTestingHelper.createTimestampTzRecord(1602599400056483L, 3600000));
    genericRecord.put(
        "date_time_column", AvroTestingHelper.createDatetimeRecord(738991, 48035000000L));
    genericRecord.put(
        "unsupported_type_column",
        new GenericData.Record(SchemaUtils.parseAvroSchema(unsupportedSchemaJson)));

    String result =
        GenericRecordTypeConvertor.handleRecordFieldType(
            "timestamp_with_time_zone_column", genericRecord);
    assertEquals("Test timestampTz conversion: ", "2020-10-13T14:30:00.056483Z", result);

    result = GenericRecordTypeConvertor.handleRecordFieldType("date_time_column", genericRecord);
    assertEquals("Test datetime conversion: ", "3993-04-16T13:20:35Z", result);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            GenericRecordTypeConvertor.handleRecordFieldType(
                "unsupported_type_column", genericRecord));
  }

  @Test
  public void testHandleRecordFieldType_nullInput() {
    GenericRecord mockRecord = mock(GenericRecord.class);
    when(mockRecord.get(anyString())).thenReturn(null);
    assertNull(GenericRecordTypeConvertor.handleRecordFieldType("col", mockRecord));
  }

  static Ddl getIdentityDdl() {
    /* Creates DDL without any schema transformations.
     */
    Ddl ddl =
        Ddl.builder()
            .createTable("all_types")
            .column("bool_col")
            .bool()
            .notNull()
            .endColumn()
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .column("float_col")
            .float64()
            .endColumn()
            .column("string_col")
            .string()
            .size(10)
            .endColumn()
            .column("numeric_col")
            .numeric()
            .endColumn()
            .column("bytes_col")
            .bytes()
            .endColumn()
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .column("date_col")
            .date()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  @Test
  public void transformChangeEventTest_identityMapper() {
    GenericRecord genericRecord =
        new GenericData.Record(
            SchemaUtils.parseAvroSchema(AvroTestingHelper.ALL_SPANNER_TYPES_AVRO_JSON));
    genericRecord.put("bool_col", true);
    genericRecord.put("int_col", 10);
    genericRecord.put("float_col", 10.34);
    genericRecord.put("string_col", "hello");
    genericRecord.put(
        "numeric_col", ByteBuffer.wrap(new BigDecimal("12.34").unscaledValue().toByteArray()));
    genericRecord.put("bytes_col", new byte[] {10, 20, 30});
    genericRecord.put(
        "timestamp_col", AvroTestingHelper.createTimestampTzRecord(1602599400056483L, 3600000));
    genericRecord.put("date_col", 738991);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        GenericRecordTypeConvertor.create(new IdentityMapper(getIdentityDdl()), "");
    Map<String, Value> actual =
        genericRecordTypeConvertor.transformChangeEvent(genericRecord, "all_types");
    Map<String, Value> expected =
        Map.of(
            "bool_col", Value.bool(true),
            "int_col", Value.int64(10),
            "float_col", Value.float64(10.34),
            "string_col", Value.string("hello"),
            "numeric_col", Value.numeric(new BigDecimal("12.340000000")),
            "bytes_col", Value.bytes(ByteArray.copyFrom(new byte[] {10, 20, 30})),
            "timestamp_col",
                Value.timestamp(Timestamp.parseTimestamp("2020-10-13T14:30:00.056483Z")),
            "date_col", Value.date(com.google.cloud.Date.parseDate("3993-04-16")));
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void transformChangeEventTest_incorrectSpannerType() {

    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn("test");
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(List.of("bool_col"));
    when(mockSchemaMapper.getSourceColumnName(anyString(), anyString(), anyString()))
        .thenReturn("bool_col");
    when(mockSchemaMapper.getSpannerColumnType(anyString(), anyString(), anyString()))
        .thenReturn(Type.array(Type.bool()));

    GenericRecord genericRecord =
        new GenericData.Record(
            SchemaUtils.parseAvroSchema(AvroTestingHelper.ALL_SPANNER_TYPES_AVRO_JSON));
    genericRecord.put("bool_col", true);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        GenericRecordTypeConvertor.create(mockSchemaMapper, "");

    genericRecordTypeConvertor.transformChangeEvent(genericRecord, "all_types");
  }
}
