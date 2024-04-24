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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

public class GenericRecordTypeConvertorTest {

  @Test
  public void testHandleLogicalFieldType() throws IOException {
    Schema avroSchema =
        SchemaUtils.parseAvroSchema(
            Files.readString(Paths.get("src/test/resources/avro/logical-types-schema.avsc")));
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("date_col", 738991);
    genericRecord.put(
        "decimal_col", ByteBuffer.wrap(new BigDecimal("12.34").unscaledValue().toByteArray()));
    genericRecord.put("time_micros_col", 48035000000L);
    genericRecord.put("time_millis_col", 48035000);
    genericRecord.put("timestamp_micros_col", 1602599400056483L);
    genericRecord.put("timestamp_millis_col", 1602599400056L);

    String col = "date_col";
    String result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test date_col conversion: ", "3993-04-16", result);

    col = "decimal_col";
    result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test decimal_col conversion: ", "12.34", result);

    col = "time_micros_col";
    result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test time_micros_col conversion: ", "13:20:35", result);

    col = "time_millis_col";
    result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test time_millis_col conversion: ", "13:20:35", result);

    col = "timestamp_micros_col";
    result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test timestamp_micros_col conversion: ", "2020-10-13T14:30:00.056483Z", result);

    col = "timestamp_millis_col";
    result =
        GenericRecordTypeConvertor.handleLogicalFieldType(
            col, genericRecord.get(col), genericRecord.getSchema().getField(col).schema());
    assertEquals("Test timestamp_millis_col conversion: ", "2020-10-13T14:30:00.056Z", result);
  }

  @Test
  public void testHandleLogicalFieldType_nullInput() {
    assertNull(
        GenericRecordTypeConvertor.handleLogicalFieldType(
            "col", null, SchemaBuilder.builder().stringType()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testHandleLogicalFieldType_unsupportedLogicalType() {
    GenericRecordTypeConvertor.handleLogicalFieldType(
        "col", "test", SchemaBuilder.builder().stringType());
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

    String col = "timestamp_with_time_zone_column";
    String result =
        GenericRecordTypeConvertor.handleRecordFieldType(
            col,
            (GenericRecord) genericRecord.get(col),
            genericRecord.getSchema().getField(col).schema());
    assertEquals("Test timestampTz conversion: ", "2020-10-13T14:30:00.056483Z", result);

    col = "date_time_column";
    result =
        GenericRecordTypeConvertor.handleRecordFieldType(
            col,
            (GenericRecord) genericRecord.get(col),
            genericRecord.getSchema().getField(col).schema());
    assertEquals("Test datetime conversion: ", "3993-04-16T13:20:35Z", result);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            GenericRecordTypeConvertor.handleRecordFieldType(
                "unsupported_type_column",
                (GenericRecord) genericRecord.get("unsupported_type_column"),
                genericRecord.getSchema().getField("unsupported_type_column").schema()));
  }

  @Test
  public void testHandleRecordFieldType_nullInput() {
    assertNull(
        GenericRecordTypeConvertor.handleRecordFieldType(
            "col", null, SchemaBuilder.builder().stringType()));
  }

  static Ddl getIdentityDdl() {
    /* Creates DDL without any schema transformations.
     */
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
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
  public void transformChangeEventTest_identityMapper() throws IOException {
    GenericRecord genericRecord =
        new GenericData.Record(
            SchemaUtils.parseAvroSchema(
                Files.readString(Paths.get("src/test/resources/avro/all-spanner-types.avsc"))));
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
        new GenericRecordTypeConvertor(new IdentityMapper(getIdentityDdl()), "");
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
  public void transformChangeEventTest_incorrectSpannerType() throws IOException {

    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn("test");
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(List.of("bool_col"));
    when(mockSchemaMapper.getSourceColumnName(anyString(), anyString(), anyString()))
        .thenReturn("bool_col");
    when(mockSchemaMapper.getSpannerColumnType(anyString(), anyString(), anyString()))
        .thenReturn(Type.array(Type.bool()));

    GenericRecord genericRecord =
        new GenericData.Record(
            SchemaUtils.parseAvroSchema(
                Files.readString(Paths.get("src/test/resources/avro/all-spanner-types.avsc"))));
    genericRecord.put("bool_col", true);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        new GenericRecordTypeConvertor(mockSchemaMapper, "");

    genericRecordTypeConvertor.transformChangeEvent(genericRecord, "all_types");
  }

  @Test
  public void transformChangeEventTest_nullDialect() throws IOException {
    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    when(mockSchemaMapper.getDialect()).thenReturn(null);
    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn("test");
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(List.of("bool_col"));
    when(mockSchemaMapper.getSourceColumnName(anyString(), anyString(), anyString()))
        .thenReturn("bool_col");
    when(mockSchemaMapper.getSpannerColumnType(anyString(), anyString(), anyString()))
        .thenReturn(Type.array(Type.bool()));

    GenericRecord genericRecord =
        new GenericData.Record(
            SchemaUtils.parseAvroSchema(
                Files.readString(Paths.get("src/test/resources/avro/all-spanner-types.avsc"))));
    genericRecord.put("bool_col", true);
    GenericRecordTypeConvertor genericRecordTypeConvertor =
        new GenericRecordTypeConvertor(mockSchemaMapper, "");

    assertThrows(
        NullPointerException.class,
        () -> genericRecordTypeConvertor.transformChangeEvent(genericRecord, "all_types"));
    // Verify that the mock method was called.
    Mockito.verify(mockSchemaMapper).getDialect();
  }
}
