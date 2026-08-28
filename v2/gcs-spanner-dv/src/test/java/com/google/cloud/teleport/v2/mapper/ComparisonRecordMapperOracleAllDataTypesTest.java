/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper;
import com.google.cloud.teleport.v2.visitor.IUnifiedVisitor;
import com.google.cloud.teleport.v2.visitor.UnifiedHasherVisitor;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit test validating that all Oracle data types specified in the Oracle Datatype Mapping Matrix
 * correctly convert from Avro GenericRecord and Spanner Struct into identical ComparisonRecord
 * hashes.
 */
@RunWith(JUnit4.class)
public class ComparisonRecordMapperOracleAllDataTypesTest {

  private ISchemaMapper mockSchemaMapper;
  private ISpannerMigrationTransformer mockTransformer;
  private Ddl mockDdl;
  private ComparisonRecordMapper mapper;

  @Before
  public void setUp() {
    mockSchemaMapper = mock(ISchemaMapper.class);
    mockTransformer = mock(ISpannerMigrationTransformer.class);
    mockDdl = mock(Ddl.class);
    mapper = new ComparisonRecordMapper(mockSchemaMapper, mockTransformer, mockDdl);
  }

  @Test
  public void testAllOracleDataTypesHashParity() throws Exception {
    String tableName = "AllDatatypes";

    // 1. Define Avro Schemas for Oracle Datatypes
    Schema decimalSchema =
        LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema timestampMicrosSchema =
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    Schema payloadSchema =
        SchemaBuilder.record("Payload")
            .fields()
            .name("id")
            .type(Schema.create(Schema.Type.LONG))
            .noDefault()
            .name("varchar2_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("varchar_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("char_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("character_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("nvarchar2_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("nchar_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("number_col")
            .type(decimalSchema)
            .noDefault()
            .name("numeric_col")
            .type(decimalSchema)
            .noDefault()
            .name("decimal_col")
            .type(decimalSchema)
            .noDefault()
            .name("dec_col")
            .type(decimalSchema)
            .noDefault()
            .name("float_col")
            .type(Schema.create(Schema.Type.DOUBLE))
            .noDefault()
            .name("double_precision_col")
            .type(Schema.create(Schema.Type.DOUBLE))
            .noDefault()
            .name("real_col")
            .type(Schema.create(Schema.Type.DOUBLE))
            .noDefault()
            .name("binary_float_col")
            .type(Schema.create(Schema.Type.FLOAT))
            .noDefault()
            .name("binary_double_col")
            .type(Schema.create(Schema.Type.DOUBLE))
            .noDefault()
            .name("integer_col")
            .type(Schema.create(Schema.Type.LONG))
            .noDefault()
            .name("int_col")
            .type(Schema.create(Schema.Type.LONG))
            .noDefault()
            .name("smallint_col")
            .type(Schema.create(Schema.Type.LONG))
            .noDefault()
            .name("date_col")
            .type(timestampMicrosSchema)
            .noDefault()
            .name("timestamp_col")
            .type(timestampMicrosSchema)
            .noDefault()
            .name("timestamp_tz_col")
            .type(timestampMicrosSchema)
            .noDefault()
            .name("timestamp_ltz_col")
            .type(timestampMicrosSchema)
            .noDefault()
            .name("interval_ym_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("interval_ds_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("raw_col")
            .type(Schema.create(Schema.Type.BYTES))
            .noDefault()
            .name("blob_col")
            .type(Schema.create(Schema.Type.BYTES))
            .noDefault()
            .name("clob_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("nclob_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("rowid_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("json_col")
            .type(SchemaBuilder.builder().stringBuilder().prop("logicalType", "json").endString())
            .noDefault()
            .name("xmltype_col")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .endRecord();

    Schema avroSchema =
        SchemaBuilder.record("SourceRow")
            .fields()
            .name("tableName")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("shardId")
            .type(Schema.create(Schema.Type.STRING))
            .noDefault()
            .name("payload")
            .type(payloadSchema)
            .noDefault()
            .endRecord();

    // 2. Populate Avro payload
    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 1L);
    payload.put("varchar2_col", "test_varchar2");
    payload.put("varchar_col", "test_varchar");
    payload.put("char_col", "test_char ");
    payload.put("character_col", "test_char ");
    payload.put("nvarchar2_col", "test_nvarchar2");
    payload.put("nchar_col", "test_nchar");
    payload.put(
        "number_col", ByteBuffer.wrap(new BigDecimal("1234.56").unscaledValue().toByteArray()));
    payload.put(
        "numeric_col", ByteBuffer.wrap(new BigDecimal("1234.56").unscaledValue().toByteArray()));
    payload.put(
        "decimal_col", ByteBuffer.wrap(new BigDecimal("1234.56").unscaledValue().toByteArray()));
    payload.put(
        "dec_col", ByteBuffer.wrap(new BigDecimal("1234.56").unscaledValue().toByteArray()));
    payload.put("float_col", 123.456d);
    payload.put("double_precision_col", 123.456d);
    payload.put("real_col", 123.456d);
    payload.put("binary_float_col", 123.0f);
    payload.put("binary_double_col", 123.0d);
    payload.put("integer_col", 12345L);
    payload.put("int_col", 12345L);
    payload.put("smallint_col", 123L);
    long timestampMicros = 1704103200000000L;
    payload.put("date_col", timestampMicros);
    payload.put("timestamp_col", timestampMicros);
    payload.put("timestamp_tz_col", timestampMicros);
    payload.put("timestamp_ltz_col", timestampMicros);
    payload.put("interval_ym_col", "P1Y2M");
    payload.put("interval_ds_col", "PT3H4M5S");
    payload.put("raw_col", ByteBuffer.wrap(new byte[] {0x41, 0x42, 0x43}));
    payload.put("blob_col", ByteBuffer.wrap(new byte[] {0x41, 0x42, 0x43, 0x44}));
    payload.put("clob_col", "test_clob_content");
    payload.put("nclob_col", "test_nclob_content");
    payload.put("rowid_col", "AAAB12AADAAAAwPAAA");
    payload.put("json_col", "{\"k1\":\"v1\"}");
    payload.put("xmltype_col", "<root><elem>test</elem></root>");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", tableName);
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    // 3. Configure Schema Mapper mocks
    List<String> columnNames =
        Arrays.asList(
            "id",
            "varchar2_col",
            "varchar_col",
            "char_col",
            "character_col",
            "nvarchar2_col",
            "nchar_col",
            "number_col",
            "numeric_col",
            "decimal_col",
            "dec_col",
            "float_col",
            "double_precision_col",
            "real_col",
            "binary_float_col",
            "binary_double_col",
            "integer_col",
            "int_col",
            "smallint_col",
            "date_col",
            "timestamp_col",
            "timestamp_tz_col",
            "timestamp_ltz_col",
            "interval_ym_col",
            "interval_ds_col",
            "raw_col",
            "blob_col",
            "clob_col",
            "nclob_col",
            "rowid_col",
            "json_col",
            "xmltype_col");

    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn(tableName);
    when(mockSchemaMapper.getSpannerColumnName(anyString(), anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(2));
    when(mockSchemaMapper.getSourceColumnName(anyString(), anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(2));
    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString())).thenReturn(columnNames);
    when(mockSchemaMapper.colExistsAtSource(anyString(), anyString(), anyString()))
        .thenReturn(true);

    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("id")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("varchar2_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("varchar_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("char_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("character_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("nvarchar2_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("nchar_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("number_col")))
        .thenReturn(Type.numeric());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("numeric_col")))
        .thenReturn(Type.numeric());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("decimal_col")))
        .thenReturn(Type.numeric());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("dec_col")))
        .thenReturn(Type.numeric());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("float_col")))
        .thenReturn(Type.float64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("double_precision_col")))
        .thenReturn(Type.float64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("real_col")))
        .thenReturn(Type.float64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("binary_float_col")))
        .thenReturn(Type.float32());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("binary_double_col")))
        .thenReturn(Type.float64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("integer_col")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("int_col")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("smallint_col")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("date_col")))
        .thenReturn(Type.timestamp());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("timestamp_col")))
        .thenReturn(Type.timestamp());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("timestamp_tz_col")))
        .thenReturn(Type.timestamp());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("timestamp_ltz_col")))
        .thenReturn(Type.timestamp());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("interval_ym_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("interval_ds_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("raw_col")))
        .thenReturn(Type.bytes());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("blob_col")))
        .thenReturn(Type.bytes());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("clob_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("nclob_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("rowid_col")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("json_col")))
        .thenReturn(Type.json());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("xmltype_col")))
        .thenReturn(Type.string());

    Table mockTable = mock(Table.class);
    when(mockDdl.table(tableName)).thenReturn(mockTable);
    IndexColumn pkCol = IndexColumn.create("id", IndexColumn.Order.ASC);
    when(mockTable.primaryKeys()).thenReturn(com.google.common.collect.ImmutableList.of(pkCol));

    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow()).thenReturn(Collections.emptyMap());
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    // 4. Map Avro Record to ComparisonRecord
    ComparisonRecord avroRecordResult = mapper.mapFrom(avroRecord);
    assertNotNull(avroRecordResult);
    assertEquals(tableName, avroRecordResult.getTableName());
    assertEquals("shard1", avroRecordResult.getShardId());

    // 5. Build identical Spanner Struct
    Timestamp spannerTimestamp = Timestamp.ofTimeMicroseconds(timestampMicros);
    Struct spannerStruct =
        Struct.newBuilder()
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to(tableName)
            .set("id")
            .to(1L)
            .set("varchar2_col")
            .to("test_varchar2")
            .set("varchar_col")
            .to("test_varchar")
            .set("char_col")
            .to("test_char ")
            .set("character_col")
            .to("test_char ")
            .set("nvarchar2_col")
            .to("test_nvarchar2")
            .set("nchar_col")
            .to("test_nchar")
            .set("number_col")
            .to(new BigDecimal("1234.560000000"))
            .set("numeric_col")
            .to(new BigDecimal("1234.560000000"))
            .set("decimal_col")
            .to(new BigDecimal("1234.560000000"))
            .set("dec_col")
            .to(new BigDecimal("1234.560000000"))
            .set("float_col")
            .to(123.456d)
            .set("double_precision_col")
            .to(123.456d)
            .set("real_col")
            .to(123.456d)
            .set("binary_float_col")
            .to(123.0f)
            .set("binary_double_col")
            .to(123.0d)
            .set("integer_col")
            .to(12345L)
            .set("int_col")
            .to(12345L)
            .set("smallint_col")
            .to(123L)
            .set("date_col")
            .to(spannerTimestamp)
            .set("timestamp_col")
            .to(spannerTimestamp)
            .set("timestamp_tz_col")
            .to(spannerTimestamp)
            .set("timestamp_ltz_col")
            .to(spannerTimestamp)
            .set("interval_ym_col")
            .to("P1Y2M")
            .set("interval_ds_col")
            .to("PT3H4M5S")
            .set("raw_col")
            .to(ByteArray.copyFrom(new byte[] {0x41, 0x42, 0x43}))
            .set("blob_col")
            .to(ByteArray.copyFrom(new byte[] {0x41, 0x42, 0x43, 0x44}))
            .set("clob_col")
            .to("test_clob_content")
            .set("nclob_col")
            .to("test_nclob_content")
            .set("rowid_col")
            .to("AAAB12AADAAAAwPAAA")
            .set("json_col")
            .to(Value.json("{\"k1\":\"v1\"}"))
            .set("xmltype_col")
            .to("<root><elem>test</elem></root>")
            .build();

    // 6. Map Spanner Struct to ComparisonRecord
    ComparisonRecord spannerRecordResult = mapper.mapFrom(spannerStruct);
    assertNotNull(spannerRecordResult);

    GenericRecordTypeConvertor convertor =
        new GenericRecordTypeConvertor(mockSchemaMapper, "", "shard1", mockTransformer);
    java.util.Map<String, Value> avroValues = convertor.transformChangeEvent(payload, tableName);

    for (String col : columnNames) {
      Value avroVal = avroValues.get(col);
      Value spannerVal = spannerStruct.getValue(col);

      com.google.common.hash.Hasher h1 = com.google.common.hash.Hashing.murmur3_128().newHasher();
      UnifiedHasherVisitor v1 = new UnifiedHasherVisitor(h1);
      IUnifiedVisitor.dispatch(avroVal, v1);

      com.google.common.hash.Hasher h2 = com.google.common.hash.Hashing.murmur3_128().newHasher();
      UnifiedHasherVisitor v2 = new UnifiedHasherVisitor(h2);
      IUnifiedVisitor.dispatch(spannerVal, v2);

      org.junit.Assert.assertEquals(
          "Hash mismatch for column "
              + col
              + " (avroVal: "
              + avroVal
              + " vs spannerVal: "
              + spannerVal
              + ")",
          h2.hash().toString(),
          h1.hash().toString());
    }

    // 7. Verify Hashes Match Exactly!
    assertEquals(
        "Avro hash and Spanner Struct hash must match for all Oracle datatypes",
        spannerRecordResult.getHash(),
        avroRecordResult.getHash());
  }

  @Test
  public void testSmokeSchemaAndRecordsDirectly() throws Exception {
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("OracleAllDatatypes")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("varchar2_col")
            .string()
            .max()
            .endColumn()
            .column("varchar_col")
            .string()
            .max()
            .endColumn()
            .column("char_col")
            .string()
            .max()
            .endColumn()
            .column("character_col")
            .string()
            .max()
            .endColumn()
            .column("nvarchar2_col")
            .string()
            .max()
            .endColumn()
            .column("nchar_col")
            .string()
            .max()
            .endColumn()
            .column("number_col")
            .numeric()
            .endColumn()
            .column("numeric_col")
            .numeric()
            .endColumn()
            .column("decimal_col")
            .numeric()
            .endColumn()
            .column("dec_col")
            .numeric()
            .endColumn()
            .column("float_col")
            .float64()
            .endColumn()
            .column("double_precision_col")
            .float64()
            .endColumn()
            .column("real_col")
            .float64()
            .endColumn()
            .column("binary_float_col")
            .float32()
            .endColumn()
            .column("binary_double_col")
            .float64()
            .endColumn()
            .column("integer_col")
            .int64()
            .endColumn()
            .column("int_col")
            .int64()
            .endColumn()
            .column("smallint_col")
            .int64()
            .endColumn()
            .column("date_col")
            .timestamp()
            .endColumn()
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .column("timestamp_tz_col")
            .timestamp()
            .endColumn()
            .column("timestamp_ltz_col")
            .timestamp()
            .endColumn()
            .column("interval_ym_col")
            .string()
            .max()
            .endColumn()
            .column("interval_ds_col")
            .string()
            .max()
            .endColumn()
            .column("raw_col")
            .bytes()
            .max()
            .endColumn()
            .column("blob_col")
            .bytes()
            .max()
            .endColumn()
            .column("clob_col")
            .string()
            .max()
            .endColumn()
            .column("nclob_col")
            .string()
            .max()
            .endColumn()
            .column("rowid_col")
            .string()
            .max()
            .endColumn()
            .column("json_col")
            .json()
            .endColumn()
            .column("xmltype_col")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    IdentityMapper identityMapper = new IdentityMapper(ddl);
    ComparisonRecordMapper smokeMapper = new ComparisonRecordMapper(identityMapper, null, ddl);

    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            GCSSpannerDVAvroSetupHelper.parseAvroSchema(
                "GCSSpannerDVOracleSmokeIT/oracle_all_datatypes.avsc"),
            "OracleAllDatatypes",
            Arrays.asList("id"));

    java.time.Instant testTimestamp = java.time.Instant.parse("2024-01-01T10:00:00Z");
    BigDecimal testNumeric = new BigDecimal("1234.560000000");
    byte[] testBytes = new byte[] {0x41, 0x42, 0x43, 0x44};

    GenericRecord avroRecord =
        new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
            .set("id", 1L)
            .set("varchar2_col", "test_varchar2")
            .set("varchar_col", "test_varchar")
            .set("char_col", "test_char  ")
            .set("character_col", "test_char  ")
            .set("nvarchar2_col", "test_nvarchar2")
            .set("nchar_col", "test_nchar ")
            .set("number_col", testNumeric)
            .set("numeric_col", testNumeric)
            .set("decimal_col", testNumeric)
            .set("dec_col", testNumeric)
            .set("float_col", 123.456)
            .set("double_precision_col", 123.456)
            .set("real_col", 123.456)
            .set("binary_float_col", 123.0f)
            .set("binary_double_col", 123.0)
            .set("integer_col", 12345L)
            .set("int_col", 12345L)
            .set("smallint_col", 123L)
            .set("date_col", testTimestamp)
            .set("timestamp_col", testTimestamp)
            .set("timestamp_tz_col", testTimestamp)
            .set("timestamp_ltz_col", testTimestamp)
            .set("interval_ym_col", "P1Y2M")
            .set("interval_ds_col", "PT3H4M5S")
            .set("raw_col", testBytes)
            .set("blob_col", testBytes)
            .set("clob_col", "test_clob_content")
            .set("nclob_col", "test_nclob_content")
            .set("rowid_col", "AAAB12AADAAAAwPAAA")
            .set("json_col", "{}")
            .set("xmltype_col", "<root><elem>test</elem></root>")
            .build();

    Struct spannerStruct =
        Struct.newBuilder()
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to("OracleAllDatatypes")
            .set("id")
            .to(1L)
            .set("varchar2_col")
            .to("test_varchar2")
            .set("varchar_col")
            .to("test_varchar")
            .set("char_col")
            .to("test_char  ")
            .set("character_col")
            .to("test_char  ")
            .set("nvarchar2_col")
            .to("test_nvarchar2")
            .set("nchar_col")
            .to("test_nchar ")
            .set("number_col")
            .to(testNumeric)
            .set("numeric_col")
            .to(testNumeric)
            .set("decimal_col")
            .to(testNumeric)
            .set("dec_col")
            .to(testNumeric)
            .set("float_col")
            .to(123.456)
            .set("double_precision_col")
            .to(123.456)
            .set("real_col")
            .to(123.456)
            .set("binary_float_col")
            .to(123.0f)
            .set("binary_double_col")
            .to(123.0)
            .set("integer_col")
            .to(12345L)
            .set("int_col")
            .to(12345L)
            .set("smallint_col")
            .to(123L)
            .set("date_col")
            .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
            .set("timestamp_col")
            .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
            .set("timestamp_tz_col")
            .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
            .set("timestamp_ltz_col")
            .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
            .set("interval_ym_col")
            .to("P1Y2M")
            .set("interval_ds_col")
            .to("PT3H4M5S")
            .set("raw_col")
            .to(ByteArray.copyFrom(testBytes))
            .set("blob_col")
            .to(ByteArray.copyFrom(testBytes))
            .set("clob_col")
            .to("test_clob_content")
            .set("nclob_col")
            .to("test_nclob_content")
            .set("rowid_col")
            .to("AAAB12AADAAAAwPAAA")
            .set("json_col")
            .to(Value.json("{}"))
            .set("xmltype_col")
            .to("<root><elem>test</elem></root>")
            .build();

    ComparisonRecord avroResult = smokeMapper.mapFrom(avroRecord);
    ComparisonRecord spannerResult = smokeMapper.mapFrom(spannerStruct);

    assertNotNull(avroResult);
    assertNotNull(spannerResult);

    GenericRecord payload = (GenericRecord) avroRecord.get("payload");
    GenericRecordTypeConvertor convertor =
        new GenericRecordTypeConvertor(identityMapper, "", null, null);
    java.util.Map<String, Value> avroValues =
        convertor.transformChangeEvent(payload, "OracleAllDatatypes");

    for (String col :
        ddl.table("OracleAllDatatypes").columns().stream().map(Column::name).toList()) {
      Value avroVal = avroValues.get(col);
      Value spannerVal = spannerStruct.getValue(col);

      com.google.common.hash.Hasher h1 = com.google.common.hash.Hashing.murmur3_128().newHasher();
      UnifiedHasherVisitor v1 = new UnifiedHasherVisitor(h1);
      IUnifiedVisitor.dispatch(avroVal, v1);

      com.google.common.hash.Hasher h2 = com.google.common.hash.Hashing.murmur3_128().newHasher();
      UnifiedHasherVisitor v2 = new UnifiedHasherVisitor(h2);
      IUnifiedVisitor.dispatch(spannerVal, v2);

      org.junit.Assert.assertEquals(
          "Hash mismatch for column "
              + col
              + " (avroVal: "
              + avroVal
              + " vs spannerVal: "
              + spannerVal
              + ")",
          h2.hash().toString(),
          h1.hash().toString());
    }

    assertEquals(spannerResult.getHash(), avroResult.getHash());
  }
}
