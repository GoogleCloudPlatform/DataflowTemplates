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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComparisonRecordMapperTest {

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
  public void testMapFromSpannerStruct() {
    String tableName = "Users";
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to("Alice")
            .set("__tableName__")
            .to(tableName)
            .build();

    Table mockTable = mock(Table.class);
    when(mockDdl.table(tableName)).thenReturn(mockTable);

    // Mock primary keys
    IndexColumn pkCol = IndexColumn.create("id", IndexColumn.Order.ASC);
    when(mockTable.primaryKeys()).thenReturn(com.google.common.collect.ImmutableList.of(pkCol));

    ComparisonRecord record = mapper.mapFrom(struct);

    assertNotNull(record);
    assertEquals(tableName, record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("id", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("1", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  @Test
  public void testMapFromAvroRecord() throws Exception {
    // Setup Avro record
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        Arrays.asList(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 1L);
    payload.put("name", "Alice");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "Users");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    String tableName = "Users";
    when(mockSchemaMapper.getSpannerTableName(anyString(), anyString())).thenReturn(tableName);
    when(mockSchemaMapper.getSpannerColumnName(anyString(), anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(2)); // Identity column mapping

    when(mockSchemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockSchemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(Arrays.asList("id", "name"));
    when(mockSchemaMapper.getSourceColumnName(anyString(), anyString(), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(2));
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("id")))
        .thenReturn(Type.int64());
    when(mockSchemaMapper.getSpannerColumnType(
            anyString(), anyString(), org.mockito.ArgumentMatchers.eq("name")))
        .thenReturn(Type.string());
    when(mockSchemaMapper.colExistsAtSource(anyString(), anyString(), anyString()))
        .thenReturn(true);

    // Mock SchemaMapper type conversion support if needed?
    // GenericRecordTypeConvertor uses schemaMapper.getSpannerColumnName/Type.
    // If not mocked, it might fail. Only simplest mocking here.

    Table mockTable = mock(Table.class);
    when(mockDdl.table(tableName)).thenReturn(mockTable);
    IndexColumn pkCol = IndexColumn.create("id", IndexColumn.Order.ASC);
    when(mockTable.primaryKeys()).thenReturn(com.google.common.collect.ImmutableList.of(pkCol));

    // We need to ensure valid type conversion can happen.
    // Mock transformer response
    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow()).thenReturn(Collections.emptyMap());
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    // Since we mock schemaMapper, we need to ensure it behaves reasonably for
    // GenericRecordTypeConvertor.

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals(tableName, record.getTableName());
    // Hash should be computed.
    assertNotNull(record.getHash());
  }
}
