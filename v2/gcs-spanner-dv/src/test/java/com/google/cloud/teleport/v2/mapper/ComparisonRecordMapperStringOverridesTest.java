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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test tests for equivalence between {@link GenericRecord} and {@link Struct} when a String
 * based schema mapper is used.
 */
@RunWith(JUnit4.class)
public class ComparisonRecordMapperStringOverridesTest {

  private ISpannerMigrationTransformer mockTransformer;
  private Ddl mockDdl;
  private SchemaStringOverridesBasedMapper schemaMapper;
  private ComparisonRecordMapper mapper;

  @Before
  public void setUp() throws Exception {
    mockTransformer = mock(ISpannerMigrationTransformer.class);
    mockDdl = mock(Ddl.class);

    // Define overrides
    // Table: SourceUsers -> SpannerUsers
    // Column: SourceUsers.user_name -> SourceUsers.name (mapped to name in Spanner)
    Map<String, String> overridesMap =
        ImmutableMap.of(
            "tableOverrides", "[{SourceUsers, SpannerUsers}]",
            "columnOverrides", "[{SourceUsers.user_name, SourceUsers.name}]");

    // Mock DDL
    when(mockDdl.dialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    Table t1 = mock(Table.class);
    when(mockDdl.table("SpannerUsers")).thenReturn(t1);
    when(t1.primaryKeys())
        .thenReturn(ImmutableList.of(IndexColumn.create("id", IndexColumn.Order.ASC)));

    // Mock columns
    com.google.cloud.teleport.v2.spanner.ddl.Column idCol =
        mock(com.google.cloud.teleport.v2.spanner.ddl.Column.class);
    when(t1.column("id")).thenReturn(idCol);
    when(idCol.type()).thenReturn(com.google.cloud.teleport.v2.spanner.type.Type.int64());
    when(idCol.name()).thenReturn("id");

    com.google.cloud.teleport.v2.spanner.ddl.Column nameCol =
        mock(com.google.cloud.teleport.v2.spanner.ddl.Column.class);
    when(t1.column("name")).thenReturn(nameCol);
    when(nameCol.type()).thenReturn(com.google.cloud.teleport.v2.spanner.type.Type.string());
    when(nameCol.name()).thenReturn("name");

    when(t1.columns()).thenReturn(ImmutableList.of(idCol, nameCol));

    // Initialize mapper
    schemaMapper = new SchemaStringOverridesBasedMapper(overridesMap, mockDdl);
    mapper = new ComparisonRecordMapper(schemaMapper, mockTransformer, mockDdl);
  }

  @Test
  public void testMapFromAvroRecord_RenamedTableAndColumn() throws Exception {
    // Payload Schema
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user_name", Schema.create(Schema.Type.STRING), null, null)));

    // Avro Schema
    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 100L);
    payload.put("user_name", "Bob");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    // Mock Transformer Behavior
    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow())
        .thenReturn(Map.of("id", Value.int64(100L), "name", Value.string("Bob")));
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals("SpannerUsers", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    // Verify hash
    assertNotNull(record.getHash());
  }

  @Test
  public void testMapFromSpannerStruct_MatchesAvro() throws Exception {
    Struct spannerStruct =
        Struct.newBuilder()
            .set("id")
            .to(100L)
            .set("name")
            .to("Bob")
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to("SpannerUsers")
            .build();

    ComparisonRecord spannerRecord = mapper.mapFrom(spannerStruct);

    // Re-create Avro record
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user_name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("id", 100L);
    payload.put("user_name", "Bob");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow())
        .thenReturn(Map.of("id", Value.int64(100L), "name", Value.string("Bob")));
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    ComparisonRecord avroComparisonRecord = mapper.mapFrom(avroRecord);

    assertEquals(spannerRecord.getHash(), avroComparisonRecord.getHash());
  }
}
