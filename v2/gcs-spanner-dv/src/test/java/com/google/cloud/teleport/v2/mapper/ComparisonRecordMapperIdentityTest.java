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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComparisonRecordMapperIdentityTest {

  private IdentityMapper identityMapper;
  private ISpannerMigrationTransformer mockTransformer;
  private Ddl mockDdl;
  private ComparisonRecordMapper mapper;

  @Before
  public void setUp() {
    mockDdl = mock(Ddl.class);
    when(mockDdl.dialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    identityMapper = new IdentityMapper(mockDdl);
    mockTransformer = mock(ISpannerMigrationTransformer.class);
    mapper = new ComparisonRecordMapper(identityMapper, mockTransformer, mockDdl);
  }

  @Test
  public void testMapFromAvroRecord_Identity() throws Exception {
    String tableName = "Users";
    setupDdl(tableName, Arrays.asList("id", "name"), Arrays.asList(Type.int64(), Type.string()));

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
    avroRecord.put("tableName", tableName);
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow()).thenReturn(Collections.emptyMap());
    when(mockTransformer.toSpannerRow(any())).thenReturn(mockResponse);

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals(tableName, record.getTableName());
    assertNotNull(record.getHash());
  }

  @Test
  public void testMapFromSpannerStruct_Identity() throws Exception {
    String tableName = "Users";
    setupDdl(tableName, Arrays.asList("id", "name"), Arrays.asList(Type.int64(), Type.string()));

    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to("Alice")
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to(tableName)
            .build();

    ComparisonRecord record = mapper.mapFrom(struct);

    assertNotNull(record);
    assertEquals(tableName, record.getTableName());
    assertNotNull(record.getHash());
  }

  @Test
  public void testMapWithNullValues() throws Exception {
    String tableName = "Users";
    setupDdl(tableName, Arrays.asList("id", "name"), Arrays.asList(Type.int64(), Type.string()));

    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to((String) null)
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to(tableName)
            .build();

    ComparisonRecord record = mapper.mapFrom(struct);

    assertNotNull(record);
    assertEquals(tableName, record.getTableName());
  }

  @Test
  public void testMapFromSpannerStruct_MatchesAvro() throws Exception {
    String tableName = "Users";
    setupDdl(tableName, Arrays.asList("id", "name"), Arrays.asList(Type.int64(), Type.string()));

    // 1. Create Avro Record
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

    GenericRecord avroGenericRecord = new GenericData.Record(avroSchema);
    avroGenericRecord.put("tableName", tableName);
    avroGenericRecord.put("shardId", "shard1");
    avroGenericRecord.put("payload", payload);

    // 2. Create Spanner Struct
    Struct spannerStruct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to("Alice")
            .set(GCSSpannerDVConstants.TABLE_NAME_COLUMN)
            .to(tableName)
            .build();

    // 3. Mock Transformer (Required for Avro path)
    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow()).thenReturn(Collections.emptyMap());
    when(mockTransformer.toSpannerRow(any())).thenReturn(mockResponse);

    // 4. Map both
    ComparisonRecord avroRec = mapper.mapFrom(avroGenericRecord);
    ComparisonRecord spannerRec = mapper.mapFrom(spannerStruct);

    // 5. Assertions
    assertNotNull(avroRec);
    assertNotNull(spannerRec);
    assertEquals(tableName, avroRec.getTableName());
    assertEquals(tableName, spannerRec.getTableName());
    assertEquals(
        "Hashes should match for equivalent data", avroRec.getHash(), spannerRec.getHash());
  }

  private void setupDdl(String tableName, List<String> columns, List<Type> types) {
    Table mockTable = mock(Table.class);
    when(mockDdl.table(tableName)).thenReturn(mockTable);

    // Setup primary key (assume first column is PK)
    IndexColumn pkCol = IndexColumn.create(columns.get(0), IndexColumn.Order.ASC);
    when(mockTable.primaryKeys()).thenReturn(ImmutableList.of(pkCol));

    // Setup columns
    ImmutableList.Builder<Column> colBuilder = ImmutableList.builder();
    for (int i = 0; i < columns.size(); i++) {
      Column col = mock(Column.class);
      when(col.name()).thenReturn(columns.get(i));
      when(col.type()).thenReturn(types.get(i));
      when(mockTable.column(columns.get(i))).thenReturn(col);
      colBuilder.add(col);
    }
    when(mockTable.columns()).thenReturn(colBuilder.build());
  }
}
