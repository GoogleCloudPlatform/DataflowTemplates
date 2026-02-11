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
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test tests for equivalence between {@link GenericRecord} and {@link Struct} when a session
 * file based schema mapper is used.
 */
@RunWith(JUnit4.class)
public class ComparisonRecordMapperSessionTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ISpannerMigrationTransformer mockTransformer;
  private Ddl mockDdl;
  private File sessionFile;
  private SessionBasedMapper schemaMapper;
  private ComparisonRecordMapper mapper;

  @Before
  public void setUp() throws Exception {
    mockTransformer = mock(ISpannerMigrationTransformer.class);
    mockDdl = mock(Ddl.class);

    // Create a temporary session file
    sessionFile = temporaryFolder.newFile("session.json");
    String sessionJson =
        "{\n"
            + "  \"SpSchema\": {\n"
            + "    \"t1\": {\n"
            + "      \"Name\": \"SpannerUsers\",\n"
            + "      \"ColIds\": [\"c1\", \"c2\"],\n"
            + "      \"ColDefs\": {\n"
            + "        \"c1\": {\"Name\": \"id\", \"Type\": {\"Name\": \"INT64\"}},\n"
            + "        \"c2\": {\"Name\": \"full_name\", \"Type\": {\"Name\": \"STRING\"}}\n"
            + "      },\n"
            + "      \"PrimaryKeys\": [{\"ColId\": \"c1\", \"Order\": 1}]\n"
            + "    }\n"
            + "  },\n"
            + "  \"SrcSchema\": {\n"
            + "    \"t1\": {\n"
            + "      \"Name\": \"SourceUsers\",\n"
            + "      \"ColIds\": [\"c1\", \"c2\"],\n"
            + "      \"ColDefs\": {\n"
            + "        \"c1\": {\"Name\": \"user_id\", \"Type\": {\"Name\": \"LONG\"}},\n"
            + "        \"c2\": {\"Name\": \"name\", \"Type\": {\"Name\": \"STRING\"}}\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"SyntheticPKeys\": {}\n"
            + "}";
    Files.write(sessionFile.toPath(), sessionJson.getBytes(StandardCharsets.UTF_8));

    // Mock DDL to match Spanner schema in session file
    when(mockDdl.dialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    Table t1 = mock(Table.class);
    when(mockDdl.table("SpannerUsers")).thenReturn(t1);
    when(t1.primaryKeys())
        .thenReturn(ImmutableList.of(IndexColumn.create("id", IndexColumn.Order.ASC)));
    com.google.cloud.teleport.v2.spanner.ddl.Column idCol =
        mock(com.google.cloud.teleport.v2.spanner.ddl.Column.class);
    when(t1.column("id")).thenReturn(idCol);
    when(idCol.type()).thenReturn(com.google.cloud.teleport.v2.spanner.type.Type.int64());
    com.google.cloud.teleport.v2.spanner.ddl.Column nameCol =
        mock(com.google.cloud.teleport.v2.spanner.ddl.Column.class);
    when(t1.column("full_name")).thenReturn(nameCol);
    when(nameCol.type()).thenReturn(com.google.cloud.teleport.v2.spanner.type.Type.string());

    // Initialize mapper
    schemaMapper = new SessionBasedMapper(sessionFile.getAbsolutePath(), mockDdl);
    mapper = new ComparisonRecordMapper(schemaMapper, mockTransformer, mockDdl);
  }

  @Test
  public void testMapFromAvroRecord_RenamedTableAndColumn() throws Exception {
    // Schema for Payload (Source Schema)
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("user_id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    // Schema for SourceRow (Avro wrapper)
    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("user_id", 100L);
    payload.put("name", "Alice");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    // Mock Transformer
    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow())
        .thenReturn(Map.of("id", Value.int64(100L), "full_name", Value.string("Alice")));
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    ComparisonRecord record = mapper.mapFrom(avroRecord);

    assertNotNull(record);
    assertEquals("SpannerUsers", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("id", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("100", record.getPrimaryKeyColumns().get(0).getColValue());
    assertNotNull(record.getHash());
  }

  @Test
  public void testMapFromSpannerStruct_MatchesAvro() throws Exception {
    // Spanner Struct representing the same data
    Struct spannerStruct =
        Struct.newBuilder()
            .set("id")
            .to(100L)
            .set("full_name")
            .to("Alice")
            .set("__tableName__")
            .to("SpannerUsers")
            .build();

    // Map Spanner Struct
    ComparisonRecord spannerRecord = mapper.mapFrom(spannerStruct);

    // Re-create Avro record from previous test for verification
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("user_id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("user_id", 100L);
    payload.put("name", "Alice");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    MigrationTransformationResponse mockResponse = mock(MigrationTransformationResponse.class);
    when(mockResponse.isEventFiltered()).thenReturn(false);
    when(mockResponse.getResponseRow())
        .thenReturn(Map.of("id", Value.int64(100L), "full_name", Value.string("Alice")));
    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any())).thenReturn(mockResponse);

    ComparisonRecord avroComparisonRecord = mapper.mapFrom(avroRecord);

    assertEquals(spannerRecord.getHash(), avroComparisonRecord.getHash());
    assertEquals(spannerRecord.getTableName(), avroComparisonRecord.getTableName());
  }

  @Test
  public void testMapWithNullValues() throws Exception {
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    // Nullable Name
    Schema nullableString =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("user_id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", nullableString, null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("user_id", 200L);
    payload.put("name", null);

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              return new MigrationTransformationResponse(
                  Map.of("id", Value.int64(200L), "full_name", Value.string(null)), false);
            });

    ComparisonRecord record = mapper.mapFrom(avroRecord);
    assertNotNull(record);
    assertEquals("SpannerUsers", record.getTableName());
  }

  @Test
  public void testMapWithTypeConversions() throws Exception {
    Schema payloadSchema = Schema.createRecord("Payload", null, "ns", false);
    payloadSchema.setFields(
        ImmutableList.of(
            new Schema.Field("user_id", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

    Schema avroSchema = Schema.createRecord("SourceRow", null, "ns", false);
    avroSchema.setFields(
        ImmutableList.of(
            new Schema.Field("tableName", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("shardId", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("payload", payloadSchema, null, null)));

    GenericRecord payload = new GenericData.Record(payloadSchema);
    payload.put("user_id", 300);
    payload.put("name", "Charlie");

    GenericRecord avroRecord = new GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceUsers");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    when(mockTransformer.toSpannerRow(org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            new MigrationTransformationResponse(
                Map.of("id", Value.int64(300L), "full_name", Value.string("Charlie")), false));

    ComparisonRecord record = mapper.mapFrom(avroRecord);
    assertNotNull(record);
    assertEquals("SpannerUsers", record.getTableName());
    assertEquals("300", record.getPrimaryKeyColumns().get(0).getColValue());
  }
}
