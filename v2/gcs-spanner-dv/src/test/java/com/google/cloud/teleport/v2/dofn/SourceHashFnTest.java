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
package com.google.cloud.teleport.v2.dofn;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class SourceHashFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider =
        mock(SerializableFunction.class);
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Ddl ddl = mock(Ddl.class);
    DoFn<GenericRecord, ComparisonRecord>.ProcessContext context = mock(DoFn.ProcessContext.class);

    // Prepare behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);
    when(schemaMapperProvider.apply(ddl)).thenReturn(schemaMapper);

    // Prepare Real Avro Record
    Schema payloadSchema =
        org.apache.avro.SchemaBuilder.record("payload")
            .fields()
            .name("col1")
            .type()
            .stringType()
            .noDefault()
            .endRecord();
    GenericRecord payload = new org.apache.avro.generic.GenericData.Record(payloadSchema);
    payload.put("col1", "value1");

    Schema avroSchema =
        org.apache.avro.SchemaBuilder.record("avroRecord")
            .fields()
            .name("tableName")
            .type()
            .stringType()
            .noDefault()
            .name("shardId")
            .type()
            .stringType()
            .noDefault()
            .name("payload")
            .type(payloadSchema)
            .noDefault()
            .endRecord();

    GenericRecord avroRecord = new org.apache.avro.generic.GenericData.Record(avroSchema);
    avroRecord.put("tableName", "SourceTable");
    avroRecord.put("shardId", "shard1");
    avroRecord.put("payload", payload);

    when(context.element()).thenReturn(avroRecord);

    // Mock SchemaMapper behavior
    when(schemaMapper.getSpannerTableName(anyString(), eq("SourceTable")))
        .thenReturn("SpannerTable");
    when(schemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(schemaMapper.getSpannerColumns(anyString(), eq("SpannerTable")))
        .thenReturn(ImmutableList.of("col1"));
    when(schemaMapper.getSpannerColumnType(anyString(), eq("SpannerTable"), eq("col1")))
        .thenReturn(com.google.cloud.teleport.v2.spanner.type.Type.string());
    when(schemaMapper.getSourceColumnName(anyString(), eq("SpannerTable"), eq("col1")))
        .thenReturn("col1");
    when(schemaMapper.colExistsAtSource(anyString(), eq("SpannerTable"), eq("col1")))
        .thenReturn(true);

    // Mock DDL behavior
    Table table = mock(Table.class);
    when(ddl.table("SpannerTable")).thenReturn(table);
    // Mock PKs
    IndexColumn pkColumn = mock(IndexColumn.class);
    when(pkColumn.name()).thenReturn("col1");
    when(table.primaryKeys()).thenReturn(ImmutableList.of(pkColumn));

    // Execute
    SourceHashFn fn = new SourceHashFn(ddlView, schemaMapperProvider);
    fn.processElement(context);
    fn.processElement(context);

    // Verify output
    ArgumentCaptor<ComparisonRecord> captor = ArgumentCaptor.forClass(ComparisonRecord.class);
    verify(context, org.mockito.Mockito.times(2)).output(captor.capture());
  }
}
