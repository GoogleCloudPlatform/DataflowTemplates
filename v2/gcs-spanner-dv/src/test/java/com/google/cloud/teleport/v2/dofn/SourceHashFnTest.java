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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class SourceHashFnTest {

  private final PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider =
      mock(SerializableFunction.class);
  private final DoFn<GenericRecord, ComparisonRecord>.ProcessContext context =
      mock(DoFn.ProcessContext.class);
  private final MetricsContainerImpl metricsContainer = new MetricsContainerImpl("step");

  @Before
  public void setUp() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Ddl ddl = mock(Ddl.class);

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
  }

  @Test
  public void testProcessElement() throws IOException {
    SourceHashFn fn = new SourceHashFn(ddlView, schemaMapperProvider, null);
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      // The same record twice: duplicates are counted as processed each time.
      fn.processElement(context);
      fn.processElement(context);
    }

    ArgumentCaptor<ComparisonRecord> captor = ArgumentCaptor.forClass(ComparisonRecord.class);
    verify(context, org.mockito.Mockito.times(2)).output(captor.capture());
    assertThat(sourceRecordsProcessed()).isEqualTo(2L);
  }

  @Test
  public void testFilteredRecordIsNotCounted() throws Exception {
    ISpannerMigrationTransformer transformer = mock(ISpannerMigrationTransformer.class);
    when(transformer.toSpannerRow(any(MigrationTransformationRequest.class)))
        .thenReturn(new MigrationTransformationResponse(null, /* isEventFiltered= */ true));
    CustomTransformation customTransformation =
        CustomTransformation.builder("jar", "class").build();
    SourceHashFn fn = new SourceHashFn(ddlView, schemaMapperProvider, customTransformation);

    try (MockedStatic<CustomTransformationImplFetcher> fetcher =
            mockStatic(CustomTransformationImplFetcher.class);
        Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      fetcher
          .when(() -> CustomTransformationImplFetcher.getCustomTransformationLogicImpl(any()))
          .thenReturn(transformer);
      fn.setup();
      fn.processElement(context);
    }

    verify(transformer).toSpannerRow(any(MigrationTransformationRequest.class));
    verify(context, never()).output(any());
    assertThat(sourceRecordsProcessed()).isEqualTo(0L);
  }

  private long sourceRecordsProcessed() {
    return metricsContainer
        .getCounter(MetricName.named(SourceHashFn.class, "source_records_processed"))
        .getCumulative();
  }
}
