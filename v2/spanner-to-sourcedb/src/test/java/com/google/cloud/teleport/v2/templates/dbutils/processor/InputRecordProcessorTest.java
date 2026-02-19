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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.utils.SpannerReadUtils;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InputRecordProcessorTest {

  @Test
  public void testUpdateChangeEnventToIncludeGeneratedColumns_fetchNeeded() throws Exception {
    TrimmedShardedDataChangeRecord spannerRecord = mock(TrimmedShardedDataChangeRecord.class);
    when(spannerRecord.getTableName()).thenReturn("my_table");
    when(spannerRecord.getCommitTimestamp()).thenReturn(Timestamp.now());

    Key primaryKey = Key.of("pk_1");
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Ddl ddl = mock(Ddl.class);
    SourceSchema sourceSchema = mock(SourceSchema.class);
    SpannerDao spannerDao = mock(SpannerDao.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ObjectMapper objectMapper = mock(ObjectMapper.class);

    when(schemaMapper.getSpannerColumns(null, "my_table"))
        .thenReturn(Arrays.asList("col1", "col2", "col3"));
    when(schemaMapper.getSourceTableName(null, "my_table")).thenReturn("source_my_table");

    SourceTable sourceTable = mock(SourceTable.class);
    when(sourceSchema.table("source_my_table")).thenReturn(sourceTable);

    Table spannerTable = mock(Table.class);
    when(ddl.table("my_table")).thenReturn(spannerTable);
    IndexColumn pkCol = mock(IndexColumn.class);
    when(pkCol.name()).thenReturn("col1");
    when(spannerTable.primaryKeys()).thenReturn(ImmutableList.of(pkCol));

    // For col2: generated in Spanner, exists at source, not PK, source not
    // generated
    when(schemaMapper.isGeneratedColumn(null, "my_table", "col2")).thenReturn(true);
    when(schemaMapper.colExistsAtSource(null, "my_table", "col2")).thenReturn(true);
    when(schemaMapper.getSourceColumnName(null, "my_table", "col2")).thenReturn("src_col2");
    SourceColumn srcCol2 = mock(SourceColumn.class);
    when(srcCol2.isGenerated()).thenReturn(false);
    when(sourceTable.column("src_col2")).thenReturn(srcCol2);

    // For col3: not generated in Spanner
    when(schemaMapper.isGeneratedColumn(null, "my_table", "col3")).thenReturn(false);
    when(schemaMapper.colExistsAtSource(null, "my_table", "col3")).thenReturn(true);

    when(spannerConfig.getRpcPriority()).thenReturn(StaticValueProvider.of(RpcPriority.HIGH));

    DatabaseClient databaseClient = mock(DatabaseClient.class);
    when(spannerDao.getDatabaseClient()).thenReturn(databaseClient);

    try (MockedStatic<SpannerReadUtils> mockedSpannerReadUtils =
        Mockito.mockStatic(SpannerReadUtils.class)) {
      Struct fetchedRow = mock(Struct.class);
      // col2 should be fetched
      List<String> expectedColumnsToFetch = Collections.singletonList("col2");

      mockedSpannerReadUtils
          .when(
              () ->
                  SpannerReadUtils.readRowAsStruct(
                      eq(databaseClient),
                      eq("my_table"),
                      eq(primaryKey),
                      eq(expectedColumnsToFetch),
                      any(Timestamp.class),
                      eq(RpcPriority.HIGH)))
          .thenReturn(fetchedRow);

      Map<String, Object> rowAsMap = new HashMap<>();
      rowAsMap.put("col2", "value2");
      mockedSpannerReadUtils
          .when(
              () ->
                  SpannerReadUtils.getRowAsMap(fetchedRow, expectedColumnsToFetch, "my_table", ddl))
          .thenReturn(rowAsMap);

      InputRecordProcessor.updateChangeEnventToIncludeGeneratedColumns(
          spannerRecord,
          primaryKey,
          schemaMapper,
          ddl,
          sourceSchema,
          spannerDao,
          spannerConfig,
          objectMapper);

      mockedSpannerReadUtils.verify(
          () ->
              SpannerReadUtils.updateColumnValues(
                  eq(spannerRecord),
                  eq("source_my_table"),
                  eq(ddl),
                  eq(fetchedRow),
                  eq(rowAsMap),
                  eq(objectMapper)),
          times(1));
    }
  }

  @Test
  public void testUpdateChangeEnventToIncludeGeneratedColumns_fetchNotNeeded() throws Exception {
    TrimmedShardedDataChangeRecord spannerRecord = mock(TrimmedShardedDataChangeRecord.class);
    when(spannerRecord.getTableName()).thenReturn("my_table");

    Key primaryKey = Key.of("pk_1");
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Ddl ddl = mock(Ddl.class);
    SourceSchema sourceSchema = mock(SourceSchema.class);
    SpannerDao spannerDao = mock(SpannerDao.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ObjectMapper objectMapper = mock(ObjectMapper.class);

    when(schemaMapper.getSpannerColumns(null, "my_table"))
        .thenReturn(Arrays.asList("col1", "col2"));
    when(schemaMapper.getSourceTableName(null, "my_table")).thenReturn("source_my_table");

    SourceTable sourceTable = mock(SourceTable.class);
    when(sourceSchema.table("source_my_table")).thenReturn(sourceTable);

    Table spannerTable = mock(Table.class);
    when(ddl.table("my_table")).thenReturn(spannerTable);
    IndexColumn pkCol = mock(IndexColumn.class);
    when(pkCol.name()).thenReturn("col1");
    when(spannerTable.primaryKeys()).thenReturn(ImmutableList.of(pkCol));

    // For col2: generated in Spanner, exists at source, not PK, BUT source is
    // generated as well
    when(schemaMapper.isGeneratedColumn(null, "my_table", "col2")).thenReturn(true);
    when(schemaMapper.colExistsAtSource(null, "my_table", "col2")).thenReturn(true);
    when(schemaMapper.getSourceColumnName(null, "my_table", "col2")).thenReturn("src_col2");
    SourceColumn srcCol2 = mock(SourceColumn.class);
    when(srcCol2.isGenerated()).thenReturn(true); // Source column generated -> skip fetch
    when(sourceTable.column("src_col2")).thenReturn(srcCol2);

    try (MockedStatic<SpannerReadUtils> mockedSpannerReadUtils =
        Mockito.mockStatic(SpannerReadUtils.class)) {
      InputRecordProcessor.updateChangeEnventToIncludeGeneratedColumns(
          spannerRecord,
          primaryKey,
          schemaMapper,
          ddl,
          sourceSchema,
          spannerDao,
          spannerConfig,
          objectMapper);

      // Should not call readRowAsStruct
      mockedSpannerReadUtils.verify(
          () ->
              SpannerReadUtils.readRowAsStruct(
                  any(DatabaseClient.class),
                  anyString(),
                  any(Key.class),
                  any(),
                  any(Timestamp.class),
                  any(RpcPriority.class)),
          never());
    }
  }
}
