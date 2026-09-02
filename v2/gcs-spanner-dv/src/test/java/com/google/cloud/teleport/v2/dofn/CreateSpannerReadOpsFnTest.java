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
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.config.ValidationTableConfig;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior
    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, ValidationTableConfig.empty());

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table1' as __tableName__ FROM `Table1`"));
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table2' as __tableName__ FROM `Table2`"));
  }

  @Test
  public void testProcessElementPostgres() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior for Postgres
    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.POSTGRESQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, ValidationTableConfig.empty());

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments (expecting double quotes for Postgres)
    verify(context)
        .output(
            ReadOperation.create()
                .withQuery("SELECT *, 'Table1' as __tableName__ FROM \"Table1\""));
    verify(context)
        .output(
            ReadOperation.create()
                .withQuery("SELECT *, 'Table2' as __tableName__ FROM \"Table2\""));
  }

  @Test
  public void testProcessElementWithConfiguredSubset() {
    // Spanner DDL contains TableA, TableB, TableC. The config specifies TableA, TableC.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("TableA", "TableB", "TableC"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("TableA,TableC");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Only TableA and TableC ReadOperations are generated. TableB is skipped.
    verify(context).output(ReadOperation.create().withQuery("SELECT *, 'TableA' as __tableName__ FROM `TableA`"));
    verify(context).output(ReadOperation.create().withQuery("SELECT *, 'TableC' as __tableName__ FROM `TableC`"));
  }

  @Test
  public void testProcessElementWithMissingSpannerTable() {
    // Configured Table Missing in Spanner: DDL contains TableA, TableB. Config specifies TableA, TableC.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("TableA", "TableB"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("TableA,TableC");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(1)).output(argument.capture());

    //Only TableA is queried. TableC is naturally skipped because it's not in the DDL.
    verify(context).output(ReadOperation.create().withQuery("SELECT *, 'TableA' as __tableName__ FROM `TableA`"));
  }

  @Test
  public void testProcessElementCompleteMismatch() {
    // DDL contains TableA. Config specifies TableB.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("TableA"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("TableB");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    // Completes successfully with zero ReadOperations output.
    verify(context, org.mockito.Mockito.never()).output(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testProcessElementWithSchemaMapper() {
    // Table Config specifies source_table which was renamed to spanner_table in Spanner. 
    // SchemaMapper should successfully map spanner_table to source_table.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("spanner_table"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("source_table");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    ISchemaMapper mockMapper = mock(ISchemaMapper.class);
    when(mockMapper.getSourceTableName(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.eq("spanner_table")))
        .thenReturn("source_table");

    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, (d) -> mockMapper, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(1)).output(argument.capture());

    verify(context).output(ReadOperation.create().withQuery("SELECT *, 'spanner_table' as __tableName__ FROM `spanner_table`"));
  }
}
