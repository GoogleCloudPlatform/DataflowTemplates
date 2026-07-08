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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class SpannerHashFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider =
        mock(SerializableFunction.class);
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Ddl ddl = mock(Ddl.class);
    DoFn<Struct, ComparisonRecord>.ProcessContext context = mock(DoFn.ProcessContext.class);

    // Prepare behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);
    when(schemaMapperProvider.apply(ddl)).thenReturn(schemaMapper);

    // Prepare Input Struct
    Struct spannerStruct =
        Struct.newBuilder().set("id").to("123").set("__tableName__").to("SpannerTable").build();
    when(context.element()).thenReturn(spannerStruct);

    // Mock DDL behavior
    Table table = mock(Table.class);
    when(ddl.table("SpannerTable")).thenReturn(table);
    // Mock PKs
    IndexColumn pkColumn = mock(IndexColumn.class);
    when(pkColumn.name()).thenReturn("id");
    when(table.primaryKeys()).thenReturn(ImmutableList.of(pkColumn));

    // Execute
    SpannerHashFn fn = new SpannerHashFn(ddlView, schemaMapperProvider);
    fn.processElement(context);
    fn.processElement(context);

    // Verify output
    ArgumentCaptor<ComparisonRecord> captor = ArgumentCaptor.forClass(ComparisonRecord.class);
    verify(context, org.mockito.Mockito.times(2)).output(captor.capture());

    // We can verify properties if needed
  }
}
