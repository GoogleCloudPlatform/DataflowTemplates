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
package com.google.cloud.teleport.v2.templates.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.datafaker.Faker;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Comprehensive unit tests covering code paths and lifecycle of {@link BatchAndWriteFn}. */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class BatchAndWriteFnTest {

  @Test
  public void constructor_nonPositiveBatchSize_fallsBackToDefault() {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 0, null, 10, 10, mock(PCollectionView.class), null, null);
    assertNotNull(fn);
  }

  @Test
  public void testSetup_initializesDefaultWriterAndFaker() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setup();

    assertNotNull(fn.getWriter());
    assertNotNull(fn.getFaker());
    assertNotNull(fn.getBatcher());
    assertNotNull(fn.getDataGeneratorEngine());
  }

  @Test
  public void testSetup_retainsInjectedWriterAndFaker() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    Faker mockFaker = mock(Faker.class);

    fn.setWriter(mockWriter);
    fn.setFaker(mockFaker);

    fn.setup();

    assertEquals(mockWriter, fn.getWriter());
    assertEquals(mockFaker, fn.getFaker());
  }

  @Test
  public void testStartBundle_callsBatcherStartBundle() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    MutationBatcher mockBatcher = mock(MutationBatcher.class);
    fn.setBatcher(mockBatcher);

    fn.startBundle();

    verify(mockBatcher).startBundle();
  }

  @Test
  public void testProcessElement_initializesSchemaWhenNull() throws Exception {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView = mock(PCollectionView.class);
    ProcessContext c = mock(ProcessContext.class);
    when(c.sideInput(schemaView)).thenReturn(schema);

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();
    when(c.element()).thenReturn(KV.of(0, GeneratedRecord.create("Users", row)));

    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, null, 1, null, 10, 10, schemaView, null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    fn.setWriter(mockWriter);
    fn.setup();
    fn.startBundle();

    ValueState<List<String>> mockInsertTopoOrderState = mock(ValueState.class);

    fn.processElement(
        c,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mockInsertTopoOrderState,
        mock(Timer.class));

    verify(c).sideInput(schemaView);
    verify(mockInsertTopoOrderState).write(any(List.class));
    verify(mockWriter).insert(any(), eq(users), any(), anyInt());
  }

  @Test
  public void testProcessElement_skipsSchemaInitializationWhenAlreadyLoaded() throws Exception {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView = mock(PCollectionView.class);
    ProcessContext c = mock(ProcessContext.class);
    when(c.sideInput(schemaView)).thenReturn(schema);

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();
    when(c.element()).thenReturn(KV.of(0, GeneratedRecord.create("Users", row)));

    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, null, 1, null, 10, 10, schemaView, null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    fn.setWriter(mockWriter);
    fn.setup();
    fn.startBundle();

    // Pre-populate schema and insertTopoOrder
    fn.setSchema(schema);
    fn.setInsertTopoOrder(ImmutableList.of("Users"));

    ValueState<List<String>> mockInsertTopoOrderState = mock(ValueState.class);

    fn.processElement(
        c,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mockInsertTopoOrderState,
        mock(Timer.class));

    // verify sideInput was never called since schema was already initialized
    verify(c, never()).sideInput(any());
    verify(mockInsertTopoOrderState, never()).write(any());
    verify(mockWriter).insert(any(), eq(users), any(), anyInt());
  }

  @Test
  public void testProcessElement_normalExecution_flushesDlqWhenPresent() throws Exception {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView = mock(PCollectionView.class);
    ProcessContext c = mock(ProcessContext.class);
    when(c.sideInput(schemaView)).thenReturn(schema);

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();
    when(c.element()).thenReturn(KV.of(0, GeneratedRecord.create("Users", row)));

    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, null, 1, null, 10, 10, schemaView, null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    fn.setWriter(mockWriter);
    fn.setup();
    fn.startBundle();

    MutationBatcher mockBatcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    dlq.add("dlq_record_1");
    when(mockBatcher.getFailedRecords()).thenReturn(dlq);
    fn.setBatcher(mockBatcher);

    fn.processElement(
        c,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(Timer.class));

    verify(c).output(eq("dlq_record_1"));
    verify(mockBatcher).clearDlq();
  }

  @Test
  public void testProcessElement_engineFailure_catchesAndOutputsToDlq() throws Exception {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView = mock(PCollectionView.class);
    ProcessContext c = mock(ProcessContext.class);
    when(c.sideInput(schemaView)).thenReturn(schema);

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();
    when(c.element()).thenReturn(KV.of(0, GeneratedRecord.create("Users", row)));

    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, null, 1, null, 10, 10, schemaView, null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    doThrow(new RuntimeException("simulated sink failure"))
        .when(mockWriter)
        .insert(any(), any(), any(), anyInt());
    fn.setWriter(mockWriter);
    fn.setup();
    fn.startBundle();

    fn.processElement(
        c,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(Timer.class));

    verify(c).output(any(String.class));
  }

  @Test
  public void testOnTimer_restoresInsertTopoOrderFromStateWhenNull() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setWriter(mock(DataWriter.class));
    fn.setup();
    fn.startBundle();

    fn.setDataGeneratorEngine(mock(DataGeneratorEngine.class));
    // ensure insertTopoOrder is null in memory
    fn.setInsertTopoOrder(null);

    ValueState<List<String>> mockInsertTopoOrderState = mock(ValueState.class);
    when(mockInsertTopoOrderState.read()).thenReturn(ImmutableList.of("TableA", "TableB"));

    fn.onTimer(
        mock(OnTimerContext.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mockInsertTopoOrderState,
        mock(Timer.class));

    verify(mockInsertTopoOrderState).read();
    assertEquals(ImmutableList.of("TableA", "TableB"), fn.getInsertTopoOrder());
  }

  @Test
  public void testOnTimer_skipsStateReadWhenInsertTopoOrderIsPresent() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setWriter(mock(DataWriter.class));
    fn.setup();
    fn.startBundle();

    fn.setDataGeneratorEngine(mock(DataGeneratorEngine.class));
    // Pre-populate insertTopoOrder in memory
    fn.setInsertTopoOrder(ImmutableList.of("TableA"));

    ValueState<List<String>> mockInsertTopoOrderState = mock(ValueState.class);

    fn.onTimer(
        mock(OnTimerContext.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mockInsertTopoOrderState,
        mock(Timer.class));

    verify(mockInsertTopoOrderState, never()).read();
  }

  @Test
  public void testOnTimer_exceptionRoutesToDlq() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setWriter(mock(DataWriter.class));
    fn.setup();
    fn.startBundle();

    DataGeneratorEngine mockEngine = mock(DataGeneratorEngine.class);
    doThrow(new RuntimeException("timer failure"))
        .when(mockEngine)
        .processScheduledEvents(any(), any(), any(), any(), any(), any(), any());
    fn.setDataGeneratorEngine(mockEngine);

    OnTimerContext c = mock(OnTimerContext.class);

    fn.onTimer(
        c,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(Timer.class));

    verify(c).output(any(String.class));
  }

  @Test
  public void testFinishBundle_flushesBatcherAndEmitsPendingDlq() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setWriter(mock(DataWriter.class));
    fn.setup();
    fn.startBundle();

    List<String> topoOrder = ImmutableList.of("TableA", "TableB");
    fn.setInsertTopoOrder(topoOrder);

    MutationBatcher mockBatcher = mock(MutationBatcher.class);
    when(mockBatcher.getFailedRecords()).thenReturn(Arrays.asList("dlq_record_1"));
    fn.setBatcher(mockBatcher);

    FinishBundleContext context = mock(FinishBundleContext.class);

    fn.finishBundle(context);

    verify(mockBatcher).flushInsertsInTopoOrder(eq(topoOrder));
    verify(mockBatcher).flushUpdates();
    verify(mockBatcher).flushDeletesInReverseTopoOrder(eq(topoOrder));
    verify(context).output(eq("dlq_record_1"), any(Instant.class), eq(GlobalWindow.INSTANCE));
    verify(mockBatcher).clearDlq();
  }

  @Test
  public void testTeardown_closesWriterSuccessfully() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    fn.setWriter(mockWriter);

    fn.teardown();

    verify(mockWriter).close();
  }

  @Test
  public void testTeardown_nullWriterDoesNothing() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    fn.setWriter(null);

    fn.teardown();
  }

  @Test(expected = RuntimeException.class)
  public void testTeardown_writerCloseThrowsException() throws Exception {
    BatchAndWriteFn fn =
        new BatchAndWriteFn(
            SinkType.SPANNER, null, 1, null, 10, 10, mock(PCollectionView.class), null, null);
    DataWriter mockWriter = mock(DataWriter.class);
    doThrow(new RuntimeException("simulated close error")).when(mockWriter).close();
    fn.setWriter(mockWriter);

    fn.teardown();
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private static DataGeneratorTable simpleUsersTable() {
    return DataGeneratorTable.builder()
        .name("Users")
        .columns(ImmutableList.of(intColumn("id")))
        .primaryKeys(ImmutableList.of("id"))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .insertQps(1)
        .updateQps(0)
        .deleteQps(0)
        .isRoot(true)
        .recordsPerTick(1.0)
        .build();
  }

  private static DataGeneratorColumn intColumn(String name) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(LogicalType.INT64)
        .isPrimaryKey(false)
        .isNullable(false)
        .isSkipped(false)
        .isGenerated(false)
        .size(null)
        .precision(null)
        .scale(null)
        .build();
  }
}
