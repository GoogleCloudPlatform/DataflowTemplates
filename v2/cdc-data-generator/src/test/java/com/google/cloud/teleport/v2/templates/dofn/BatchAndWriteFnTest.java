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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BatchAndWriteFn}. */
@RunWith(JUnit4.class)
public class BatchAndWriteFnTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void resetRecorder() {
    RecordingDataWriter.reset();
    FailingDataWriter.reset();
  }

  // ===========================================================================
  // Constructor / config defaults
  // ===========================================================================

  @Test
  public void constructor_nullBatchSize_fallsBackToDefault() {
    BatchAndWriteFn fn = new BatchAndWriteFn(SinkType.SPANNER, "{}", null, null, null, null, null);
    assertThat(BatchAndWriteFn.DEFAULT_BATCH_SIZE).isGreaterThan(0);
    assertThat(fn).isNotNull();
  }

  @Test
  public void constructor_nonPositiveBatchSize_fallsBackToDefault() {
    BatchAndWriteFn fn = new BatchAndWriteFn(SinkType.SPANNER, "{}", 0, null, null, null, null);
    assertThat(fn).isNotNull();
  }

  // ===========================================================================
  // End-to-end: a single insert flows through to DataWriter.insert
  //
  // batchSize = 1 forces a flush per element so we can assert the writer call
  // even with DirectRunner's per-element DoFn instantiation. We use a static
  // RecordingDataWriter rather than a Mockito mock because Mockito mocks are
  // not reliably serializable across DirectRunner's bundle boundary.
  // ===========================================================================

  @Test
  public void processElement_singleRow_invokesWriterInsert() {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();

    pipeline
        .apply("Input", Create.of(KV.of(0, GeneratedRecord.create("Users", row))))
        .apply(
            "BatchAndWrite",
            ParDo.of(new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaView))
                .withSideInputs(schemaView));

    pipeline.run().waitUntilFinish();

    assertThat(RecordingDataWriter.INSERT_CALL_COUNT.get()).isAtLeast(1);
    assertThat(RecordingDataWriter.INSERTED_ROW_COUNT.get()).isAtLeast(1);
    assertThat(RecordingDataWriter.INSERTED_TABLE_NAMES).contains("Users");
  }

  @Test
  public void processElement_unknownTable_isDroppedWithoutCallingWriter() {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();

    pipeline
        .apply("Input", Create.of(KV.of(0, GeneratedRecord.create("MissingTable", row))))
        .apply(
            "BatchAndWrite",
            ParDo.of(new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaView))
                .withSideInputs(schemaView));

    pipeline.run().waitUntilFinish();

    assertThat(RecordingDataWriter.INSERT_CALL_COUNT.get()).isEqualTo(0);
  }

  // ===========================================================================
  // Parent → child cascade
  //
  // Schema: Users (root) → Orders (child via FK Orders.user_id → Users.id).
  // One Users row in → both Users and Orders should reach the writer.
  // ===========================================================================

  @Test
  public void processElement_parentChildCascade_writesBothTables() {
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .childTables(ImmutableList.of("Orders"))
            .build();

    DataGeneratorForeignKey fk =
        DataGeneratorForeignKey.builder()
            .name("orders_user_fk")
            .referencedTable("Users")
            .keyColumns(ImmutableList.of("user_id"))
            .referencedColumns(ImmutableList.of("id"))
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Orders")
            .columns(ImmutableList.of(intColumn("id"), intColumn("user_id")))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of(fk))
            .uniqueKeys(ImmutableList.of())
            .insertQps(1) // 1:1 cascade so we always emit one child per parent.
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Users", parent, "Orders", child))
            .build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").addStringField("name").build();
    Row userRow = Row.withSchema(rowSchema).addValues(1L, "Alice").build();

    pipeline
        .apply("Input", Create.of(KV.of(0, GeneratedRecord.create("Users", userRow))))
        .apply(
            "BatchAndWrite",
            ParDo.of(new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaView))
                .withSideInputs(schemaView));

    pipeline.run().waitUntilFinish();

    assertThat(RecordingDataWriter.INSERTED_TABLE_NAMES).containsAtLeast("Users", "Orders");
    assertThat(RecordingDataWriter.INSERT_CALL_COUNT.get()).isAtLeast(2);
  }

  // ===========================================================================
  // Sink-write failure → DLQ output
  //
  // The writer throws on every insert. The pipeline must NOT crash; the DLQ
  // PCollection<String> must receive at least one FailureRecord JSON line.
  // ===========================================================================

  @Test
  public void processElement_sinkWriteFailure_routesRowToDlqOutput() {
    DataGeneratorTable users = simpleUsersTable();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of(users.name(), users)).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValue(1L).build();

    PCollection<String> dlq =
        pipeline
            .apply("Input", Create.of(KV.of(0, GeneratedRecord.create("Users", row))))
            .apply(
                "BatchAndWrite",
                ParDo.of(new FailingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaView))
                    .withSideInputs(schemaView));

    PAssert.thatSingleton(dlq.apply("Count", Count.globally())).isEqualTo(1L);

    pipeline.run().waitUntilFinish();

    assertThat(FailingDataWriter.INSERT_CALL_COUNT.get()).isAtLeast(1);
  }

  // ===========================================================================
  // Unresolvable FK → child skipped, parent succeeds
  //
  // Schema: Parent (root) declares Child as a child table. Child has an FK to
  // a third table "Stranger" which is NOT in the parent's ancestor chain.
  // Expectation: Parent insert lands; Child insert is skipped; pipeline does
  // not crash.
  // ===========================================================================

  @Test
  public void processElement_unresolvableFk_skipsChildButWritesParent() {
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .childTables(ImmutableList.of("Child"))
            .build();

    // Child FK targets "Stranger" — a table never placed in Child's ancestor chain when
    // BatchAndWriteFn recurses from Parent → Child.
    DataGeneratorForeignKey badFk =
        DataGeneratorForeignKey.builder()
            .name("child_to_stranger")
            .referencedTable("Stranger")
            .keyColumns(ImmutableList.of("stranger_id"))
            .referencedColumns(ImmutableList.of("id"))
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(intColumn("id"), intColumn("stranger_id")))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of(badFk))
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("MkSchema", Create.of(schema)).apply("AsView", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").addStringField("name").build();
    Row parentRow = Row.withSchema(rowSchema).addValues(1L, "Alice").build();

    pipeline
        .apply("Input", Create.of(KV.of(0, GeneratedRecord.create("Parent", parentRow))))
        .apply(
            "BatchAndWrite",
            ParDo.of(new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaView))
                .withSideInputs(schemaView));

    pipeline.run().waitUntilFinish();

    assertThat(RecordingDataWriter.INSERTED_TABLE_NAMES).contains("Parent");
    assertThat(RecordingDataWriter.INSERTED_TABLE_NAMES).doesNotContain("Child");
  }

  @Test
  public void testCreateWriterMySql() {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.MYSQL, "{}", 1, null, null, null, schemaSideInput());
    DataWriter writer = fn.createWriter(SinkType.MYSQL, "{}");
    assertThat(writer).isNotNull();
  }

  @Test
  public void testCreateWriterSpanner() {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, "{}", 1, null, null, null, schemaSideInput());
    DataWriter writer = fn.createWriter(SinkType.SPANNER, "{}");
    assertThat(writer).isNotNull();
  }

  @Test(expected = NullPointerException.class)
  public void testCreateWriterUnsupportedThrowsException() {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn =
        new BatchAndWriteFn(SinkType.SPANNER, "{}", 1, null, null, null, schemaSideInput());
    fn.createWriter(null, "{}");
  }

  @Test
  public void testOnTimer() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn = new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaSideInput());
    fn.setup();
    java.lang.reflect.Field engineField = BatchAndWriteFn.class.getDeclaredField("engine");
    engineField.setAccessible(true);
    engineField.set(fn, org.mockito.Mockito.mock(DataGeneratorEngine.class));

    org.apache.beam.sdk.transforms.DoFn<KV<Integer, GeneratedRecord>, String>.OnTimerContext
        context =
            org.mockito.Mockito.mock(org.apache.beam.sdk.transforms.DoFn.OnTimerContext.class);

    fn.onTimer(
        context,
        org.mockito.Mockito.mock(org.apache.beam.sdk.state.MapState.class),
        org.mockito.Mockito.mock(org.apache.beam.sdk.state.ValueState.class),
        org.mockito.Mockito.mock(org.apache.beam.sdk.state.MapState.class),
        org.mockito.Mockito.mock(org.apache.beam.sdk.state.Timer.class));
  }

  @Test
  public void testFinishBundleWithDlq() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn = new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaSideInput());
    fn.setup();

    MutationBatcher mockBatcher = org.mockito.Mockito.mock(MutationBatcher.class);
    org.mockito.Mockito.when(mockBatcher.getFailedRecords())
        .thenReturn(java.util.Arrays.asList("dlq_record_1"));

    java.lang.reflect.Field batcherField = BatchAndWriteFn.class.getDeclaredField("batcher");
    batcherField.setAccessible(true);
    batcherField.set(fn, mockBatcher);

    org.apache.beam.sdk.transforms.DoFn<KV<Integer, GeneratedRecord>, String>.FinishBundleContext
        context =
            org.mockito.Mockito.mock(org.apache.beam.sdk.transforms.DoFn.FinishBundleContext.class);

    fn.finishBundle(context);

    org.mockito.Mockito.verify(context)
        .output(
            org.mockito.Mockito.eq("dlq_record_1"),
            org.mockito.Mockito.any(org.joda.time.Instant.class),
            org.mockito.Mockito.eq(org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE));
    org.mockito.Mockito.verify(mockBatcher).clearDlq();
  }

  @Test
  public void testTeardownClosesWriter() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);
    BatchAndWriteFn fn = new RecordingBatchAndWriteFn(SinkType.SPANNER, "{}", 1, schemaSideInput());
    DataWriter mockWriter = org.mockito.Mockito.mock(DataWriter.class);

    java.lang.reflect.Field writerField = BatchAndWriteFn.class.getDeclaredField("writer");
    writerField.setAccessible(true);
    writerField.set(fn, mockWriter);

    fn.teardown();

    org.mockito.Mockito.verify(mockWriter).close();
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

  private static DataGeneratorColumn stringColumn(String name, boolean nullable) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(LogicalType.STRING)
        .isPrimaryKey(false)
        .isNullable(nullable)
        .isSkipped(false)
        .isGenerated(false)
        .size(20L)
        .precision(null)
        .scale(null)
        .build();
  }

  /** Minimal table for topo-order tests (no columns / FK / unique keys). */
  private static DataGeneratorTable.Builder tableBuilder(String name, boolean isRoot) {
    return DataGeneratorTable.builder()
        .name(name)
        .columns(ImmutableList.of())
        .primaryKeys(ImmutableList.of())
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .insertQps(1)
        .updateQps(0)
        .deleteQps(0)
        .isRoot(isRoot)
        .recordsPerTick(1.0);
  }

  /** Trivial side input used by constructor tests that never run the pipeline. */
  private PCollectionView<DataGeneratorSchema> schemaSideInput() {
    DataGeneratorSchema empty = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    return pipeline.apply("EmptySchema", Create.of(empty)).apply("AsView", View.asSingleton());
  }

  /**
   * DataWriter that records call counts and the set of tables it received in static fields, so the
   * test can assert against them after the pipeline finishes — avoids Mockito's serialization
   * pitfalls under DirectRunner.
   */
  static class RecordingDataWriter implements DataWriter, Serializable {
    private static final long serialVersionUID = 1L;

    static final AtomicInteger INSERT_CALL_COUNT = new AtomicInteger();
    static final AtomicInteger INSERTED_ROW_COUNT = new AtomicInteger();
    static final AtomicInteger UPDATE_CALL_COUNT = new AtomicInteger();
    static final AtomicInteger DELETE_CALL_COUNT = new AtomicInteger();
    static final AtomicInteger CLOSE_CALL_COUNT = new AtomicInteger();
    static final Set<String> INSERTED_TABLE_NAMES =
        Collections.newSetFromMap(new ConcurrentHashMap<>());

    static void reset() {
      INSERT_CALL_COUNT.set(0);
      INSERTED_ROW_COUNT.set(0);
      UPDATE_CALL_COUNT.set(0);
      DELETE_CALL_COUNT.set(0);
      CLOSE_CALL_COUNT.set(0);
      INSERTED_TABLE_NAMES.clear();
    }

    @Override
    public void insert(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
      INSERT_CALL_COUNT.incrementAndGet();
      INSERTED_ROW_COUNT.addAndGet(rows == null ? 0 : rows.size());
      if (table != null) {
        INSERTED_TABLE_NAMES.add(table.name());
      }
    }

    @Override
    public void update(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
      UPDATE_CALL_COUNT.incrementAndGet();
    }

    @Override
    public void delete(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
      DELETE_CALL_COUNT.incrementAndGet();
    }

    @Override
    public void close() {
      CLOSE_CALL_COUNT.incrementAndGet();
    }
  }

  /** Writer that throws on every insert; used to drive the DLQ path. */
  static class FailingDataWriter implements DataWriter, Serializable {
    private static final long serialVersionUID = 1L;
    static final AtomicInteger INSERT_CALL_COUNT = new AtomicInteger();

    static void reset() {
      INSERT_CALL_COUNT.set(0);
    }

    @Override
    public void insert(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
      INSERT_CALL_COUNT.incrementAndGet();
      throw new RuntimeException("simulated sink failure");
    }

    @Override
    public void update(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {}

    @Override
    public void delete(
        List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {}

    @Override
    public void close() {}
  }

  /** {@link BatchAndWriteFn} that swaps the real writer factory for {@link RecordingDataWriter}. */
  static class RecordingBatchAndWriteFn extends BatchAndWriteFn {
    RecordingBatchAndWriteFn(
        SinkType sinkType,
        String sinkOptionsPath,
        Integer batchSize,
        PCollectionView<DataGeneratorSchema> schemaView) {
      super(sinkType, sinkOptionsPath, batchSize, null, null, null, schemaView);
    }

    @Override
    protected DataWriter createWriter(SinkType type, String configPath) {
      return new RecordingDataWriter();
    }
  }

  /** {@link BatchAndWriteFn} that swaps the real writer factory for {@link FailingDataWriter}. */
  static class FailingBatchAndWriteFn extends BatchAndWriteFn {
    FailingBatchAndWriteFn(
        SinkType sinkType,
        String sinkOptionsPath,
        Integer batchSize,
        PCollectionView<DataGeneratorSchema> schemaView) {
      super(sinkType, sinkOptionsPath, batchSize, null, null, null, schemaView);
    }

    @Override
    protected DataWriter createWriter(SinkType type, String configPath) {
      return new FailingDataWriter();
    }
  }
}
