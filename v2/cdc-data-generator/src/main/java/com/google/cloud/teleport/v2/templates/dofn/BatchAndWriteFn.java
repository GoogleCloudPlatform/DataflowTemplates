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

import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.model.LifecycleEvent;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.sink.DataWriterFactory;
import com.google.cloud.teleport.v2.templates.utils.CustomDataGeneratorFetcher;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.function.Consumer;
import net.datafaker.Faker;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful {@link DoFn} that manages persistence lifecycles, state fields, and timers, delegating
 * core traversal to {@link DataGeneratorEngine} and mutation batch writing to {@link
 * MutationBatcher}.
 */
public class BatchAndWriteFn extends DoFn<KV<Integer, GeneratedRecord>, String> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchAndWriteFn.class);

  private final SinkType sinkType;
  private final SinkConfig sinkConfig;
  private final int batchSize;
  private final Integer jdbcPoolSize;
  private final Integer updateInterval;
  private final Integer deleteInterval;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  private transient DataWriter writer;
  private transient Faker faker;
  private transient CustomDataGenerator customGenerator;
  private transient volatile DataGeneratorSchema schema;
  private transient volatile List<String> insertTopoOrder;

  private transient DataGeneratorEngine dataGeneratorEngine;
  private transient MutationBatcher batcher;
  private final String customJarPath;
  private final String customClassName;

  @StateId("eventQueue")
  private final StateSpec<MapState<Long, List<LifecycleEvent>>> eventQueueSpec =
      StateSpecs.map(VarLongCoder.of(), ListCoder.of(SerializableCoder.of(LifecycleEvent.class)));

  @StateId("activeTimestamps")
  private final StateSpec<ValueState<List<Long>>> activeTimestampsSpec =
      StateSpecs.value(ListCoder.of(VarLongCoder.of()));

  @StateId("tableMapState")
  private final StateSpec<MapState<String, DataGeneratorTable>> tableMapSpec =
      StateSpecs.map(StringUtf8Coder.of(), SerializableCoder.of(DataGeneratorTable.class));

  @StateId("insertTopoOrderState")
  private final StateSpec<ValueState<List<String>>> insertTopoOrderSpec =
      StateSpecs.value(ListCoder.of(StringUtf8Coder.of()));

  @TimerId("eventTimer")
  private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  public BatchAndWriteFn(
      SinkType sinkType,
      SinkConfig sinkConfig,
      Integer batchSize,
      Integer jdbcPoolSize,
      Integer updateInterval,
      Integer deleteInterval,
      PCollectionView<DataGeneratorSchema> schemaView,
      String customJarPath,
      String customClassName) {
    this.sinkType = sinkType;
    this.sinkConfig = sinkConfig;
    this.batchSize = batchSize;
    this.jdbcPoolSize = jdbcPoolSize;
    this.updateInterval = updateInterval;
    this.deleteInterval = deleteInterval;
    this.schemaView = schemaView;
    this.customJarPath = customJarPath;
    this.customClassName = customClassName;
  }

  @Setup
  public void setup() {
    this.customGenerator =
        CustomDataGeneratorFetcher.getCustomDataGenerator(customJarPath, customClassName);
    this.schema = null;
    this.insertTopoOrder = null;
    if (writer == null) {
      writer = DataWriterFactory.createWriter(sinkType, sinkConfig);
    }
    if (faker == null) {
      faker = new Faker();
    }

    this.batcher = new MutationBatcher(batchSize, jdbcPoolSize, writer);
    this.dataGeneratorEngine =
        new DataGeneratorEngine(updateInterval, deleteInterval, faker, customGenerator);
  }

  @StartBundle
  public void startBundle() {
    this.batcher.startBundle();
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
      @StateId("activeTimestamps") ValueState<List<Long>> activeTimestamps,
      @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
      @StateId("insertTopoOrderState") ValueState<List<String>> insertTopoOrderState,
      @TimerId("eventTimer") Timer eventTimer) {

    ensureSchemaInitialized(c, insertTopoOrderState);

    GeneratedRecord record = c.element().getValue();
    String tableName = record.tableName();
    Row pkValues = record.primaryKeyValues();

    try {
      dataGeneratorEngine.processRecord(
          tableName,
          pkValues,
          eventQueueState,
          activeTimestamps,
          tableMapState,
          eventTimer,
          schema,
          batcher,
          insertTopoOrder);
    } catch (Exception genError) {
      LOG.error("Generation failed for table {}", tableName, genError);
      Metrics.counter(BatchAndWriteFn.class, "generationFailures").inc();
      batcher
          .getFailedRecords()
          .add(
              FailureRecord.toJson(
                  tableName, FailureRecord.OPERATION_GENERATION, pkValues, genError));
    }

    writeFailedRecords(c::output);
  }

  @OnTimer("eventTimer")
  public void onTimer(
      OnTimerContext c,
      @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
      @StateId("activeTimestamps") ValueState<List<Long>> activeTimestamps,
      @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
      @StateId("insertTopoOrderState") ValueState<List<String>> insertTopoOrderState,
      @TimerId("eventTimer") Timer eventTimer) {

    if (this.insertTopoOrder == null) {
      this.insertTopoOrder = insertTopoOrderState.read();
    }

    try {
      dataGeneratorEngine.processScheduledEvents(
          eventQueueState,
          activeTimestamps,
          tableMapState,
          eventTimer,
          batcher,
          batcher.getFailedRecords(),
          this.insertTopoOrder);
    } catch (Exception timerError) {
      LOG.error("Scheduled events generation failed during timer processing", timerError);
      Metrics.counter(BatchAndWriteFn.class, "generationFailures").inc();
      batcher.getFailedRecords().add(FailureRecord.toJson("UNKNOWN_TABLE", null, null, timerError));
    }

    writeFailedRecords(c::output);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    batcher.flushInsertsInTopoOrder(insertTopoOrder);
    batcher.flushUpdates();
    batcher.flushDeletesInReverseTopoOrder(insertTopoOrder);

    List<String> pendingDlq = batcher.getFailedRecords();
    if (pendingDlq != null && !pendingDlq.isEmpty()) {
      Instant now = Instant.now();
      for (String record : pendingDlq) {
        c.output(record, now, GlobalWindow.INSTANCE);
      }
      batcher.clearDlq();
    }
  }

  @Teardown
  public void teardown() {
    if (writer != null) {
      try {
        writer.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to close writer", e);
      }
    }
  }

  private void ensureSchemaInitialized(
      ProcessContext c, ValueState<List<String>> insertTopoOrderState) {
    if (schema != null && insertTopoOrder != null) {
      return;
    }
    DataGeneratorSchema loaded = c.sideInput(schemaView);
    this.insertTopoOrder = SchemaUtils.buildInsertTopoOrder(loaded);
    insertTopoOrderState.write(this.insertTopoOrder);
    this.schema = loaded;
  }

  private void writeFailedRecords(Consumer<String> sink) {
    List<String> dlq = batcher.getFailedRecords();
    if (dlq == null || dlq.isEmpty()) {
      return;
    }
    for (String record : dlq) {
      sink.accept(record);
    }
    batcher.clearDlq();
  }

  @VisibleForTesting
  DataWriter getWriter() {
    return writer;
  }

  @VisibleForTesting
  void setWriter(DataWriter writer) {
    this.writer = writer;
  }

  @VisibleForTesting
  Faker getFaker() {
    return faker;
  }

  @VisibleForTesting
  void setFaker(Faker faker) {
    this.faker = faker;
  }

  @VisibleForTesting
  DataGeneratorSchema getSchema() {
    return schema;
  }

  @VisibleForTesting
  void setSchema(DataGeneratorSchema schema) {
    this.schema = schema;
  }

  @VisibleForTesting
  List<String> getInsertTopoOrder() {
    return insertTopoOrder;
  }

  @VisibleForTesting
  void setInsertTopoOrder(List<String> insertTopoOrder) {
    this.insertTopoOrder = insertTopoOrder;
  }

  @VisibleForTesting
  DataGeneratorEngine getDataGeneratorEngine() {
    return dataGeneratorEngine;
  }

  @VisibleForTesting
  void setDataGeneratorEngine(DataGeneratorEngine dataGeneratorEngine) {
    this.dataGeneratorEngine = dataGeneratorEngine;
  }

  @VisibleForTesting
  MutationBatcher getBatcher() {
    return batcher;
  }

  @VisibleForTesting
  void setBatcher(MutationBatcher batcher) {
    this.batcher = batcher;
  }
}
