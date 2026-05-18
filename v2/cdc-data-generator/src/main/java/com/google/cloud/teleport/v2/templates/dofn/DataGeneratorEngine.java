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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LifecycleEvent;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import net.datafaker.Faker;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates schema tree traversal and synthesized value updates, scheduling lifecycle timer
 * events to match operational distribution patterns.
 *
 * <p><b>End-to-End Lifecycle Walkthrough Example:</b> <br>
 * Consider a schema with a Parent table (insertQps=10, updateQps=20, deleteQps=5) and a Child table
 * (insertQps=30, updateQps=300, deleteQps=30) where Child references Parent via a foreign key.
 *
 * <ol>
 *   <li><b>Root Record Ingestion (T0):</b>
 *       <ul>
 *         <li>The engine receives an initial partially-populated Parent Row.
 *         <li>It completes the Parent Row, buffering an {@code INSERT} mutation for Parent into
 *             {@link MutationBatcher}.
 *         <li>It calculates the Parent's lifecycle timing bounds based on QPS ratios: <br>
 *             - Num Updates = updateQps / insertQps = 2 updates. <br>
 *             - Delete Ratio = deleteQps / insertQps = 0.5 (50% chance of deletion). <br>
 *             - Supposing {@code updateInterval=5s} and {@code deleteInterval=5s}, if this record
 *             falls into the 50% deletion cohort, Parent schedules Update1 at T0+5s, Update2 at
 *             T0+10s, and Delete at T0+15s.
 *       </ul>
 *   <li><b>Cascading Generation, Interval Readjustment & Delete Suppression (Fan-Out to Child
 *       Tables):</b>
 *       <ul>
 *         <li>The engine calculates the child fan-out ratio: Child insertQps / Parent insertQps = 3
 *             Child records per Parent.
 *         <li>It synthesizes 3 distinct Child rows, inheriting the Parent's primary key for the
 *             foreign key relation.
 *         <li>For each Child row, it buffers a Child {@code INSERT} mutation into {@link
 *             MutationBatcher}.
 *         <li>Child has an independent deleteQps=30 configured in schema, but because its Parent
 *             table has deleteQps > 0, {@link SchemaUtils#generateSchemaDAG} overrides the Child's
 *             configured deleteQps to 0. This prevents double-deletion conflicts (where a child
 *             table independently deletes itself at a different time than its parent).
 *         <li>Instead, the Child adopts the Parent's exact forced deletion timestamp (T0+15s) as
 *             {@code forcedDeleteTimestamp}.
 *         <li>Child has a high update QPS: updateQps / insertQps = 10 updates per child record.
 *         <li><b>Dynamic Interval Compression:</b> Normally, 10 updates spaced by the default 5s
 *             update interval would require 50 seconds. However, the Child must complete all
 *             updates before the Parent's forced deletion at T0+15s.
 *         <li>The engine reserves 5s for the trailing delete interval (delInterval), leaving an
 *             active time budget of: (T0+15s) - T0 - 5s = 10 seconds.
 *         <li>To fit 10 updates into 10 seconds, the engine proportionally compresses the child's
 *             update interval: 10 seconds / 10 updates = 1-second update interval.
 *         <li>Each Child schedules 10 compressed updates (T0+1s, T0+2s, ..., T0+10s) and its forced
 *             {@code DELETE} timer at exactly T0+15s.
 *       </ul>
 *   <li><b>Lifecycle Timer Execution (T0+1s to T0+10s):</b>
 *       <ul>
 *         <li>As processing time advances, Beam fires {@code eventTimer} at each snapped 1-second
 *             bucket, invoking {@code processScheduledEvents}.
 *         <li>At T0+1s, T0+2s, T0+3s, T0+4s: Timers fire. The 3 Child records synthesize and buffer
 *             their scheduled updates into {@link MutationBatcher}.
 *         <li>At T0+5s: Timer fires. Parent Update1 and Child Update5 are synthesized and buffered.
 *         <li>At T0+6s, T0+7s, T0+8s, T0+9s: Timers fire. Child updates 6 through 9 are synthesized
 *             and buffered.
 *         <li>At T0+10s: Timer fires. Parent Update2 and final Child Update10 are synthesized and
 *             buffered.
 *       </ul>
 *   <li><b>Referential Integrity Safe Deletion (T0+15s):</b>
 *       <ul>
 *         <li>At T0+15s: The scheduled {@code DELETE} timers fire for both Parent and Child tables.
 *         <li>{@link MutationBatcher} buffers the deletion rows. When flushed at bundle completion,
 *             {@code flushDeletesInReverseTopoOrder} iterates in reverse topological order (Child
 *             before Parent).
 *         <li>All 3 Child {@code DELETE} mutations are flushed to the sink first, followed by the
 *             Parent {@code DELETE} mutation, guaranteeing zero foreign key constraint violations.
 *       </ul>
 * </ol>
 */
public class DataGeneratorEngine {
  private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorEngine.class);

  private final Integer updateInterval;
  private final Integer deleteInterval;
  private final Faker faker;

  private transient volatile DataGeneratorSchema schema;

  private final Counter insertsGenerated =
      Metrics.counter(DataGeneratorEngine.class, "insertsGenerated");
  private final Counter updatesGenerated =
      Metrics.counter(DataGeneratorEngine.class, "updatesGenerated");
  private final Counter deletesGenerated =
      Metrics.counter(DataGeneratorEngine.class, "deletesGenerated");
  private final Counter unresolvableFkChildrenDropped =
      Metrics.counter(DataGeneratorEngine.class, "unresolvableFkChildrenDropped");

  public DataGeneratorEngine(Integer updateInterval, Integer deleteInterval, Faker faker) {
    this.updateInterval = updateInterval;
    this.deleteInterval = deleteInterval;
    this.faker = faker;
  }

  /**
   * Processes an initial root record from the data generator source, initializing topological
   * orders, buffering the row, and cascading down to child tables.
   */
  public void processRecord(
      String tableName,
      Row row,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      DataGeneratorSchema loadedSchema,
      MutationBatcher batcher,
      List<String> insertTopoOrder) {

    this.schema = loadedSchema;

    DataGeneratorTable table = schema.tables().get(tableName);

    if (table == null) {
      Metrics.counter(DataGeneratorEngine.class, "tableNotFound_" + tableName).inc();
      return;
    }

    tableMapState.put(tableName, table);

    generateAndBufferInsertWithLifecycle(
        table,
        row,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        /* forcedDeleteTimestamp= */ 0L,
        /* earliestAncestorDelete= */ Long.MAX_VALUE,
        new HashMap<>(),
        batcher,
        insertTopoOrder);
  }

  /**
   * Processes all scheduled future lifecycle events (updates and deletes) for active timestamps up
   * to the current wall-clock time.
   */
  public void processScheduledEvents(
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      MutationBatcher batcher,
      List<String> pendingDlq,
      List<String> insertTopoOrder) {

    List<Long> timestamps = activeTimestamps.read();
    if (timestamps == null || timestamps.isEmpty()) {
      return;
    }

    long now = System.currentTimeMillis();
    int firstFutureIdx = 0;
    for (Long ts : timestamps) {
      // Timestamps are maintained in sorted order. Once we encounter a timestamp
      // in the future, we stop processing immediately to keep future events buffered in state.
      if (ts > now) {
        break;
      }
      List<LifecycleEvent> events = eventQueueState.get(ts).read();
      if (events != null) {
        for (LifecycleEvent event : events) {
          try {
            executeScheduledLifecycleMutation(event, tableMapState, batcher, insertTopoOrder);
          } catch (Exception genError) {
            LOG.error(
                "Lifecycle event generation failed for table {} ({})",
                event.tableName,
                event.type,
                genError);
            Metrics.counter(DataGeneratorEngine.class, "generationFailures").inc();
            pendingDlq.add(FailureRecord.toJson(event.tableName, event.type, null, genError));
          }
        }
        // Clear executed event bucket from state storage to reclaim memory
        eventQueueState.remove(ts);
      }
      firstFutureIdx++;
    }

    // Prune executed timestamps and update state tracking
    timestamps = new ArrayList<>(timestamps.subList(firstFutureIdx, timestamps.size()));
    activeTimestamps.write(timestamps);
    if (!timestamps.isEmpty()) {
      // Reset Beam timer to fire at the very next upcoming event timestamp
      eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
    }
  }

  /**
   * Assembles and buffers an insert row for the specified table, calculates lifecycle mutation
   * schedules, and cascades generation to its child tables.
   */
  private void generateAndBufferInsertWithLifecycle(
      DataGeneratorTable table,
      Row row,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      long forcedDeleteTimestamp,
      long earliestAncestorDelete,
      Map<String, Row> ancestorRows,
      MutationBatcher batcher,
      List<String> insertTopoOrder) {

    String tableName = table.name();
    tableMapState.put(tableName, table);

    // 1. Complete Row & Buffer Insert Mutation
    Row fullRow = RowAssembler.completeRow(table, row, faker);
    String shardId =
        fullRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)
            ? fullRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : "";
    insertsGenerated.inc();
    LinkedHashMap<String, Object> pkMap;
    try {
      pkMap = RowAssembler.pkValuesOf(fullRow, table);
    } catch (IllegalArgumentException e) {
      LOG.error("Primary key validation failed for table {}", tableName, e);
      if (batcher != null && batcher.getFailedRecords() != null) {
        batcher
            .getFailedRecords()
            .add(FailureRecord.toJson(tableName, Constants.MUTATION_INSERT, fullRow, e));
      }
      return;
    }

    Row reducedRow = null;
    if (!pkMap.isEmpty()) {
      reducedRow = RowAssembler.createReducedRow(fullRow, table);
    }

    batcher.bufferRow(
        tableName, fullRow, Constants.MUTATION_INSERT, table, shardId, insertTopoOrder);

    // 2. Calculate Lifecycle Timing Bounds (Updates & Deletes)
    long now = System.currentTimeMillis();
    long deleteTimestamp = 0L;
    int numUpdates = 0;
    long upInterval = this.updateInterval;
    long delInterval = this.deleteInterval;

    if (!pkMap.isEmpty()) {
      int tableInsertQps = table.insertQps();
      int tableUpdateQps = table.updateQps();
      int tableDeleteQps = table.deleteQps();

      numUpdates = calculateNumUpdates(tableInsertQps, tableUpdateQps);
      double deleteRatio = tableInsertQps > 0 ? (double) tableDeleteQps / tableInsertQps : 0.0;

      // If a parent table is scheduled for deletion, child tables adopt that exact deletion
      // timestamp
      // to guarantee consistent lifecycle teardown across the hierarchy.
      if (forcedDeleteTimestamp > 0) {
        deleteTimestamp = forcedDeleteTimestamp;
      } else if (ThreadLocalRandom.current().nextDouble() < deleteRatio) {
        deleteTimestamp = now + upInterval * numUpdates + delInterval;
      }

      // Dynamic Interval Compression: Ensure scheduled updates complete before row deletion.
      // If the standard update interval exceeds the remaining lifespan before deletion,
      // proportionally compress the update interval to fit all updates into the available budget.
      long myDeleteBound = deleteTimestamp > 0 ? deleteTimestamp : Long.MAX_VALUE;
      long effectiveDeleteBound = Math.min(myDeleteBound, earliestAncestorDelete);
      if (effectiveDeleteBound < Long.MAX_VALUE && numUpdates > 0) {
        long budget = effectiveDeleteBound - now - delInterval;
        if (upInterval * numUpdates > budget) {
          if (budget < 0) {
            upInterval = 0;
          } else {
            upInterval = budget / numUpdates;
          }
        }
      }
    }

    // 3. Cascade Generation Fan-Out to Child Tables
    Map<String, Row> updatedAncestorRows = new HashMap<>(ancestorRows);
    updatedAncestorRows.put(tableName, fullRow);

    long childEarliestAncestorDelete =
        deleteTimestamp > 0
            ? Math.min(earliestAncestorDelete, deleteTimestamp)
            : earliestAncestorDelete;

    if (table.childTables() != null) {
      for (String childTableName : table.childTables()) {
        DataGeneratorTable childTable = schema.tables().get(childTableName);
        if (childTable == null) {
          Metrics.counter(DataGeneratorEngine.class, "childTableNotFound_" + childTableName).inc();
          continue;
        }
        generateAndWriteChildren(
            table,
            fullRow,
            childTable,
            eventQueueState,
            activeTimestamps,
            tableMapState,
            eventTimer,
            deleteTimestamp,
            childEarliestAncestorDelete,
            updatedAncestorRows,
            batcher,
            insertTopoOrder);
      }
    }

    // 4. Enqueue Future Lifecycle Mutations in State
    if (!pkMap.isEmpty()) {
      for (int i = 1; i <= numUpdates; i++) {
        enqueueLifecycleEvent(
            now + upInterval * i,
            new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, tableName, reducedRow),
            eventQueueState,
            activeTimestamps,
            eventTimer);
      }
      if (deleteTimestamp > 0) {
        enqueueLifecycleEvent(
            deleteTimestamp,
            new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, tableName, reducedRow),
            eventQueueState,
            activeTimestamps,
            eventTimer);
      }
    }
  }

  /**
   * Cascades top-down row generation to child tables based on QPS ratios, inheriting referenced
   * foreign key and interleaved parent columns.
   */
  private void generateAndWriteChildren(
      DataGeneratorTable parentTable,
      Row parentRow,
      DataGeneratorTable childTable,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      long forcedDeleteTimestamp,
      long earliestAncestorDelete,
      Map<String, Row> ancestorRows,
      MutationBatcher batcher,
      List<String> insertTopoOrder) {

    int numChildren = calculateNumChildren(parentTable.insertQps(), childTable.insertQps());

    for (int i = 0; i < numChildren; i++) {
      Row childRow = generateChildRow(parentRow, childTable, ancestorRows);
      if (childRow == null) {
        unresolvableFkChildrenDropped.inc();
        batcher
            .getFailedRecords()
            .add(
                FailureRecord.toJson(
                    childTable.name(),
                    FailureRecord.OPERATION_GENERATION,
                    null,
                    new IllegalArgumentException(
                        String.format(
                            "Cannot resolve structural dependency (FK/Interleave) for table: %s",
                            childTable.name()))));
        break;
      }
      generateAndBufferInsertWithLifecycle(
          childTable,
          childRow,
          eventQueueState,
          activeTimestamps,
          tableMapState,
          eventTimer,
          forcedDeleteTimestamp,
          earliestAncestorDelete,
          ancestorRows,
          batcher,
          insertTopoOrder);
    }
  }

  /**
   * Synthesizes a single child row, resolving foreign keys and interleaved primary keys from the
   * ancestor chain.
   */
  private Row generateChildRow(
      Row parentRow, DataGeneratorTable childTable, Map<String, Row> ancestorRows) {

    Map<String, Object> columnValues = new HashMap<>();

    if (childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty()) {
      for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
        Row source = ancestorRows.get(fk.referencedTable());
        if (source == null) {
          LOG.warn(
              "Cannot resolve FK {} from {} -> {}: target table is not in the ancestor chain.",
              fk.name(),
              childTable.name(),
              fk.referencedTable());
          return null;
        }
        for (int i = 0; i < fk.keyColumns().size(); i++) {
          String refCol = fk.referencedColumns().get(i);
          if (!source.getSchema().hasField(refCol)) {
            LOG.warn(
                "Foreign key constraint '{}' references missing column '{}' on table '{}'.",
                fk.name(),
                refCol,
                fk.referencedTable());
            return null;
          }
          columnValues.put(fk.keyColumns().get(i), source.getValue(refCol));
        }
      }
    }

    if (childTable.interleavedInTable() != null) {
      String interleavedParentName = childTable.interleavedInTable();
      Row interleavedParentRow = ancestorRows.get(interleavedParentName);
      DataGeneratorTable interleavedParentTable =
          schema != null && schema.tables() != null
              ? schema.tables().get(interleavedParentName)
              : null;
      if (interleavedParentRow == null || interleavedParentTable == null) {
        LOG.warn(
            "Cannot resolve interleaved parent table '{}' for child '{}': parent is not in the ancestor chain or schema.",
            interleavedParentName,
            childTable.name());
        return null;
      }
      for (String pk : interleavedParentTable.primaryKeys()) {
        if (!interleavedParentRow.getSchema().hasField(pk)) {
          LOG.warn(
              "Interleaved child table '{}' references missing primary key column '{}' on parent '{}'.",
              childTable.name(),
              pk,
              interleavedParentName);
          return null;
        }
        Object val = interleavedParentRow.getValue(pk);
        if (val != null) {
          columnValues.put(pk, val);
        }
      }
    }

    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();

    for (DataGeneratorColumn col : childTable.columns()) {
      if (col.isSkipped()) {
        continue;
      }
      Object val;
      if (columnValues.containsKey(col.name())) {
        val = columnValues.get(col.name());
      } else {
        val = DataGeneratorUtils.generateValue(col, faker);
      }
      schemaBuilder.addField(
          Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      values.add(val);
    }

    String shardId =
        parentRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)
            ? parentRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : "";
    schemaBuilder.addField(
        Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
    values.add(shardId);
    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }

  /**
   * Persists a future lifecycle event into the state queue, keeping the active timestamp list
   * sorted via binary search.
   */
  private void enqueueLifecycleEvent(
      long timestamp,
      LifecycleEvent event,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      Timer eventTimer) {

    // Snap timer scheduling to discrete 1-second buckets to minimize state tracking overhead
    long snappedTimestamp = (timestamp / 1000) * 1000;

    List<LifecycleEvent> events = eventQueueState.get(snappedTimestamp).read();
    if (events == null) {
      events = new ArrayList<>();
    }
    events.add(event);
    eventQueueState.put(snappedTimestamp, events);

    List<Long> timestamps = activeTimestamps.read();
    if (timestamps == null) {
      timestamps = new ArrayList<>();
    }
    // Perform binary search to maintain timestamps in strictly ascending order for quick pruning
    int idx = Collections.binarySearch(timestamps, snappedTimestamp);
    if (idx < 0) {
      timestamps.add(-(idx + 1), snappedTimestamp);
      activeTimestamps.write(timestamps);
    }
    eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
  }

  /**
   * Executes a scheduled update or delete event by assembling the mutated row and buffering it into
   * the batcher.
   */
  private void executeScheduledLifecycleMutation(
      LifecycleEvent event,
      MapState<String, DataGeneratorTable> tableMapState,
      MutationBatcher batcher,
      List<String> insertTopoOrder) {

    DataGeneratorTable table = tableMapState.get(event.tableName).read();
    if (table == null) {
      return;
    }

    Row originalRow = event.reducedRow;
    String shardId =
        (originalRow != null && originalRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME))
            ? originalRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : "";

    if (Constants.MUTATION_UPDATE.equals(event.type)) {
      Row updateRow = RowAssembler.generateUpdateRow(event.pkValues, table, originalRow, faker);
      batcher.bufferRow(
          event.tableName, updateRow, Constants.MUTATION_UPDATE, table, shardId, insertTopoOrder);
      updatesGenerated.inc();
    } else if (Constants.MUTATION_DELETE.equals(event.type)) {
      Row deleteRow = RowAssembler.generateDeleteRow(event.pkValues, table);
      batcher.bufferRow(
          event.tableName, deleteRow, Constants.MUTATION_DELETE, table, shardId, insertTopoOrder);
      deletesGenerated.inc();
    }
  }

  /**
   * Probabilistically calculates the number of updates to generate for a single insert record based
   * on QPS ratios.
   */
  private int calculateNumUpdates(int insertQps, int updateQps) {
    if (insertQps <= 0 || updateQps <= 0) {
      return 0;
    }
    double ratio = (double) updateQps / insertQps;
    int count = (int) ratio;
    if (ThreadLocalRandom.current().nextDouble() < (ratio - count)) {
      count++;
    }
    return count;
  }

  /**
   * Probabilistically calculates the number of child records to fan out per parent record based on
   * QPS ratios.
   */
  private int calculateNumChildren(int parentInsertQps, int childInsertQps) {
    if (parentInsertQps <= 0 || childInsertQps <= 0) {
      return 0;
    }
    double ratio = (double) childInsertQps / parentInsertQps;
    int count = (int) ratio;
    if (faker.random().nextDouble() < (ratio - count)) {
      count++;
    }
    return count;
  }
}
