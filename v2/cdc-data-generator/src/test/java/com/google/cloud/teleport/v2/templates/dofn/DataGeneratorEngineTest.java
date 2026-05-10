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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import net.datafaker.Faker;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;

@RunWith(JUnit4.class)
public class DataGeneratorEngineTest {

  @Test
  public void testEngine_initializesCorrectly() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());
    assertNotNull(engine);
  }

  @Test(expected = IllegalStateException.class)
  public void testProcessRecord_throwsExceptionWhenFkColumnCasingMismatches() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable parentTable =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.builder()
            .name("Parent")
            .columns(
                ImmutableList.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
                        .name("deptCode") // defined in lowercase camelCase
                        .logicalType(
                            com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
                        .build()))
            .primaryKeys(ImmutableList.of("deptCode"))
            .insertQps(10)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable childTable =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.builder()
            .name("Child")
            .columns(
                ImmutableList.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
                        .name("DeptCode") // FK column matching field name
                        .logicalType(
                            com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
                        .build()))
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey.builder()
                        .name("fk_dept")
                        .keyColumns(ImmutableList.of("DeptCode"))
                        .referencedTable("Parent")
                        .referencedColumns(
                            ImmutableList.of(
                                "DeptCode")) // Mismatched Case: "DeptCode" vs "deptCode"
                        .build()))
            .insertQps(10)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Row mockRow = mock(Row.class);
    org.apache.beam.sdk.schemas.Schema parentSchema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addField(
                org.apache.beam.sdk.schemas.Schema.Field.of(
                    "deptCode", org.apache.beam.sdk.schemas.Schema.FieldType.STRING))
            .build();
    when(mockRow.getSchema()).thenReturn(parentSchema);
    when(mockRow.getValue("deptCode")).thenReturn("HR");

    engine.processRecord(
        "Parent",
        mockRow,
        mock(MapState.class),
        mock(org.apache.beam.sdk.state.ValueState.class),
        mock(MapState.class),
        mock(org.apache.beam.sdk.state.Timer.class),
        schema,
        mock(MutationBatcher.class));
  }

  @Test
  public void testProcessRecord_tableNotFound() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    engine.processRecord(
        "UnknownTable",
        mock(Row.class),
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mock(Timer.class),
        schema,
        mock(MutationBatcher.class));
  }

  @Test
  public void testProcessRecord_endToEndLifecycle() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(20)
            .deleteQps(20)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .depth(0)
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .interleavedInTable("Parent")
            .insertQps(10)
            .updateQps(20)
            .deleteQps(20)
            .isRoot(false)
            .depth(1)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Schema rowSchema =
        Schema.builder()
            .addField(Schema.Field.of("id", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("id")).thenReturn("pk123");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ReadableState<List<LifecycleEvent>> readableEvents = mock(ReadableState.class);
    when(eventQueueState.get(ArgumentMatchers.anyLong())).thenReturn(readableEvents);

    List<LifecycleEvent> capturedEvents = new ArrayList<>();
    when(readableEvents.read()).thenReturn(capturedEvents);

    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    List<Long> timestampsList = new ArrayList<>();
    when(activeTimestamps.read()).thenReturn(timestampsList);

    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    ReadableState<DataGeneratorTable> readableTable = mock(ReadableState.class);
    when(tableMapState.get(ArgumentMatchers.anyString())).thenReturn(readableTable);
    when(readableTable.read()).thenReturn(parentTable);

    MutationBatcher batcher = mock(MutationBatcher.class);
    when(batcher.getFailedRecords()).thenReturn(new ArrayList<>());

    Timer eventTimer = mock(Timer.class);

    // 1. Trigger generation and scheduling
    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schema,
        batcher);

    // 2. Ensure events were scheduled and execute scheduled events loop
    if (!timestampsList.isEmpty()) {
      // Populate captured events to simulate queue retrieval
      LinkedHashMap<String, Object> pkMap = new LinkedHashMap<>();
      pkMap.put("id", "pk123");
      capturedEvents.add(new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, "Parent", mockRow));
      capturedEvents.add(new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, "Parent", mockRow));

      engine.processScheduledEvents(
          eventQueueState, activeTimestamps, tableMapState, eventTimer, batcher, new ArrayList<>());
    }
  }

  @Test
  public void testProcessRecord_unresolvableFkRoutesToDlq() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_missing")
                        .keyColumns(ImmutableList.of("id"))
                        .referencedTable("MissingAncestor")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Schema rowSchema =
        Schema.builder()
            .addField(Schema.Field.of("id", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("id")).thenReturn("pk123");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    Timer eventTimer = mock(Timer.class);

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schema,
        batcher);

    Assert.assertFalse(dlq.isEmpty());
  }

  @Test
  public void testProcessRecord_missingFkColumnRoutesToDlq() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_missing_col")
                        .keyColumns(ImmutableList.of("id"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("nonexistent_col"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Schema rowSchema =
        Schema.builder()
            .addField(Schema.Field.of("id", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("id")).thenReturn("pk123");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    Timer eventTimer = mock(Timer.class);

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schema,
        batcher);

    Assert.assertFalse(dlq.isEmpty());
  }

  @Test
  public void testProcessScheduledEvents_catchesErrorAndRoutesToDlq() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ReadableState<List<LifecycleEvent>> readableEvents = mock(ReadableState.class);
    when(eventQueueState.get(ArgumentMatchers.anyLong())).thenReturn(readableEvents);

    List<LifecycleEvent> events = new ArrayList<>();
    events.add(
        new LifecycleEvent(new LinkedHashMap<String, Object>(), "INVALID_TYPE", "Parent", null));
    when(readableEvents.read()).thenReturn(events);

    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    when(activeTimestamps.read())
        .thenReturn(Collections.singletonList(System.currentTimeMillis() - 1000L));

    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    ReadableState<DataGeneratorTable> readableTable = mock(ReadableState.class);
    when(tableMapState.get(ArgumentMatchers.anyString())).thenReturn(readableTable);
    when(readableTable.read()).thenThrow(new RuntimeException("Forced generation error"));

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processScheduledEvents(
        eventQueueState, activeTimestamps, tableMapState, mock(Timer.class), batcher, dlq);

    Assert.assertFalse(dlq.isEmpty());
  }

  @Test
  public void testProcessScheduledEvents_success() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(ImmutableList.of())
            .build();

    Schema rowSchema =
        Schema.builder()
            .addField(Schema.Field.of("id", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("id")).thenReturn("pk123");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ReadableState<List<LifecycleEvent>> readableEvents = mock(ReadableState.class);
    when(eventQueueState.get(ArgumentMatchers.anyLong())).thenReturn(readableEvents);

    LinkedHashMap<String, Object> pkMap = new LinkedHashMap<>();
    pkMap.put("id", "pk123");

    List<LifecycleEvent> events = new ArrayList<>();
    events.add(new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, "Parent", mockRow));
    events.add(new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, "Parent", mockRow));
    when(readableEvents.read()).thenReturn(events);

    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    List<Long> pastList = new ArrayList<>();
    pastList.add(System.currentTimeMillis() - 5000L);
    when(activeTimestamps.read()).thenReturn(pastList);

    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    ReadableState<DataGeneratorTable> readableTable = mock(ReadableState.class);
    when(tableMapState.get(ArgumentMatchers.anyString())).thenReturn(readableTable);
    when(readableTable.read()).thenReturn(parentTable);

    MutationBatcher batcher = mock(MutationBatcher.class);
    when(batcher.getFailedRecords()).thenReturn(new ArrayList<>());

    engine.processScheduledEvents(
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        batcher,
        new ArrayList<>());
  }

  @Test
  public void testProcessRecord_missingInterleavedParentRoutesToDlq() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .interleavedInTable("MissingAncestor")
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Schema rowSchema =
        Schema.builder()
            .addField(Schema.Field.of("id", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("id")).thenReturn("pk123");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    Timer eventTimer = mock(Timer.class);

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schema,
        batcher);

    Assert.assertFalse(dlq.isEmpty());
  }
}
