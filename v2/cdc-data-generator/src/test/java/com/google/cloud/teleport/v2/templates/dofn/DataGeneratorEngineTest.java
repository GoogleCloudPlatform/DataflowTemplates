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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LifecycleEvent;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

@RunWith(JUnit4.class)
public class DataGeneratorEngineTest {

  @Test
  public void testEngine_initializesCorrectly() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());
    assertNotNull("Engine instance should be successfully initialized", engine);
  }

  @Test
  public void testProcessRecord_throwsExceptionWhenFkColumnCasingMismatches() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("deptCode")
                        .logicalType(LogicalType.STRING)
                        .isNullable(false)
                        .isGenerated(false)
                        .build()))
            .primaryKeys(ImmutableList.of("deptCode"))
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
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("DeptCode")
                        .logicalType(LogicalType.STRING)
                        .isNullable(false)
                        .isGenerated(false)
                        .build()))
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_dept")
                        .keyColumns(ImmutableList.of("DeptCode"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("DeptCode"))
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

    Row mockRow = mock(Row.class);
    Schema parentSchema =
        Schema.builder().addField(Schema.Field.of("deptCode", Schema.FieldType.STRING)).build();
    when(mockRow.getSchema()).thenReturn(parentSchema);
    when(mockRow.getValue("deptCode")).thenReturn("HR");

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processRecord(
        "Parent",
        mockRow,
        mock(MapState.class),
        mock(ValueState.class),
        mock(MapState.class),
        mock(Timer.class),
        schema,
        batcher,
        ImmutableList.of("Parent", "Child"));

    assertEquals("Should have exactly 1 record in the DLQ", 1, dlq.size());
    String dlqPayload = dlq.get(0);
    assertTrue(
        "DLQ payload should target the Child table", dlqPayload.contains("\"table\":\"Child\""));
    assertTrue(
        "DLQ payload should state dependency error",
        dlqPayload.contains("Cannot resolve structural dependency"));
  }

  @Test
  public void testProcessRecord_tableNotFound() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    Row mockRow = mock(Row.class);
    MutationBatcher batcher = mock(MutationBatcher.class);
    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);

    engine.processRecord(
        "UnknownTable",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        schema,
        batcher,
        ImmutableList.of());

    verifyNoInteractions(batcher);
    verifyNoInteractions(eventQueueState);
    verifyNoInteractions(activeTimestamps);
    verifyNoInteractions(tableMapState);
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

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schema,
        batcher,
        ImmutableList.of("Parent", "Child"));

    assertFalse(
        "Active timestamps should be scheduled after processing parent record",
        timestampsList.isEmpty());

    LinkedHashMap<String, Object> pkMap = new LinkedHashMap<>();
    pkMap.put("id", "pk123");
    capturedEvents.add(new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, "Parent", mockRow));
    capturedEvents.add(new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, "Parent", mockRow));

    timestampsList.add(0, System.currentTimeMillis() - 5000L);

    List<String> dlq = new ArrayList<>();
    engine.processScheduledEvents(
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        batcher,
        dlq,
        ImmutableList.of("Parent", "Child"));

    assertTrue("DLQ should remain empty on successful lifecycle execution", dlq.isEmpty());
    verify(eventQueueState, atLeastOnce()).remove(ArgumentMatchers.anyLong());

    verify(batcher, atLeastOnce())
        .bufferRow(
            ArgumentMatchers.eq("Parent"),
            ArgumentMatchers.any(Row.class),
            ArgumentMatchers.eq(Constants.MUTATION_INSERT),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any());
    verify(batcher, atLeastOnce())
        .bufferRow(
            ArgumentMatchers.eq("Parent"),
            ArgumentMatchers.any(Row.class),
            ArgumentMatchers.eq(Constants.MUTATION_UPDATE),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any());
    verify(batcher, atLeastOnce())
        .bufferRow(
            ArgumentMatchers.eq("Parent"),
            ArgumentMatchers.any(Row.class),
            ArgumentMatchers.eq(Constants.MUTATION_DELETE),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any());
  }

  @Test
  public void testProcessRecord_success_compositeKeysWithDifferentNames() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn parentCol1 =
        DataGeneratorColumn.builder()
            .name("parentId1")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();
    DataGeneratorColumn parentCol2 =
        DataGeneratorColumn.builder()
            .name("parentId2")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of(parentCol1, parentCol2))
            .primaryKeys(ImmutableList.of("parentId1", "parentId2"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();

    DataGeneratorColumn childCol1 =
        DataGeneratorColumn.builder()
            .name("childFkId1")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();
    DataGeneratorColumn childCol2 =
        DataGeneratorColumn.builder()
            .name("childFkId2")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(childCol1, childCol2))
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_composite_different_names")
                        .keyColumns(ImmutableList.of("childFkId1", "childFkId2"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("parentId1", "parentId2"))
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
            .addField(Schema.Field.of("parentId1", Schema.FieldType.STRING))
            .addField(Schema.Field.of("parentId2", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchema);
    when(mockRow.getValue("parentId1")).thenReturn("parentVal1");
    when(mockRow.getValue("parentId2")).thenReturn("parentVal2");
    when(mockRow.getString(Constants.SHARD_ID_COLUMN_NAME)).thenReturn("shard1");

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);
    ReadableState<DataGeneratorTable> readableTable = mock(ReadableState.class);
    when(tableMapState.get(ArgumentMatchers.anyString())).thenReturn(readableTable);
    when(readableTable.read()).thenReturn(childTable);

    MutationBatcher batcher = mock(MutationBatcher.class);
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(dlq);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        schema,
        batcher,
        ImmutableList.of("Parent", "Child"));

    assertTrue(
        "DLQ should be empty for schema with composite columns of different names", dlq.isEmpty());

    ArgumentCaptor<Row> rowCaptor = ArgumentCaptor.forClass(Row.class);
    verify(batcher, times(2))
        .bufferRow(
            ArgumentMatchers.anyString(),
            rowCaptor.capture(),
            ArgumentMatchers.eq(Constants.MUTATION_INSERT),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.anyList());

    List<Row> capturedRows = rowCaptor.getAllValues();
    assertEquals("Should buffer exactly 2 rows (Parent + Child)", 2, capturedRows.size());

    Row parentInserted = capturedRows.get(0);
    assertEquals("parentVal1", parentInserted.getValue("parentId1"));
    assertEquals("parentVal2", parentInserted.getValue("parentId2"));

    Row childInserted = capturedRows.get(1);
    assertEquals("parentVal1", childInserted.getValue("childFkId1"));
    assertEquals("parentVal2", childInserted.getValue("childFkId2"));
  }

  @Test
  public void testProcessRecord_invalidFkConfigurationsRouteToDlq() {
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

    // --- Scenario A: Referenced Table Does Not Exist (Unresolvable FK) ---
    DataGeneratorTable childTableUnresolvableTable =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of(pkCol))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_missing")
                        .keyColumns(ImmutableList.of("id"))
                        .referencedTable("MissingAncestor") // Non-existent table name
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schemaA =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTableUnresolvableTable))
            .build();

    MutationBatcher batcherA = mock(MutationBatcher.class);
    List<String> dlqA = new ArrayList<>();
    when(batcherA.getFailedRecords()).thenReturn(dlqA);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schemaA,
        batcherA,
        ImmutableList.of("Parent", "Child"));

    assertEquals(
        "Should have exactly 1 record in DLQ for missing table constraint", 1, dlqA.size());
    String dlqPayloadA = dlqA.get(0);
    assertTrue(
        "DLQ payload should target Child table", dlqPayloadA.contains("\"table\":\"Child\""));
    assertTrue(
        "DLQ payload should show dependency resolution issue",
        dlqPayloadA.contains("Cannot resolve structural dependency"));

    // --- Scenario B: Referenced Column Does Not Exist (Missing Column) ---
    DataGeneratorTable childTableMissingColumn =
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
                        .referencedColumns(
                            ImmutableList.of("nonexistent_col")) // Non-existent column name
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schemaB =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTableMissingColumn))
            .build();

    MutationBatcher batcherB = mock(MutationBatcher.class);
    List<String> dlqB = new ArrayList<>();
    when(batcherB.getFailedRecords()).thenReturn(dlqB);

    engine.processRecord(
        "Parent",
        mockRow,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        schemaB,
        batcherB,
        ImmutableList.of("Parent", "Child"));

    assertEquals("Should have exactly 1 record in DLQ for missing parent column", 1, dlqB.size());
    String dlqPayloadB = dlqB.get(0);
    assertTrue(
        "DLQ payload should target Child table", dlqPayloadB.contains("\"table\":\"Child\""));
    assertTrue(
        "DLQ payload should show dependency resolution issue",
        dlqPayloadB.contains("Cannot resolve structural dependency"));
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
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        batcher,
        dlq,
        ImmutableList.of("Parent"));

    assertEquals("Failed scheduled execution must output to the pending DLQ list", 1, dlq.size());
    String dlqPayload = dlq.get(0);
    assertTrue(
        "DLQ must capture target table details", dlqPayload.contains("\"table\":\"Parent\""));
    assertTrue(
        "DLQ must log operation type", dlqPayload.contains("\"operation\":\"INVALID_TYPE\""));
    assertTrue(
        "DLQ must contain stack trace/exception details",
        dlqPayload.contains("Forced generation error"));
  }

  @Test
  public void testProcessScheduledEvents_success() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .isPrimaryKey(true)
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
    List<String> dlq = new ArrayList<>();
    when(batcher.getFailedRecords()).thenReturn(new ArrayList<>());

    engine.processScheduledEvents(
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        batcher,
        dlq,
        ImmutableList.of("Parent"));

    assertTrue("DLQ should be empty on successful scheduled events processing", dlq.isEmpty());
    verify(eventQueueState, atLeastOnce()).remove(ArgumentMatchers.anyLong());

    ArgumentCaptor<Row> updateCaptor = ArgumentCaptor.forClass(Row.class);
    verify(batcher)
        .bufferRow(
            ArgumentMatchers.eq("Parent"),
            updateCaptor.capture(),
            ArgumentMatchers.eq(Constants.MUTATION_UPDATE),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any());
    assertEquals("pk123", updateCaptor.getValue().getValue("id"));

    ArgumentCaptor<Row> deleteCaptor = ArgumentCaptor.forClass(Row.class);
    verify(batcher)
        .bufferRow(
            ArgumentMatchers.eq("Parent"),
            deleteCaptor.capture(),
            ArgumentMatchers.eq(Constants.MUTATION_DELETE),
            ArgumentMatchers.any(DataGeneratorTable.class),
            ArgumentMatchers.anyString(),
            ArgumentMatchers.any());
    assertEquals("pk123", deleteCaptor.getValue().getValue("id"));
  }

  @Test
  public void testProcessRecord_missingInterleavedParentRoutesToDlq() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .isPrimaryKey(true)
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
        batcher,
        ImmutableList.of("Parent", "Child"));

    assertEquals(
        "Should have exactly 1 record in DLQ for missing interleaved parent mapping",
        1,
        dlq.size());
    String dlqPayload = dlq.get(0);
    assertTrue("DLQ payload should target Child table", dlqPayload.contains("\"table\":\"Child\""));
    assertTrue(
        "DLQ payload should show dependency resolution issue",
        dlqPayload.contains("Cannot resolve structural dependency"));
  }

  @Test
  public void testProcessScheduledEvents_emptyActiveTimestamps() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    when(activeTimestamps.read()).thenReturn(Collections.emptyList());

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);

    engine.processScheduledEvents(
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        mock(MutationBatcher.class),
        new ArrayList<>(),
        ImmutableList.of());

    verifyNoInteractions(eventQueueState);
    verifyNoInteractions(tableMapState);
  }

  @Test
  public void testProcessScheduledEvents_futureTimestampBreaksLoop() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    ValueState<List<Long>> activeTimestamps = mock(ValueState.class);
    when(activeTimestamps.read())
        .thenReturn(Collections.singletonList(System.currentTimeMillis() + 100000L));

    MapState<Long, List<LifecycleEvent>> eventQueueState = mock(MapState.class);
    MapState<String, DataGeneratorTable> tableMapState = mock(MapState.class);

    engine.processScheduledEvents(
        eventQueueState,
        activeTimestamps,
        tableMapState,
        mock(Timer.class),
        mock(MutationBatcher.class),
        new ArrayList<>(),
        ImmutableList.of());

    verifyNoInteractions(eventQueueState);
    verifyNoInteractions(tableMapState);
  }

  @Test
  public void testGenerateChildRow_missingInterleavedPkColumnOnParent() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5000, 5000, new Faker());

    DataGeneratorColumn pkCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isGenerated(false)
            .isPrimaryKey(true)
            .build();

    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
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
            .interleavedInTable("Parent")
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parentTable, "Child", childTable))
            .build();

    Schema rowSchemaWithoutId =
        Schema.builder()
            .addField(Schema.Field.of("other_col", Schema.FieldType.STRING))
            .addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING))
            .build();

    Row mockRow = mock(Row.class);
    when(mockRow.getSchema()).thenReturn(rowSchemaWithoutId);
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
        batcher,
        ImmutableList.of("Parent", "Child"));

    assertEquals(
        "Should have exactly 1 record in DLQ for missing source interleaved columns",
        1,
        dlq.size());
    String dlqPayload = dlq.get(0);
    assertTrue(
        "DLQ payload should target Parent table", dlqPayload.contains("\"table\":\"Parent\""));
    assertTrue(
        "DLQ payload should show dependency resolution issue",
        dlqPayload.contains("Required Primary Key column"));
  }
}
