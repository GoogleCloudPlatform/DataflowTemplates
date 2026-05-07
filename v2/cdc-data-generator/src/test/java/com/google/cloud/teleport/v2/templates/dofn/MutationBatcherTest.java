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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MutationBatcherTest {

  private DataWriter mockWriter;
  private MutationBatcher batcher;
  private DataGeneratorTable sampleTable;
  private Schema rowSchema;
  private Row sampleRow;
  private List<String> topoOrder;

  @Before
  public void setUp() {
    mockWriter = mock(DataWriter.class);
    batcher = new MutationBatcher(2, 10, mockWriter);
    batcher.startBundle();

    sampleTable =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .recordsPerTick(1.0)
            .build();

    rowSchema = Schema.builder().addInt64Field("id").build();
    sampleRow = Row.withSchema(rowSchema).addValue(1L).build();
    topoOrder = Arrays.asList("Users");
  }

  @Test
  public void testBufferRow_triggersFlushOnThreshold() {
    // Threshold is set to 2. Inserting 1st row should not trigger writer.
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    verify(mockWriter, times(0)).insert(any(), any(), anyString(), anyInt());

    // Inserting 2nd row should cross threshold and flush inserts in topo order.
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    verify(mockWriter, times(1)).insert(any(), any(), anyString(), anyInt());
  }

  @Test
  public void testBufferRow_sinkErrorRoutesToDlq() {
    doThrow(new RuntimeException("Sink error simulation"))
        .when(mockWriter)
        .insert(any(), any(), anyString(), anyInt());

    assertTrue(batcher.getFailedRecords().isEmpty());

    // Push two rows to force threshold trip and catch error routing
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_INSERT, sampleTable, "shard0", topoOrder);

    assertFalse(batcher.getFailedRecords().isEmpty());
    assertEquals(2, batcher.getFailedRecords().size());
  }

  @Test
  public void testBufferRow_withShardIdFromRowSchema() {
    Schema schemaWithShard =
        Schema.builder().addInt64Field("id").addStringField(Constants.SHARD_ID_COLUMN_NAME).build();
    Row rowWithShard = Row.withSchema(schemaWithShard).addValues(1L, "shard-from-row").build();

    // Tripping threshold with null/empty shard hint argument, should pull shard-from-row
    batcher.bufferRow(
        "Users", rowWithShard, MutationBatcher.MUTATION_INSERT, sampleTable, null, topoOrder);
    batcher.bufferRow(
        "Users", rowWithShard, MutationBatcher.MUTATION_INSERT, sampleTable, "", topoOrder);

    verify(mockWriter, times(1))
        .insert(any(), any(), org.mockito.Mockito.eq("shard-from-row"), anyInt());
  }

  @Test
  public void testBufferRow_mutationDelete_flushesInReverseTopoOrder() {
    List<String> extendedTopo = Arrays.asList("Companies", "Users");
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_DELETE, sampleTable, "shard0", extendedTopo);
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_DELETE, sampleTable, "shard0", extendedTopo);

    verify(mockWriter, times(1)).delete(any(), any(), anyString(), anyInt());
  }

  @Test
  public void testBufferRow_unknownOperation_logsErrorToDlq() {
    batcher.bufferRow("Users", sampleRow, "INVALID_OP", sampleTable, "shard0", topoOrder);
    batcher.bufferRow("Users", sampleRow, "INVALID_OP", sampleTable, "shard0", topoOrder);

    // Shoud go to DLQ since switch fallback throws IllegalStateException caught inside flush loop
    assertFalse(batcher.getFailedRecords().isEmpty());
  }

  @Test
  public void testFlushUpdates_processesUpdatesOnly() {
    batcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_UPDATE, sampleTable, "shard0", topoOrder);
    verify(mockWriter, times(0)).update(any(), any(), anyString(), anyInt());

    batcher.flushUpdates();
    verify(mockWriter, times(1)).update(any(), any(), anyString(), anyInt());
  }

  @Test
  public void testFlush_emptyBatch_returnsSafely() {
    // Forcing direct call on table and operation with empty buffer
    batcher.flushInsertsInTopoOrder(topoOrder);
    batcher.flushDeletesInReverseTopoOrder(topoOrder);
    verify(mockWriter, times(0)).insert(any(), any(), anyString(), anyInt());
  }

  @Test
  public void testFlush_customJdbcPoolSize_honorsPoolLimits() {
    MutationBatcher customBatcher = new MutationBatcher(1, 45, mockWriter);
    customBatcher.startBundle();
    customBatcher.bufferRow(
        "Users", sampleRow, MutationBatcher.MUTATION_INSERT, sampleTable, "shard0", topoOrder);

    verify(mockWriter, times(1)).insert(any(), any(), anyString(), org.mockito.Mockito.eq(45));
  }
}
