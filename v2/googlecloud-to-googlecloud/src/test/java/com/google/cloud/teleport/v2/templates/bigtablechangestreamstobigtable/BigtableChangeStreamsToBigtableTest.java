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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToBigtableOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable.BigtableChangeStreamsToBigtable.ConvertChangeStreamToNativeMutationFn;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link ConvertChangeStreamToNativeMutationFn}. */
@RunWith(JUnit4.class)
public class BigtableChangeStreamsToBigtableTest {

  private static final String ROW_KEY = "row1";
  private static final String CF = "cf";
  private static final String QUAL = "col1";
  private static final String VAL = "val1";

  private static ChangeStreamMutation createMutation(
      ChangeStreamMutation.MutationType type, List<Entry> entries) {
    ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
    when(mutation.getType()).thenReturn(type);
    when(mutation.getEntries()).thenReturn(com.google.common.collect.ImmutableList.copyOf(entries));
    return mutation;
  }

  private static SetCell createSetCell(String cf, String qual, String val, long ts) {
    SetCell setCell = mock(SetCell.class);
    when(setCell.getFamilyName()).thenReturn(cf);
    when(setCell.getQualifier()).thenReturn(ByteString.copyFromUtf8(qual));
    when(setCell.getValue()).thenReturn(ByteString.copyFromUtf8(val));
    when(setCell.getTimestamp()).thenReturn(ts);
    return setCell;
  }

  private static DeleteCells createDeleteCells(String cf, String qual, long startTs, long endTs) {
    DeleteCells deleteCells = mock(DeleteCells.class);
    when(deleteCells.getFamilyName()).thenReturn(cf);
    when(deleteCells.getQualifier()).thenReturn(ByteString.copyFromUtf8(qual));

    TimestampRange range = TimestampRange.create(startTs, endTs);
    when(deleteCells.getTimestampRange()).thenReturn(range);
    return deleteCells;
  }

  private static DeleteFamily createDeleteFamily(String cf) {
    DeleteFamily deleteFamily = mock(DeleteFamily.class);
    when(deleteFamily.getFamilyName()).thenReturn(cf);
    return deleteFamily;
  }

  @Test
  public void testStandardMutationConversion() throws Exception {
    ConvertChangeStreamToNativeMutationFn fn =
        new ConvertChangeStreamToNativeMutationFn(false, "SOURCE_CBT", "SOURCE_CBT", false);

    SetCell setCell = createSetCell(CF, QUAL, VAL, 1000000);
    ChangeStreamMutation mutation =
        createMutation(ChangeStreamMutation.MutationType.USER, List.of(setCell));

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(KV.of(ByteString.copyFromUtf8(ROW_KEY), mutation), receiver);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<KV<ByteString, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver).output(captor.capture());

    KV<ByteString, Iterable<Mutation>> kv = captor.getValue();
    assertEquals(ROW_KEY, kv.getKey().toStringUtf8());

    Iterator<Mutation> iter = kv.getValue().iterator();
    assertTrue(iter.hasNext());
    Mutation mut = iter.next();
    assertTrue(mut.hasSetCell());

    Mutation.SetCell protoSetCell = mut.getSetCell();
    assertEquals(CF, protoSetCell.getFamilyName());
    assertEquals(QUAL, protoSetCell.getColumnQualifier().toStringUtf8());
    assertEquals(VAL, protoSetCell.getValue().toStringUtf8());
    assertEquals(1000000, protoSetCell.getTimestampMicros());
  }

  @Test
  public void testBidirectionalReplicationAppendsTag() throws Exception {
    ConvertChangeStreamToNativeMutationFn fn =
        new ConvertChangeStreamToNativeMutationFn(true, "SOURCE_CBT", "SOURCE_CBT", false);

    SetCell setCell = createSetCell(CF, QUAL, VAL, 1000000);
    ChangeStreamMutation mutation =
        createMutation(ChangeStreamMutation.MutationType.USER, List.of(setCell));

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(KV.of(ByteString.copyFromUtf8(ROW_KEY), mutation), receiver);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<KV<ByteString, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver).output(captor.capture());

    KV<ByteString, Iterable<Mutation>> kv = captor.getValue();

    List<Mutation> muts = new ArrayList<>();
    kv.getValue().forEach(muts::add);

    // Should contain the original SetCell plus the hidden DeleteFromColumn source tag
    assertEquals(2, muts.size());
    assertTrue(muts.get(0).hasSetCell());
    assertTrue(muts.get(1).hasDeleteFromColumn());

    Mutation.DeleteFromColumn del = muts.get(1).getDeleteFromColumn();
    assertEquals("SOURCE_CBT", del.getColumnQualifier().toStringUtf8());
  }

  @Test
  public void testBidirectionalReplicationFiltersIncomingTag() throws Exception {
    ConvertChangeStreamToNativeMutationFn fn =
        new ConvertChangeStreamToNativeMutationFn(true, "SOURCE_CBT", "SOURCE_CBT", false);

    SetCell setCell = createSetCell(CF, QUAL, VAL, 1000000);
    DeleteCells deleteCells = createDeleteCells(CF, "SOURCE_CBT", 0, 1000000);
    ChangeStreamMutation mutation =
        createMutation(ChangeStreamMutation.MutationType.USER, List.of(setCell, deleteCells));

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(KV.of(ByteString.copyFromUtf8(ROW_KEY), mutation), receiver);

    // Should be completely filtered out because the incoming mutation carries the replication tag
    verifyNoInteractions(receiver);
  }

  @Test
  public void testDeleteCellsConversion() throws Exception {
    ConvertChangeStreamToNativeMutationFn fn =
        new ConvertChangeStreamToNativeMutationFn(false, "SOURCE_CBT", "SOURCE_CBT", false);

    DeleteCells deleteCells = createDeleteCells(CF, QUAL, 1000, 2000);
    ChangeStreamMutation mutation =
        createMutation(ChangeStreamMutation.MutationType.USER, List.of(deleteCells));

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(KV.of(ByteString.copyFromUtf8(ROW_KEY), mutation), receiver);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<KV<ByteString, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver).output(captor.capture());

    KV<ByteString, Iterable<Mutation>> kv = captor.getValue();
    assertEquals(ROW_KEY, kv.getKey().toStringUtf8());

    Iterator<Mutation> iter = kv.getValue().iterator();
    assertTrue(iter.hasNext());
    Mutation mut = iter.next();
    assertTrue(mut.hasDeleteFromColumn());

    Mutation.DeleteFromColumn delCol = mut.getDeleteFromColumn();
    assertEquals(CF, delCol.getFamilyName());
    assertEquals(QUAL, delCol.getColumnQualifier().toStringUtf8());
    assertEquals(1000, delCol.getTimeRange().getStartTimestampMicros());
    assertEquals(2000, delCol.getTimeRange().getEndTimestampMicros());
  }

  @Test
  public void testDeleteFamilyConversion() throws Exception {
    ConvertChangeStreamToNativeMutationFn fn =
        new ConvertChangeStreamToNativeMutationFn(false, "SOURCE_CBT", "SOURCE_CBT", false);

    DeleteFamily deleteFamily = createDeleteFamily(CF);
    ChangeStreamMutation mutation =
        createMutation(ChangeStreamMutation.MutationType.USER, List.of(deleteFamily));

    @SuppressWarnings("unchecked")
    OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver = mock(OutputReceiver.class);

    fn.processElement(KV.of(ByteString.copyFromUtf8(ROW_KEY), mutation), receiver);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<KV<ByteString, Iterable<Mutation>>> captor = ArgumentCaptor.forClass(KV.class);

    verify(receiver).output(captor.capture());

    KV<ByteString, Iterable<Mutation>> kv = captor.getValue();
    assertEquals(ROW_KEY, kv.getKey().toStringUtf8());

    Iterator<Mutation> iter = kv.getValue().iterator();
    assertTrue(iter.hasNext());
    Mutation mut = iter.next();
    assertTrue(mut.hasDeleteFromFamily());

    Mutation.DeleteFromFamily delFam = mut.getDeleteFromFamily();
    assertEquals(CF, delFam.getFamilyName());
  }

  @Test
  public void testCreateWrite() {
    BigtableChangeStreamsToBigtableOptions options =
        mock(BigtableChangeStreamsToBigtableOptions.class);
    when(options.getBigtableWriteInstanceId()).thenReturn("inst1");
    when(options.getBigtableWriteTableId()).thenReturn("table1");
    when(options.getBigtableWriteProjectId()).thenReturn("proj1");
    when(options.getBigtableWriteAppProfile()).thenReturn("appProfile1");
    when(options.getBigtableBulkWriteMaxRowKeyCount()).thenReturn(100);
    when(options.getBigtableBulkWriteMaxRequestSizeBytes()).thenReturn(1024);
    when(options.getBigtableBulkWriteFlowControl()).thenReturn(true);

    BigtableIO.Write write = BigtableChangeStreamsToBigtable.createWrite(options);
    assertNotNull(write);
  }
}
