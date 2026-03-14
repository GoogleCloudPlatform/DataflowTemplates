/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeClassifierDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeClassifierDoFnTest {

  @Mock ProcessContext mockProcessContext;

  @Test
  public void testRangeClassifierNoAutoAdjust() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());
    Range rangeToRetainDueToCount =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setCount(10L)
            .setSplitIndex("1-0-0")
            .build();
    Range rangeToSplit =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(1000)
            .setEnd(2000)
            .setSplitIndex("1-0-1")
            .setCount(1000L)
            .build();
    Range rangeToAddColumn =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setSplitIndex("1-1-0")
            .setCount(1000L)
            .build();

    Range rangeToRetainUnSplitable =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2001)
            .setEnd(2002)
            .setSplitIndex("d")
            .build()
            .withChildRange(
                Range.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setColName("col2")
                    .setColClass(Integer.class)
                    .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                    .setStart(1000)
                    .setEnd(1001)
                    .setCount(1000L)
                    .build(),
                mockProcessContext);
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(2L)
                    .setSplitStagesCount(2L)
                    .setApproxRowCount(10L)
                    .setMaxPartitionsHint(10L)
                    .setSplitStagesCount(2L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build(),
                            PartitionColumn.builder()
                                .setColumnName("col2")
                                .setColumnClass(Integer.class)
                                .build()))
                    .build())
            .setStageIdx(1L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    Pair<Range, Range> splitRangeToCount = rangeToSplit.split(mockProcessContext);

    rangeClassifierDoFn.processElement(
        KV.of(
            0,
            ImmutableList.of(
                rangeToRetainDueToCount, rangeToSplit, rangeToAddColumn, rangeToRetainUnSplitable)),
        mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .isEqualTo(ImmutableList.of(rangeToRetainDueToCount, rangeToRetainUnSplitable));
    assertThat(taggedOutputCaptor.toCountAccumulator.build())
        .isEqualTo(ImmutableList.of(splitRangeToCount.getLeft(), splitRangeToCount.getRight()));
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build())
        .isEqualTo(
            ImmutableList.of(
                ColumnForBoundaryQuery.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setColumnName("col2")
                    .setColumnClass(Integer.class)
                    .setParentRange(rangeToAddColumn)
                    .build()));
  }

  @Test
  public void testRangeClassifierAutoAdjust() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());
    Range rangeToRetainDueToCount =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setCount(10L)
            .setSplitIndex("1-0-0")
            .build();
    // Here, the code auto-adjusts to 5 partitions
    Range rangeToRetainAndNotSplit =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(1000)
            .setEnd(2000)
            .setSplitIndex("1-0-1")
            .setCount(1000L)
            .build();
    Range rangeToRetainNotAddColumn =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setSplitIndex("1-1-0")
            .setCount(1000L)
            .build();

    Range rangeToRetainUnSplitable =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2001)
            .setEnd(2002)
            .setSplitIndex("1-1-1")
            .build()
            .withChildRange(
                Range.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setColName("col2")
                    .setColClass(Integer.class)
                    .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                    .setStart(1000)
                    .setEnd(1001)
                    .setCount(1000L)
                    .build(),
                mockProcessContext);
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(1L)
                    .setSplitStagesCount(2L)
                    .setApproxRowCount(1L)
                    .setMaxPartitionsHint(1L)
                    .setSplitStagesCount(2L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build(),
                            PartitionColumn.builder()
                                .setColumnName("col2")
                                .setColumnClass(Integer.class)
                                .build()))
                    .build())
            .setStageIdx(1L)
            .setAutoAdjustMaxPartitions(true)
            .build();

    rangeClassifierDoFn.processElement(
        KV.of(
            0,
            ImmutableList.of(
                rangeToRetainDueToCount, rangeToRetainAndNotSplit, rangeToRetainNotAddColumn)),
        mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .isEqualTo(
            ImmutableList.of(
                rangeToRetainDueToCount, rangeToRetainAndNotSplit, rangeToRetainNotAddColumn));
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEqualTo(ImmutableList.of());
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEqualTo(ImmutableList.of());
  }

  @Test
  public void testRangeClassifierStage0() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());
    Range rangeToCount =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setSplitIndex("1-0-0")
            .build();
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(1L)
                    .setSplitStagesCount(1L)
                    .setApproxRowCount(10L)
                    .setMaxPartitionsHint(10L)
                    .setSplitStagesCount(2L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build(),
                            PartitionColumn.builder()
                                .setColumnName("col2")
                                .setColumnClass(Integer.class)
                                .build()))
                    .build())
            .setStageIdx(0L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    ImmutableList<Range> initialSplit =
        ImmutableList.of(
            rangeToCount,
            rangeToCount.toBuilder().setStart(1000).setEnd(2000).setSplitIndex("1-0-1").build(),
            rangeToCount.toBuilder().setStart(2000).setEnd(3000).setSplitIndex("1-1-0").build());

    rangeClassifierDoFn.processElement(KV.of(0, initialSplit), mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEqualTo(initialSplit);
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }

  @Test
  public void testRangeClassifierWithMultipleTableSpecs() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());
    Range rangeToCount =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setSplitIndex("1-0-0")
            .build();
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecifications(
                ImmutableList.of(
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder().setTableName("testTable").build())
                        .setInitialSplitHeight(1L)
                        .setApproxRowCount(10L)
                        .setMaxPartitionsHint(10L)
                        .setSplitStagesCount(1L)
                        .setPartitionColumns(
                            ImmutableList.of(
                                PartitionColumn.builder()
                                    .setColumnName("col1")
                                    .setColumnClass(Integer.class)
                                    .build()))
                        .build(),
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder().setTableName("testTable2").build())
                        .setInitialSplitHeight(1L)
                        .setSplitStagesCount(1L)
                        .setApproxRowCount(10L)
                        .setMaxPartitionsHint(10L)
                        .setPartitionColumns(
                            ImmutableList.of(
                                PartitionColumn.builder()
                                    .setColumnName("col1")
                                    .setColumnClass(Integer.class)
                                    .build()))
                        .build()))
            .setStageIdx(0L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    ImmutableList<Range> initialSplit =
        ImmutableList.of(
            rangeToCount,
            rangeToCount.toBuilder().setStart(1000).setEnd(2000).setSplitIndex("1-0-1").build(),
            rangeToCount.toBuilder().setStart(2000).setEnd(3000).setSplitIndex("1-1-0").build());

    rangeClassifierDoFn.processElement(KV.of(0, initialSplit), mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEqualTo(initialSplit);
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }

  @Test
  public void testRangeClassifierWithUnknownTableIdentifier() {
    Range rangeToCount =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("unknownTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .build();
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(1L)
                    .setSplitStagesCount(1L)
                    .setApproxRowCount(10L)
                    .setMaxPartitionsHint(10L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build()))
                    .build())
            .setStageIdx(0L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    ImmutableList<Range> initialSplit = ImmutableList.of(rangeToCount);
    assertThrows(
        RuntimeException.class,
        () -> rangeClassifierDoFn.processElement(KV.of(0, initialSplit), mockProcessContext));
  }

  @Test
  public void testRangeClassifierDoFn_withEmptyInput_doesNothing() {
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(2L)
                    .setApproxRowCount(10L)
                    .setMaxPartitionsHint(10L)
                    .setSplitStagesCount(2L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build()))
                    .build())
            .setStageIdx(1L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    rangeClassifierDoFn.processElement(KV.of(0, ImmutableList.of()), mockProcessContext);

    Mockito.verify(mockProcessContext, Mockito.never()).output(any(), any());
  }

  @Test
  public void testRangeClassifierNoAutoAdjustNoMoreColumn() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());
    Range rangeToAddColumnButNoMoreColumns =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setCount(1000L)
            .build();

    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(
                TableSplitSpecification.builder()
                    .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
                    .setInitialSplitHeight(1L)
                    .setInitialSplitHeight(2L)
                    .setApproxRowCount(10L)
                    .setMaxPartitionsHint(10L)
                    .setSplitStagesCount(2L)
                    .setPartitionColumns(
                        ImmutableList.of(
                            PartitionColumn.builder()
                                .setColumnName("col1")
                                .setColumnClass(Integer.class)
                                .build())) // Only one column
                    .build())
            .setStageIdx(1L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    rangeClassifierDoFn.processElement(
        KV.of(0, ImmutableList.of(rangeToAddColumnButNoMoreColumns)), mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .isEqualTo(ImmutableList.of(rangeToAddColumnButNoMoreColumns));
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }

  // Traditional Argument Captor gets tricky with multiple tags of multiple types.
  private class TaggedOutputCaptor {
    public ImmutableList.Builder toCountAccumulator = ImmutableList.<Range>builder();

    public ImmutableList.Builder toAddColumnAccumulator =
        ImmutableList.<ColumnForBoundaryQuery>builder();
    public ImmutableList.Builder toRetainAccumulator = ImmutableList.<Range>builder();

    public ImmutableList.Builder toReadyToReadAccumulator = ImmutableList.<Range>builder();

    public Void out(TupleTag tag, Object element) {
      if (tag.equals(RangeClassifierDoFn.TO_COUNT_TAG)) {
        toCountAccumulator.add(element);
      } else if (tag.equals(RangeClassifierDoFn.TO_ADD_COLUMN_TAG)) {
        toAddColumnAccumulator.add(element);
      } else if (tag.equals(RangeClassifierDoFn.TO_RETAIN_TAG)) {
        toRetainAccumulator.add(element);
      } else if (tag.equals(RangeClassifierDoFn.READY_TO_READ_TAG)) {
        toReadyToReadAccumulator.add(element);
      } else {
        assert (false);
      }
      return null;
    }
  }

  @Test
  public void testGraduationBySplitHeight() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());

    TableIdentifier table = TableIdentifier.builder().setTableName("testTable").build();
    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(table)
            .setInitialSplitHeight(2L)
            .setSplitStagesCount(2L) // Graduate at stage 2
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .build();

    Range range1 =
        Range.builder()
            .setTableIdentifier(table)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .setSplitIndex("1-0")
            .setCount(50L)
            .build();
    Range range2 =
        range1.toBuilder().setStart(101).setEnd(200).setCount(50L).setSplitIndex("1-1").build();
    ImmutableList<Range> ranges = ImmutableList.of(range1, range2);

    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(spec)
            .setStageIdx(2L) // Current stage is 2, which meets the splitHeight.
            .setAutoAdjustMaxPartitions(false)
            .build();

    rangeClassifierDoFn.processElement(KV.of(0, ranges), mockProcessContext);

    // Assert all ranges are graduated.
    assertThat(taggedOutputCaptor.toReadyToReadAccumulator.build())
        .containsExactlyElementsIn(ranges);
    // Assert no ranges are sent to other tags.
    assertThat(taggedOutputCaptor.toRetainAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }

  @Test
  public void testNoGraduationBeforeSplitHeight() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());

    TableIdentifier table = TableIdentifier.builder().setTableName("testTable").build();
    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(table)
            .setInitialSplitHeight(3L)
            .setSplitStagesCount(3L) // Graduate at stage 3
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .build();

    Range rangeToSplit =
        Range.builder()
            .setTableIdentifier(table)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .setCount(50L) // 50 > (1 + 0.25) * (100/10) = 12.5, so should be split.
            .build();
    ImmutableList<Range> ranges = ImmutableList.of(rangeToSplit);

    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecification(spec)
            .setStageIdx(2L) // Current stage is 2, which is less than the splitHeight.
            .setAutoAdjustMaxPartitions(false)
            .build();

    rangeClassifierDoFn.processElement(KV.of(0, ranges), mockProcessContext);

    // Assert no ranges are graduated.
    assertThat(taggedOutputCaptor.toReadyToReadAccumulator.build()).isEmpty();
    // Assert range was sent to be split.
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isNotEmpty();
    assertThat(taggedOutputCaptor.toRetainAccumulator.build()).isEmpty();
  }

  @Test
  public void testProcessElementWithMultipleTables() {
    TaggedOutputCaptor taggedOutputCaptor = new TaggedOutputCaptor();
    Mockito.doAnswer(
            invocationOnMock ->
                taggedOutputCaptor.out(
                    invocationOnMock.getArgument(0), invocationOnMock.getArgument(1)))
        .when(mockProcessContext)
        .output(any(), any());

    TableIdentifier table1 = TableIdentifier.builder().setTableName("table1").build();
    TableIdentifier table2 = TableIdentifier.builder().setTableName("table2").build();
    TableIdentifier table3 = TableIdentifier.builder().setTableName("table3").build();

    TableSplitSpecification spec1 =
        TableSplitSpecification.builder()
            .setTableIdentifier(table1)
            .setInitialSplitHeight(1L)
            .setSplitStagesCount(8L)
            .setApproxRowCount(100L)
            .setMaxPartitionsHint(10L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build()))
            .build();

    TableSplitSpecification spec2 =
        TableSplitSpecification.builder()
            .setTableIdentifier(table2)
            .setInitialSplitHeight(1L)
            .setSplitStagesCount(8L)
            .setApproxRowCount(10L)
            .setMaxPartitionsHint(2L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("colA")
                        .setColumnClass(Long.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("colB")
                        .setColumnClass(Long.class)
                        .build()))
            .build();

    TableSplitSpecification spec3 =
        TableSplitSpecification.builder()
            .setTableIdentifier(table3)
            .setInitialSplitHeight(2L)
            .setSplitStagesCount(8L)
            .setApproxRowCount(50L)
            .setMaxPartitionsHint(5L)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("colX")
                        .setColumnClass(Integer.class)
                        .build()))
            .build();

    Range range1Table1 =
        Range.builder()
            .setTableIdentifier(table1)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(100)
            .setSplitIndex("1-0")
            .setCount(50L) // Should be retained (mean for table1 is 100/10 = 10, 50 > 2*10)
            .build();
    Range range2Table1 =
        Range.builder()
            .setTableIdentifier(table1)
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(101)
            .setEnd(200)
            .setSplitIndex("1-1")
            .setCount(5L) // Should be retained (5 < 2*10)
            .build();

    Range range1Table2 =
        Range.builder()
            .setTableIdentifier(table2)
            .setColName("colA")
            .setColClass(Long.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setStart(0L)
            .setEnd(100L)
            .setSplitIndex("1-0")
            .setCount(8L) // Should be retained (mean for table2 is 10/2 = 5, 8 > 2*5)
            .build();
    Range range2Table2 =
        Range.builder()
            .setTableIdentifier(table2)
            .setColName("colA")
            .setColClass(Long.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setStart(101L)
            .setEnd(200L)
            .setSplitIndex("1-1")
            .setCount(1L) // Should be graduated (1 < 1.25*5)
            .build();

    Range range1Table3 =
        Range.builder()
            .setTableIdentifier(table3)
            .setColName("colX")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(50)
            .setSplitIndex("1-0")
            .setCount(30L) // Should be split (mean for table3 is 50/5 = 10, 30 > 1.25*10)
            .build();
    Range range2Table3 =
        Range.builder()
            .setTableIdentifier(table3)
            .setColName("colX")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(51)
            .setEnd(100)
            .setSplitIndex("1-1")
            .setCount(2L) // Should be graduated (2 < 1.25*10)
            .build();

    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setTableSplitSpecifications(ImmutableList.of(spec1, spec2, spec3))
            .setStageIdx(1L)
            .setAutoAdjustMaxPartitions(false)
            .build();

    rangeClassifierDoFn.processElement(
        KV.of(
            0,
            ImmutableList.of(
                range1Table1,
                range2Table1,
                range1Table2,
                range2Table2,
                range1Table3,
                range2Table3)),
        mockProcessContext);

    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .containsExactly(range2Table1, range1Table2, range2Table2, range2Table3);
    assertThat(taggedOutputCaptor.toCountAccumulator.build())
        .containsExactly(
            range1Table1.split(mockProcessContext).getLeft(),
            range1Table1.split(mockProcessContext).getRight(),
            range1Table3.split(mockProcessContext).getLeft(),
            range1Table3.split(mockProcessContext).getRight());
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }
}
