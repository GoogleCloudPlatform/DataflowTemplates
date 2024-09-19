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
import static org.mockito.ArgumentMatchers.any;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
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
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setCount(10L)
            .build();
    Range rangeToSplit =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(1000)
            .setEnd(2000)
            .setCount(1000L)
            .build();
    Range rangeToAddColumn =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setCount(1000L)
            .build();

    Range rangeToRetainUnSplitable =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2001)
            .setEnd(2002)
            .build()
            .withChildRange(
                Range.builder()
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
            .setApproxTotalRowCount(10L)
            .setStageIdx(1L)
            .setMaxPartitionHint(10L)
            .setAutoAdjustMaxPartitions(false)
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

    Pair<Range, Range> splitRangeToCount = rangeToSplit.split(mockProcessContext);

    rangeClassifierDoFn.processElement(
        ImmutableList.of(
            rangeToRetainDueToCount, rangeToSplit, rangeToAddColumn, rangeToRetainUnSplitable),
        mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .isEqualTo(ImmutableList.of(rangeToRetainDueToCount, rangeToRetainUnSplitable));
    assertThat(taggedOutputCaptor.toCountAccumulator.build())
        .isEqualTo(ImmutableList.of(splitRangeToCount.getLeft(), splitRangeToCount.getRight()));
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build())
        .isEqualTo(
            ImmutableList.of(
                ColumnForBoundaryQuery.builder()
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
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .setCount(10L)
            .build();
    // Here, the code auto-adjusts to 5 partitions
    Range rangeToRetainAndNotSplit =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(1000)
            .setEnd(2000)
            .setCount(1000L)
            .build();
    Range rangeToRetainNotAddColumn =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2000)
            .setEnd(2000)
            .setCount(1000L)
            .build();

    Range rangeToRetainUnSplitable =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(2001)
            .setEnd(2002)
            .build()
            .withChildRange(
                Range.builder()
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
            .setApproxTotalRowCount(1L)
            .setStageIdx(1L)
            .setMaxPartitionHint(1L)
            .setAutoAdjustMaxPartitions(true)
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

    Pair<Range, Range> splitRangeToCount = rangeToRetainAndNotSplit.split(mockProcessContext);

    rangeClassifierDoFn.processElement(
        ImmutableList.of(
            rangeToRetainDueToCount,
            rangeToRetainAndNotSplit,
            rangeToRetainNotAddColumn,
            rangeToRetainUnSplitable),
        mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build())
        .isEqualTo(
            ImmutableList.of(
                rangeToRetainDueToCount,
                rangeToRetainAndNotSplit,
                rangeToRetainNotAddColumn,
                rangeToRetainUnSplitable));
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
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(0)
            .setEnd(1000)
            .build();
    RangeClassifierDoFn rangeClassifierDoFn =
        RangeClassifierDoFn.builder()
            .setApproxTotalRowCount(10L)
            .setStageIdx(0L)
            .setAutoAdjustMaxPartitions(false)
            .setMaxPartitionHint(10L)
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

    ImmutableList<Range> initialSplit =
        ImmutableList.of(
            rangeToCount,
            rangeToCount.toBuilder().setStart(1000).setEnd(2000).build(),
            rangeToCount.toBuilder().setStart(2000).setEnd(3000).build());

    rangeClassifierDoFn.processElement(initialSplit, mockProcessContext);
    assertThat(taggedOutputCaptor.toRetainAccumulator.build()).isEmpty();
    assertThat(taggedOutputCaptor.toCountAccumulator.build()).isEqualTo(initialSplit);
    assertThat(taggedOutputCaptor.toAddColumnAccumulator.build()).isEmpty();
  }

  // Traditional Argument Captor gets tricky with multiple tags of multiple types.
  private class TaggedOutputCaptor {
    public ImmutableList.Builder toCountAccumulator = ImmutableList.<Range>builder();

    public ImmutableList.Builder toAddColumnAccumulator =
        ImmutableList.<ColumnForBoundaryQuery>builder();
    public ImmutableList.Builder toRetainAccumulator = ImmutableList.<Range>builder();

    public Void out(TupleTag tag, Object element) {
      if (tag.equals(RangeClassifierDoFn.TO_COUNT_TAG)) {
        toCountAccumulator.add(element);
      } else if (tag.equals(RangeClassifierDoFn.TO_ADD_COLUMN_TAG)) {
        toAddColumnAccumulator.add(element);
      } else if (tag.equals(RangeClassifierDoFn.TO_RETAIN_TAG)) {
        toRetainAccumulator.add(element);
      } else {
        assert (false);
      }
      return null;
    }
  }
}
