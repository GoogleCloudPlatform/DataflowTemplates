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
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link MergeRangesDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class MergeRangesDoFnTest {

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<ImmutableList<Range>> rangesCaptor;
  @Mock DoFn.ProcessContext mockProcessContext;

  @Test
  public void testMergeRangesDoFnWithAutoAdjustment() {
    ImmutableList<Range> testRanges = getDummyRanges(mockProcessContext);
    MergeRangesDoFn mergeRangesDoFn =
        MergeRangesDoFn.builder()
            .setMaxPartitionHint(2L)
            .setApproxTotalRowCount(3200L)
            .setAutoAdjustMaxPartitions(true)
            .setTableName("testTable")
            .build();
    mergeRangesDoFn.processElement(testRanges, mockOut, mockProcessContext);

    verify(mockOut).output(rangesCaptor.capture());
    ImmutableList<Range> dummy = rangesCaptor.getValue();
    assertThat(rangesCaptor.getValue())
        .isEqualTo(
            ImmutableList.of(
                testRanges.get(0),
                // Can not merge after 0 into 1 as count exceeds mean.
                testRanges.get(1),
                // Can not merge after 1 into 2 as count exceeds mean.
                testRanges.get(2).mergeRange(testRanges.get(3), mockProcessContext),
                // Can not merge after 3 into 4 as the ranges are not mergable.
                testRanges.get(4).mergeRange(testRanges.get(5), mockProcessContext),
                // can not merge after 5 into 6 as ranges are not mergable.
                testRanges
                    .get(6)
                    .mergeRange(testRanges.get(7), mockProcessContext)
                    .mergeRange(testRanges.get(8), mockProcessContext)));
  }

  @Test
  public void testMergeRangesDoFnNoAutoAdjustment() {
    ImmutableList<Range> testRanges = getDummyRanges(mockProcessContext);
    MergeRangesDoFn mergeRangesDoFn =
        MergeRangesDoFn.builder()
            .setMaxPartitionHint(2L)
            .setApproxTotalRowCount(3200L)
            .setAutoAdjustMaxPartitions(false)
            .setTableName("testTable")
            .build();
    mergeRangesDoFn.processElement(testRanges, mockOut, mockProcessContext);

    verify(mockOut).output(rangesCaptor.capture());
    assertThat(rangesCaptor.getValue())
        .isEqualTo(
            ImmutableList.of(
                testRanges.get(0),
                // Can not merge after 0 into 1 as count exceeds mean.
                testRanges
                    .get(1)
                    .mergeRange(testRanges.get(2), mockProcessContext)
                    .mergeRange(testRanges.get(3), mockProcessContext),
                // Can not merge after 3 into 4 as the ranges are not mergable.
                testRanges.get(4).mergeRange(testRanges.get(5), mockProcessContext),
                // can not merge after 5 into 6 as ranges are not mergable.
                testRanges
                    .get(6)
                    .mergeRange(testRanges.get(7), mockProcessContext)
                    .mergeRange(testRanges.get(8), mockProcessContext)));
  }

  private static ImmutableList<Range> getDummyRanges(ProcessContext mockProcessContext) {

    Range testRange =
        dummyCounter(
            Range.builder()
                .setStart(0L)
                .setEnd(64L)
                .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
                .setColName("col1")
                .setColClass(Long.class)
                .build(),
            mockProcessContext);
    ImmutableList.Builder<Range> testRangesBuilder = ImmutableList.builder();
    for (int i = 0; i < 6; i++) {
      Pair<Range, Range> splitRanges = testRange.split(mockProcessContext);
      if (i == 2) {
        testRange = dummyCounter(splitRanges.getLeft(), mockProcessContext);
        for (int j = 0; j < 3; j++) {
          Pair<Range, Range> splitRangesFine = testRange.split(mockProcessContext);
          testRangesBuilder.add(dummyCounter(splitRangesFine.getLeft(), mockProcessContext));
          testRange = dummyCounter(splitRangesFine.getRight(), mockProcessContext);
        }

      } else {
        testRangesBuilder.add(dummyCounter(splitRanges.getLeft(), mockProcessContext));
      }
      testRange = dummyCounter(splitRanges.getRight(), mockProcessContext);
    }
    /*Ranges we get here are 0-32, 32-48, 48-52, 52-54, 54-55, 56-60, 60-62, 62-63*/
    ImmutableList<Range> testRanges = testRangesBuilder.build();
    Pair<Range, Range> rangeWithChildSplit =
        testRanges
            .get(4)
            .withChildRange(
                dummyCounter(
                    testRange.toBuilder().setColName("col2").setStart(0L).setEnd(15L).build(),
                    mockProcessContext),
                mockProcessContext)
            .split(null);

    testRangesBuilder = new ImmutableList.Builder<>();
    for (int i = 0; i < testRanges.size(); i++) {
      if (i == 4) {
        testRangesBuilder
            .add(rangeWithChildSplit.getLeft().withCount(1500L, mockProcessContext))
            .add(rangeWithChildSplit.getRight().withCount(1500L, mockProcessContext));
      } else {
        testRangesBuilder.add(testRanges.get(i));
      }
    }
    /*
     * Ranges we get here are 0-32, 32-48, 48-52, 52-54, 54-55(withChild 0-8), 54-55(withChild 9 - 16), 56-60, 60-62, 62-63
     * For testing purpose, All ranges have counts equal to 300 * (end - start) (for testing) except for the ones with a child with count 1500.
     */
    testRanges = testRangesBuilder.build();
    return testRanges;
  }

  private static Range dummyCounter(Range range, ProcessContext mockProcessContext) {
    return range.withCount(300L * ((Long) range.end() - (Long) range.start()), mockProcessContext);
  }
}
