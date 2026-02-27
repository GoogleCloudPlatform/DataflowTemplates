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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeCombiner}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeCombinerTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGloballyCombine() {

    Range rangeBase =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("testTable").build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_base")
            .setColClass(Long.class)
            .setStart(0L)
            .setIsLast(true)
            .setEnd(32L)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    int splitDepth = 3;
    ImmutableList<Range> ranges = ImmutableList.of(rangeBase);
    for (int i = 0; i < splitDepth; i++) {
      ImmutableList.Builder<Range> curBuilder = ImmutableList.builder();
      for (Range range : ranges) {
        if (rangeBase.isSplittable(null)) {
          var splitRanges = range.split(null);
          curBuilder.add(splitRanges.getLeft());
          curBuilder.add(splitRanges.getRight());
        } else {
          curBuilder.add(range);
        }
      }
      ranges = curBuilder.build();
    }
    var mutableList = new ArrayList<>(ranges);
    Collections.shuffle(mutableList);
    ImmutableList<Range> shuffledRanges =
        mutableList.stream().collect(ImmutableList.toImmutableList());

    // Setup input PCollection
    PCollection<Range> input = pipeline.apply(Create.of(shuffledRanges));

    // Apply the RangeCombiner
    PCollection<ImmutableList<Range>> output = input.apply(RangeCombiner.globally());

    // Verify the output
    PAssert.that(output).containsInAnyOrder(ranges);

    // Run the pipeline
    pipeline.run();
  }

  @Test
  public void testPerKeyBatched() {
    // Arrange
    final int numBatches = 10;
    TableIdentifier table1 = TableIdentifier.builder().setTableName("table1").build();
    TableIdentifier table2 = TableIdentifier.builder().setTableName("table2").build();

    Range range1 =
        Range.builder()
            .setTableIdentifier(table1)
            .setColName("col1")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(20L)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .build();

    Range range1a = range1.split(null).getLeft();
    Range range1b = range1.split(null).getRight();

    Range range2 =
        Range.builder()
            .setTableIdentifier(table2)
            .setColName("col2")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(30L)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .build();

    Range range2a = range2.split(null).getLeft();
    Range range2b = range2.split(null).getRight();

    PCollection<Range> input = pipeline.apply(Create.of(range1a, range1b, range2a, range2b));

    // Act
    PCollection<KV<Integer, ImmutableList<Range>>> output =
        input.apply(RangeCombiner.perKeyBatched(numBatches));

    // Assert
    int key1 = Math.floorMod(table1.hashCode(), numBatches);
    int key2 = Math.floorMod(table2.hashCode(), numBatches);

    // Note: The order within the ImmutableList matters for the assertion.
    // The RangeCombiner sorts the ranges, so we should expect them in sorted order.
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(key1, ImmutableList.of(range1a, range1b)),
            KV.of(key2, ImmutableList.of(range2a, range2b)));

    pipeline.run();
  }
}
