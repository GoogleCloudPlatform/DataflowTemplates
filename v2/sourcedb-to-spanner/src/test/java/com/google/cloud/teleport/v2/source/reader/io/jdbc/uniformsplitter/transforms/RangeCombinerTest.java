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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
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
}
