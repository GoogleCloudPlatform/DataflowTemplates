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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** Combine PCollection&lt;Range&gt; into PCollection&lt;Range&gt;. */
public class RangeCombiner extends Combine.CombineFn<Range, List<Range>, ImmutableList<Range>> {

  @Override
  public List<Range> createAccumulator() {
    return Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public List<Range> addInput(List<Range> accumulator, Range input) {
    accumulator.add(input);
    return accumulator;
  }

  @Override
  public List<Range> mergeAccumulators(Iterable<List<Range>> accumulators) {
    List<Range> merged = Collections.synchronizedList(new ArrayList<>());
    for (List<Range> accumulator : accumulators) {
      merged.addAll(accumulator);
    }
    return merged;
  }

  @Override
  public ImmutableList<Range> extractOutput(List<Range> accumulator) {
    Collections.sort(accumulator); // Sort the list before making it immutable
    return ImmutableList.copyOf(accumulator);
  }

  /**
   * Returns a {@link PTransform} that combines the Range elements in its input {@link PCollection}.
   */
  public static PTransform<PCollection<Range>, PCollection<ImmutableList<Range>>> globally() {
    return Combine.globally(new RangeCombiner());
  }
}
