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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.tuple.Pair;

@AutoValue
public abstract class InitialSplitRangeDoFn extends DoFn<Range, ImmutableList<Range>> {
  abstract long splitHeight();

  @ProcessElement
  public void processElement(
      @Element Range input, OutputReceiver<ImmutableList<Range>> out, ProcessContext c) {

    ArrayList<Range> ranges = new ArrayList<Range>();
    ranges.add(input);
    ArrayList<Range> splitRanges = new ArrayList<>();
    for (long i = 0; i < splitHeight(); i++) {
      for (Range range : ranges) {
        if (range.isSplittable(c)) {
          Pair<Range, Range> splitPair = range.split(c);
          splitRanges.add(splitPair.getLeft());
          splitRanges.add(splitPair.getRight());
        } else {
          splitRanges.add(range);
        }
      }
      ranges = new ArrayList<>(splitRanges);
      splitRanges.clear();
    }
    Collections.sort(ranges);
    out.output(ImmutableList.copyOf(ranges));
  }

  public static Builder builder() {
    return new AutoValue_InitialSplitRangeDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSplitHeight(long value);

    public abstract InitialSplitRangeDoFn build();
  }
}
