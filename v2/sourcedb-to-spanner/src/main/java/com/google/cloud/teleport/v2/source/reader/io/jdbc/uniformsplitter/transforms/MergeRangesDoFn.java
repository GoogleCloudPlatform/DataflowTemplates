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
import org.apache.beam.sdk.transforms.DoFn;

@AutoValue
public abstract class MergeRangesDoFn extends DoFn<ImmutableList<Range>, ImmutableList<Range>> {

  abstract Long approxTableCount();

  abstract Long maxPartitionHint();

  @ProcessElement
  public void processElement(
      @Element ImmutableList<Range> input,
      OutputReceiver<ImmutableList<Range>> out,
      ProcessContext c) {
    out.output(mergeRanges(input, c));
  }

  public static Builder builder() {
    return new AutoValue_MergeRangesDoFn.Builder();
  }

  private ImmutableList<Range> mergeRanges(ImmutableList<Range> input, ProcessContext c) {

    long tableCount = approxTableCount();

    long mean = 0;

    long accumulatedCount = 0;

    // Refine the Count.
    for (Range range : input) {
      accumulatedCount = range.accumulateCount(accumulatedCount);
    }
    if (accumulatedCount != Range.INDETERMINATE_COUNT) {
      tableCount = accumulatedCount;
    }

    mean = Math.max(1, tableCount / maxPartitionHint());

    ImmutableList.Builder<Range> mergedRanges = ImmutableList.builder();
    Range lastMergedRange = null; // Store the last merged range
    for (Range currentRange : input) {
      if (lastMergedRange == null) {
        lastMergedRange = currentRange; // First range, no merging yet
      } else if (lastMergedRange.isMergable(currentRange)
          && lastMergedRange.accumulateCount(currentRange.count()) <= mean) {
        // Merge ranges and update lastMergedRange
        lastMergedRange = lastMergedRange.mergeRange(currentRange, c);
      } else {
        // Ranges aren't mergeable, add the lastMergedRange to the result
        mergedRanges.add(lastMergedRange);
        lastMergedRange = currentRange; // Update lastMergedRange
      }
    }
    if (lastMergedRange != null) {
      mergedRanges.add(lastMergedRange); // Add the last merged range
    }

    return mergedRanges.build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setApproxTableCount(Long value);

    public abstract Builder setMaxPartitionHint(Long value);

    public abstract MergeRangesDoFn build();
  }
}
