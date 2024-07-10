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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class MergeRangesDoFn extends DoFn<ImmutableList<Range>, ImmutableList<Range>> {

  private static final Logger logger = LoggerFactory.getLogger(MergeRangesDoFn.class);

  /** Approximate row count of the table. */
  abstract Long approxTotalRowCount();

  /** Max partitions hint as set or auto-inferred by {@link ReadWithUniformPartitions}. */
  abstract Long maxPartitionHint();

  /**
   * If true, MaxPartitions will be inferred again after total counts of all ranges is not
   * in-determinate.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /** Name of the table. */
  abstract String tableName();

  /**
   * Merge the ranges to get closer to the mean if possible. This DoFn applied at the end of the
   * split process tries to give number of ranges closer to maxPartitions, to avoid unintented
   * partitions. Note: In case of composite keys, it is possible to end up with more ranges than the
   * maxPartitions, for example a range with first column along can not merge with a consecutive
   * range that spans first and second column both.
   *
   * @param input list of ranges to merge.
   * @param out output receiver for the merged ranges.
   * @param c process context.
   */
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

    long totalCount = approxTotalRowCount();

    long mean = 0;

    long accumulatedCount = 0;

    logger.info(
        "RWUPT - Began merging split-ranges for table {} initial split range count as {}",
        tableName(),
        input.size());

    // TODO(vardhanvthigle): moving the total count clcuation to combiner will remove code
    // duplication with {@link RangeClassifierDoFn}.
    // Refine the Count.
    for (Range range : input) {
      accumulatedCount = range.accumulateCount(accumulatedCount);
    }
    if (accumulatedCount != Range.INDETERMINATE_COUNT) {
      totalCount = accumulatedCount;
    }

    long maxPartitions = maxPartitionHint();
    if (autoAdjustMaxPartitions()) {
      maxPartitions = ReadWithUniformPartitions.inferMaxPartitions(totalCount);
    }
    mean = Math.max(1, totalCount / maxPartitions);

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

    ImmutableList<Range> output = mergedRanges.build();

    // Note Searching for "RWUPT -" for ReadWithUniformPartition gives the most import logs for the
    // splitting process.
    logger.info(
        "RWUPT - Completed split process (merging) split-ranges for table {} initial split range count {}, final range count as {}",
        tableName(),
        input.size(),
        output.size());

    return output;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setApproxTotalRowCount(Long value);

    public abstract Builder setMaxPartitionHint(Long value);

    public abstract Builder setAutoAdjustMaxPartitions(Boolean value);

    public abstract Builder setTableName(String value);

    public abstract MergeRangesDoFn build();
  }
}
