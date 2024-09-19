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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Classify ranges into ranges to count, split or retain. */
@AutoValue
public abstract class RangeClassifierDoFn extends DoFn<ImmutableList<Range>, Range>
    implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(RangeCountDoFn.class);

  // Note it is necessary to retain `new TupleTag<Range>() {}`  though editor might point to
  // dropping `Range` or the `{}`.
  // It is necessary to retain the type information for the coder inference of beam to work.
  // Dropping this will make `ReadWithUniformPartitionsTest` fail with coder not found error.
  public static final TupleTag<Range> TO_COUNT_TAG = new TupleTag<Range>() {};

  // Note it is necessary to retain `new TupleTag<Range>() {}`  though editor might point to
  // dropping `Range` or the `{}`.
  // It is necessary to retain the type information for the coder inference of beam to work.
  // Dropping this will make `ReadWithUniformPartitionsTest` fail with coder not found error.
  public static final TupleTag<Range> TO_RETAIN_TAG = new TupleTag<Range>() {};

  // Note it is necessary to retain `new TupleTag<ColumnForBoundaryQuery>() {}`  though editor might
  // point to dropping `ColumnForBoundaryQuery` or the `{}`.
  // It is necessary to retain the type information for the coder inference of beam to work.
  // Dropping this will make `ReadWithUniformPartitionsTest` fail with coder not found error.
  public static final TupleTag<ColumnForBoundaryQuery> TO_ADD_COLUMN_TAG =
      new TupleTag<ColumnForBoundaryQuery>() {};

  /** Partition Columns. */
  abstract ImmutableList<PartitionColumn> partitionColumns();

  /** Approximate row count of the table. */
  abstract Long approxTotalRowCount();

  /** Max partitions hint as set or auto-inferred by {@link ReadWithUniformPartitions}. */
  abstract Long maxPartitionHint();

  /**
   * If true, MaxPartitions will be inferred again after total counts of all ranges is not
   * in-determinate.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /** Stage Index. */
  abstract Long stageIdx();

  @ProcessElement
  public void processElement(@Element ImmutableList<Range> input, ProcessContext c) {

    logger.debug("Classifying ranges {} for stage {}.", input, stageIdx());

    long totalCount = approxTotalRowCount();

    long mean = 0;

    long accumulatedCount = 0;

    // TODO(vardhanvthigle): moving the total count clcuation to combiner will remove code
    // duplication with {@link MergeRangeDoFn}.
    // Refine the Count.
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

    for (Range range : input) {
      if (range.isUncounted()
          || range.count()
              > ((1 + ReadWithUniformPartitions.SPLITTER_MAX_RELATIVE_DEVIATION) * mean)) {
        if (stageIdx() == 0) {
          // For the first stage, we have an initial split without the counts.
          c.output(TO_COUNT_TAG, range);
        } else if (range.isSplittable(c)) {
          Pair<Range, Range> splitPair = range.split(c);
          logger.debug(
              "Counting range {} and {} for stage {}.",
              splitPair.getLeft(),
              splitPair.getRight(),
              stageIdx());
          c.output(TO_COUNT_TAG, splitPair.getLeft());
          c.output(TO_COUNT_TAG, splitPair.getRight());
        } else {
          if (range.height() + 1 < partitionColumns().size()) {
            PartitionColumn newColumn = partitionColumns().get((int) (range.height() + 1));
            ColumnForBoundaryQuery columnForBoundaryQuery =
                ColumnForBoundaryQuery.builder()
                    .setPartitionColumn(newColumn)
                    .setParentRange(range)
                    .build();
            logger.debug("Adding Column {} for stage {}.", columnForBoundaryQuery, stageIdx());
            c.output(TO_ADD_COLUMN_TAG, columnForBoundaryQuery);
          } else {
            logger.debug("Retaining range {} for stage {}.", range, stageIdx());
            c.output(TO_RETAIN_TAG, range);
          }
        }
      } else {
        logger.debug("Retaining range {} for stage {}.", range, stageIdx());
        c.output(TO_RETAIN_TAG, range);
      }
    }
  }

  public static Builder builder() {
    return new AutoValue_RangeClassifierDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPartitionColumns(ImmutableList<PartitionColumn> value);

    public abstract Builder setApproxTotalRowCount(Long value);

    public abstract Builder setMaxPartitionHint(Long value);

    public abstract Builder setAutoAdjustMaxPartitions(Boolean value);

    public abstract Builder setStageIdx(Long value);

    public abstract RangeClassifierDoFn build();
  }
}
