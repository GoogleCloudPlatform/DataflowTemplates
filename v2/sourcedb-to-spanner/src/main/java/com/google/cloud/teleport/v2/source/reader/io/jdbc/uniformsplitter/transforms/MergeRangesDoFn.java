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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that merges consecutive small ranges to approach a target mean count.
 *
 * <p>This transform is applied at the end of the splitting process to optimize the final number of
 * partitions. It handles ranges from multiple tables independently, ensuring that each table's
 * ranges are merged based on its specific {@link TableSplitSpecification#maxPartitionsHint()} and
 * actual data density.
 */
@AutoValue
public abstract class MergeRangesDoFn
    extends DoFn<KV<Integer, ImmutableList<Range>>, KV<Integer, ImmutableList<Range>>> {

  private static final Logger logger = LoggerFactory.getLogger(MergeRangesDoFn.class);

  /** Table Split Specification. */
  abstract ImmutableMap<TableIdentifier, TableSplitSpecification> tableSplitSpecifications();

  /**
   * If true, MaxPartitions will be inferred again after total counts of all ranges is not
   * in-determinate.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /**
   * Processes a batch of ranges, grouping them by table and merging them independently.
   *
   * @param input the batch of ranges keyed by a synthetic bucket ID.
   * @param out output receiver for the merged ranges.
   * @param c process context.
   */
  @ProcessElement
  public void processElement(
      @Element KV<Integer, ImmutableList<Range>> input,
      OutputReceiver<KV<Integer, ImmutableList<Range>>> out,
      ProcessContext c) {

    if (input.getValue().isEmpty()) {
      return;
    }

    // Group ranges by TableIdentifier to process tables independently.
    Map<TableIdentifier, List<Range>> rangesByTable = new HashMap<>();
    for (Range range : input.getValue()) {
      rangesByTable.computeIfAbsent(range.tableIdentifier(), k -> new ArrayList<>()).add(range);
    }

    ImmutableList.Builder<Range> allMergedRanges = ImmutableList.builder();

    for (Map.Entry<TableIdentifier, List<Range>> tableEntry : rangesByTable.entrySet()) {
      List<Range> tableRanges = tableEntry.getValue();
      // IMPORTANT: Sort ranges before merging
      Collections.sort(tableRanges);
      allMergedRanges.addAll(mergeRangesForTable(ImmutableList.copyOf(tableRanges), c));
    }

    out.output(KV.of(input.getKey(), allMergedRanges.build()));
  }

  public static Builder builder() {
    return new AutoValue_MergeRangesDoFn.Builder();
  }

  @VisibleForTesting
  protected ImmutableList<Range> mergeRangesForTable(ImmutableList<Range> input, ProcessContext c) {

    if (!tableSplitSpecifications().containsKey(input.get(0).tableIdentifier())) {
      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {}",
          input,
          tableSplitSpecifications());
      throw new RuntimeException("Invalid Range");
    }
    TableSplitSpecification tableSplitSpecification =
        tableSplitSpecifications().get(input.get(0).tableIdentifier());

    long totalCount = tableSplitSpecification.approxRowCount();

    long mean = 0;

    long accumulatedCount = 0;

    logger.info(
        "RWUPT - Began merging split-ranges for table {} initial split range count as {}",
        tableSplitSpecification.tableIdentifier(),
        input.size());

    // TODO(vardhanvthigle): moving the total count clcuation to combiner will remove code
    // duplication with {@link RangeClassifierDoFn}.
    // Refine the Count.
    for (Range range : input) {
      if (!range.tableIdentifier().equals(tableSplitSpecification.tableIdentifier())) {
        logger.error(
            "Got mismatched Range {} for tableSpecification {}.", range, tableSplitSpecification);
        throw new RuntimeException("Mismatched Range");
      }
      accumulatedCount = range.accumulateCount(accumulatedCount);
    }
    if (accumulatedCount != Range.INDETERMINATE_COUNT) {
      totalCount = accumulatedCount;
    }

    long maxPartitions = tableSplitSpecification.maxPartitionsHint();
    if (autoAdjustMaxPartitions()) {
      maxPartitions = TableSplitSpecification.inferMaxPartitions(totalCount);
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
        tableSplitSpecification.tableIdentifier(),
        input.size(),
        output.size());

    return output;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract ImmutableMap.Builder<TableIdentifier, TableSplitSpecification>
        tableSplitSpecificationsBuilder();

    public Builder setTableSplitSpecification(TableSplitSpecification tableSplitSpecification) {
      tableSplitSpecificationsBuilder()
          .put(tableSplitSpecification.tableIdentifier(), tableSplitSpecification);
      return this;
    }

    public Builder setTableSplitSpecifications(
        List<TableSplitSpecification> tableSplitSpecificationList) {
      tableSplitSpecificationList.forEach(t -> setTableSplitSpecification(t));
      return this;
    }

    public abstract Builder setAutoAdjustMaxPartitions(Boolean value);

    public abstract MergeRangesDoFn build();
  }
}
