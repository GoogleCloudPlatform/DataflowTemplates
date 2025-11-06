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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classifies ranges into ranges to count, split, retain, or graduate.
 *
 * <p>This {@link DoFn} acts as the central logic for the iterative splitting process. It examines a
 * batch of ranges and decides their next stage based on their current size (count) relative to a
 * target mean.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li><b>Heterogeneous Processing</b>: Handles ranges from multiple tables independently by
 *       grouping them by {@link TableIdentifier}.
 *   <li><b>Early Graduation</b>: Implements the "Graduation Lane" optimization where tables that
 *       have reached their maximum split height are immediately marked as ready for reading.
 *   <li><b>Multi-Column Splitting</b>: When a range on a single column becomes too large but cannot
 *       be split further (e.g., all rows have the same value for that column), it triggers the
 *       addition of the next configured partition column.
 * </ul>
 */
@AutoValue
public abstract class RangeClassifierDoFn extends DoFn<KV<Integer, ImmutableList<Range>>, Range>
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

  // Note it is necessary to retain `new TupleTag<Range>() {}`  though editor might point to
  // dropping `Range` or the `{}`.
  // It is necessary to retain the type information for the coder inference of beam to work.
  // Dropping this will make `ReadWithUniformPartitionsTest` fail with coder not found error.
  public static final TupleTag<Range> READY_TO_READ_TAG = new TupleTag<Range>() {};

  // Note it is necessary to retain `new TupleTag<ColumnForBoundaryQuery>() {}`  though editor might
  // point to dropping `ColumnForBoundaryQuery` or the `{}`.
  // It is necessary to retain the type information for the coder inference of beam to work.
  // Dropping this will make `ReadWithUniformPartitionsTest` fail with coder not found error.
  public static final TupleTag<ColumnForBoundaryQuery> TO_ADD_COLUMN_TAG =
      new TupleTag<ColumnForBoundaryQuery>() {};

  /** Table Split Specification. */
  abstract ImmutableMap<TableIdentifier, TableSplitSpecification> tableSplitSpecifications();

  /**
   * If true, MaxPartitions will be inferred again after total counts of all ranges is not
   * in-determinate.
   */
  abstract Boolean autoAdjustMaxPartitions();

  /** Stage Index. */
  abstract Long stageIdx();

  /**
   * Processes a batch of ranges, grouping them by table and classifying them.
   *
   * @param input the batch of ranges keyed by a synthetic bucket ID.
   * @param c the process context.
   */
  @ProcessElement
  public void processElement(@Element KV<Integer, ImmutableList<Range>> input, ProcessContext c) {
    logger.debug("Classifying ranges for batch {} for stage {}.", input.getKey(), stageIdx());
    if (input.getValue().isEmpty()) {
      return;
    }
    // Group ranges by TableIdentifier to process tables independently.
    Map<TableIdentifier, List<Range>> rangesByTable = new HashMap<>();

    input.getValue().stream()
        .forEach(
            range -> {
              if (!rangesByTable.containsKey(range.tableIdentifier())) {
                rangesByTable.put(range.tableIdentifier(), new ArrayList<>());
              }
              rangesByTable.get(range.tableIdentifier()).add(range);
            });

    for (Map.Entry<TableIdentifier, List<Range>> entry : rangesByTable.entrySet()) {
      TableIdentifier tableIdentifier = entry.getKey();
      List<Range> ranges = entry.getValue();
      Collections.sort(ranges);
      processTable(tableIdentifier, ranges, c);
    }
  }

  private void processTable(TableIdentifier tableId, List<Range> ranges, ProcessContext c) {

    if (!tableSplitSpecifications().containsKey(tableId)) {
      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {}",
          ranges,
          tableSplitSpecifications());
      throw new RuntimeException("Invalid Range");
    }
    TableSplitSpecification tableSplitSpecification = tableSplitSpecifications().get(tableId);

    // Determine maxSplitHeight for this table
    long tableMaxSplitHeight = tableSplitSpecification.splitStagesCount();

    // Graduation check: if the table is uniform (not yet implemented, assuming false for now)
    // or if the current stageIdx has reached or exceeded the table's maxSplitHeight.
    // TODO(vardhanvthigle): Add actual uniformity check when implemented.
    if (stageIdx() >= tableMaxSplitHeight) {
      // THE GRADUATION LANE:
      // If a table has reached its configured max split height (depth), we graduate all its
      // ranges immediately to the read phase. This prevents complex or skewed tables from
      // forcing simple tables to wait through unnecessary loop iterations, significantly
      // improving pipeline efficiency and resource utilization.
      logger.debug("Graduating table {} at stage {}.", tableId.tableName(), stageIdx());
      for (Range range : ranges) {
        c.output(READY_TO_READ_TAG, range);
      }
      return; // All ranges for this table are graduated, no further classification needed.
    }

    long totalCount = tableSplitSpecification.approxRowCount();
    long mean = 0;
    long accumulatedCount = 0;

    // Refine the Count.
    for (Range range : ranges) {
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

    for (Range range : ranges) {
      if (range.isUncounted()
          || range.count()
              > ((1 + TableSplitSpecification.SPLITTER_MAX_RELATIVE_DEVIATION) * mean)) {
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
          if (range.height() + 1 < tableSplitSpecification.partitionColumns().size()) {
            PartitionColumn newColumn =
                tableSplitSpecification.partitionColumns().get((int) (range.height() + 1));
            ColumnForBoundaryQuery columnForBoundaryQuery =
                ColumnForBoundaryQuery.builder()
                    .setTableIdentifier(range.tableIdentifier())
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

    public abstract Builder setStageIdx(Long value);

    public abstract RangeClassifierDoFn build();
  }
}
