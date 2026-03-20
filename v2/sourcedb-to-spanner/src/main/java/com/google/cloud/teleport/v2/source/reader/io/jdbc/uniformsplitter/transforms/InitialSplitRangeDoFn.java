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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates initial splits for the first partition column for multiple tables, assuming uniform
 * density to begin with.
 *
 * <p>This {@link DoFn} handles the bootstrapping of the partitioning process for a collection of
 * tables. It respects table-specific configuration, such as {@link
 * TableSplitSpecification#initialSplitHeight()}, ensuring that each table starts with a granularity
 * appropriate for its size and distribution.
 */
@AutoValue
public abstract class InitialSplitRangeDoFn extends DoFn<Range, ImmutableList<Range>> {

  abstract ImmutableMap<TableIdentifier, Long> initialSplitsHeights();

  private static final Logger logger = LoggerFactory.getLogger(InitialSplitRangeDoFn.class);

  /**
   * Processes an initial {@link Range} (representing the min/max of the primary partition column)
   * and produces a list of split ranges for the iterative splitting process.
   *
   * @param input the initial range for a table.
   * @param out output receiver for the list of split ranges.
   * @param c process context.
   */
  @ProcessElement
  public void processElement(
      @Element Range input, OutputReceiver<ImmutableList<Range>> out, ProcessContext c) {
    // Note Searching for "RWUPT -" for ReadWithUniformPartition gives the most import logs for the
    // splitting process.
    logger.info(
        "RWUPT - Began split process for table {} with initial range as {}",
        input.tableIdentifier(),
        input);

    ArrayList<Range> ranges = new ArrayList<Range>();
    ranges.add(input.toBuilder().build());
    ArrayList<Range> splitRanges = new ArrayList<>();
    if (!initialSplitsHeights().containsKey(input.tableIdentifier())) {
      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {}",
          input,
          initialSplitsHeights());
      throw new RuntimeException("Invalid Range");
    }
    Long initialSplitsHeight = initialSplitsHeights().get(input.tableIdentifier());
    for (long i = 0; i < initialSplitsHeight; i++) {
      logger.info(
          "RWUPT - Creating initial split for table {}. Iteration {} of {}",
          input.tableIdentifier(),
          i,
          initialSplitsHeight);
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
    logger.info(
        "RWUPT - Completed initial split for table {} with initial range as {}, and {} split ranges",
        input.tableIdentifier(),
        input,
        ranges.size());
    out.output(ImmutableList.copyOf(ranges));
  }

  public static Builder builder() {
    return new AutoValue_InitialSplitRangeDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract ImmutableMap.Builder initialSplitsHeightsBuilder();

    public Builder setTableSplitSpecification(TableSplitSpecification tableSplitSpecification) {
      this.initialSplitsHeightsBuilder()
          .put(
              tableSplitSpecification.tableIdentifier(),
              tableSplitSpecification.initialSplitHeight());
      return this;
    }

    public Builder setTableSplitSpecifications(
        List<TableSplitSpecification> tableSplitSpecifications) {
      tableSplitSpecifications.forEach(t -> setTableSplitSpecification(t));
      return this;
    }

    public abstract InitialSplitRangeDoFn build();
  }
}
