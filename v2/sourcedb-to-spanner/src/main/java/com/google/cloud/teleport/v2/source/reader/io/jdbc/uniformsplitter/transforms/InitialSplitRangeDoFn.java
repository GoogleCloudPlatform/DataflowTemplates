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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates initial splits for the first partition column, upto max partitions, assuming uniform
 * density to begin with.
 */
@AutoValue
public abstract class InitialSplitRangeDoFn extends DoFn<Range, ImmutableList<Range>> {

  /**
   * How many iterations of splits to do. For example if you want 1024 ranges to get created
   * initially, set this to 10.
   *
   * @return split height for initial split.
   */
  abstract long splitHeight();

  abstract String tableName();

  private static final Logger logger = LoggerFactory.getLogger(InitialSplitRangeDoFn.class);

  /**
   * @param input Range indicating Min and Max of the first partition column.
   * @param out output receiver for a list of initial split of ranges. The ranges are not counted as
   *     yet.
   * @param c process context
   */
  @ProcessElement
  public void processElement(
      @Element Range input, OutputReceiver<ImmutableList<Range>> out, ProcessContext c) {
    // Note Searching for "RWUPT -" for ReadWithUniformPartition gives the most import logs for the
    // splitting process.
    logger.info(
        "RWUPT - Began split process for table {} with initial range as {}", tableName(), input);

    ArrayList<Range> ranges = new ArrayList<Range>();
    ranges.add(input.toBuilder().build());
    ArrayList<Range> splitRanges = new ArrayList<>();
    for (long i = 0; i < splitHeight(); i++) {
      logger.info(
          "RWUPT - Creating initial split for table {}. Iteration {} of {}",
          tableName(),
          i,
          splitHeight());
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
        tableName(),
        input,
        ranges.size());
    out.output(ImmutableList.copyOf(ranges));
  }

  public static Builder builder() {
    return new AutoValue_InitialSplitRangeDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSplitHeight(long value);

    public abstract Builder setTableName(String value);

    public abstract InitialSplitRangeDoFn build();
  }
}
