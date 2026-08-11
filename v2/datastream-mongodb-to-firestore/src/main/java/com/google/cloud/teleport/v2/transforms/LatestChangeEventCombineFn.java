/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * High-performance CombineFn that compacts bursts of change events for the same document key into
 * the latest monotonic event in worker RAM before shuffle and state evaluation.
 */
public class LatestChangeEventCombineFn
    extends CombineFn<
        MongoDbChangeEventContext, MongoDbChangeEventContext, MongoDbChangeEventContext> {

  @Override
  public MongoDbChangeEventContext createAccumulator() {
    return null;
  }

  @Override
  public MongoDbChangeEventContext addInput(
      MongoDbChangeEventContext accumulator, MongoDbChangeEventContext input) {
    if (accumulator == null) {
      return input;
    }
    if (input == null) {
      return accumulator;
    }

    TimestampSortKey accKey = TimestampSortKey.of(accumulator);
    TimestampSortKey inputKey = TimestampSortKey.of(input);

    int cmp = (accKey == null) ? 1 : inputKey.compareTo(accKey);

    // If input is strictly newer, or equal with DLQ reconsumption, prefer input
    if (cmp > 0 || (cmp == 0 && input.getIsDlqReconsumed())) {
      return input;
    }
    return accumulator;
  }

  @Override
  public MongoDbChangeEventContext mergeAccumulators(
      Iterable<MongoDbChangeEventContext> accumulators) {
    MongoDbChangeEventContext winner = null;
    TimestampSortKey maxKey = null;

    for (MongoDbChangeEventContext acc : accumulators) {
      if (acc != null) {
        TimestampSortKey key = TimestampSortKey.of(acc);
        if (winner == null
            || key.compareTo(maxKey) > 0
            || (key.compareTo(maxKey) == 0 && acc.getIsDlqReconsumed())) {
          maxKey = key;
          winner = acc;
        }
      }
    }
    return winner;
  }

  @Override
  public MongoDbChangeEventContext extractOutput(MongoDbChangeEventContext accumulator) {
    return accumulator;
  }
}
