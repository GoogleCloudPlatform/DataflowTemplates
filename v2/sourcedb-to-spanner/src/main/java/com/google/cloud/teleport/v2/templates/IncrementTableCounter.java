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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that waits for the completion of table migrations (represented by {@link
 * Wait.OnSignal} objects) and increments a counter for each completed table.
 */
public class IncrementTableCounter extends PTransform<PBegin, PCollection<Void>> {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementTableCounter.class);

  private static final Counter tablesCompleted =
      Metrics.counter(IncrementTableCounter.class, MetricCounters.TABLES_COMPLETED);

  private final Map<Integer, Wait.OnSignal<?>> levelWaits;

  private final String shardId;

  private final Map<Integer, List<String>> levelToSpannerTables;

  /**
   * @param levelWaits Map of src table name to Wait.OnSignals, denoting which pcollection should a
   *     table wait on to signal completion.
   * @param shardId logical shard id for the set of tables.
   * @param levelToSpannerTables level wise list of spanner Tables.
   */
  public IncrementTableCounter(
      Map<Integer, Wait.OnSignal<?>> levelWaits,
      String shardId,
      Map<Integer, List<String>> levelToSpannerTables) {
    this.levelWaits = levelWaits;
    this.shardId = shardId;
    this.levelToSpannerTables = levelToSpannerTables;
  }

  @Override
  public PCollection<Void> expand(PBegin input) {
    List<PCollection<Integer>> tableNamesPCollections = new ArrayList<>();
    // The level names are used as dummy elements to trigger the counter increment, as the original
    // SpannerIO output (PCollection<Void>) doesn't contain elements. This is needed to increment
    // counters. (Running this directly on Spanner output does not increment counters).
    // We merge and flatten the pcollections to ensure beam keeps a single counter for all tables.
    for (Map.Entry<Integer, Wait.OnSignal<?>> entry : levelWaits.entrySet()) {
      int level = entry.getKey();
      Wait.OnSignal<?> wait = entry.getValue();
      PCollection<Integer> waitedPCollection =
          wait(input.apply("Create_" + level, Create.of(level)), wait, level);
      tableNamesPCollections.add(waitedPCollection);
    }
    PCollectionList<Integer> delayedLevelList = PCollectionList.of(tableNamesPCollections);
    PCollection<Integer> mergedLevels = delayedLevelList.apply(Flatten.pCollections());
    return mergedLevels.apply(
        "IncrementCounter",
        ParDo.of(
            new DoFn<Integer, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                List<String> spannerTables = levelToSpannerTables.get(c.element());
                for (String table : spannerTables) {
                  String msg = String.format("Completed spanner table: %s", table);
                  if (!StringUtils.isEmpty(shardId)) {
                    msg += " for shard: " + shardId;
                  }
                  LOG.info(msg);
                  tablesCompleted.inc();
                }
              }
            }));
  }

  private <V extends PCollection> V wait(V input, Wait.OnSignal<?> wait, int level) {
    if (wait == null) {
      return input;
    } else {
      return (V) input.apply("Add_waits_signal_" + level, wait);
    }
  }
}
