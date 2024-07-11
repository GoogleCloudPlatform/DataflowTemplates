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

  private Map<String, Wait.OnSignal<?>> tableWaits;

  private final String shardId;

  /**
   * @param tableWaits Map of src table name to Wait.OnSignals, denoting which pcollection should a
   *     table wait on to signal completion.
   * @param shardId logical shard id for the set of tables.
   */
  public IncrementTableCounter(Map<String, Wait.OnSignal<?>> tableWaits, String shardId) {
    this.tableWaits = tableWaits;
    this.shardId = shardId;
  }

  @Override
  public PCollection<Void> expand(PBegin input) {
    List<PCollection<String>> tableNamesPCollections = new ArrayList<>();
    // The table names are used as dummy elements to trigger the counter increment, as the original
    // SpannerIO output (PCollection<Void>) doesn't contain elements. This is needed to increment
    // counters. (Running this directly on Spanner output does not increment counters).
    // We merge and flatten the pcollections to ensure beam keeps a single counter for all tables.
    for (Map.Entry<String, Wait.OnSignal<?>> entry : tableWaits.entrySet()) {
      String tableName = entry.getKey();
      Wait.OnSignal<?> wait = entry.getValue();
      PCollection<String> waitedPCollection =
          wait(input.apply("Create_" + tableName, Create.of(tableName)), wait, tableName);
      tableNamesPCollections.add(waitedPCollection);
    }
    PCollectionList<String> delayedTableNamesList = PCollectionList.of(tableNamesPCollections);
    PCollection<String> mergedTableNames = delayedTableNamesList.apply(Flatten.pCollections());
    return mergedTableNames.apply(
        "IncrementCounter",
        ParDo.of(
            new DoFn<String, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                String msg = String.format("Completed table: %s", c.element());
                if (!StringUtils.isEmpty(shardId)) {
                  msg += " for shard: " + shardId;
                }
                LOG.info(msg);
                tablesCompleted.inc();
              }
            }));
  }

  private <V extends PCollection> V wait(V input, Wait.OnSignal<?> wait, String tableName) {
    if (wait == null) {
      return input;
    } else {
      return (V) input.apply("Add_waits_signal_" + tableName, wait);
    }
  }
}
