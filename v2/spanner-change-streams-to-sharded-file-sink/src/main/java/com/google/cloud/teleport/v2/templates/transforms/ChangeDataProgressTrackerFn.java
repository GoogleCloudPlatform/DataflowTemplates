/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.utils.DataSeenTracker;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Captures the data seen per window per shard. */
public class ChangeDataProgressTrackerFn
    extends DoFn<TrimmedShardedDataChangeRecord, TrimmedShardedDataChangeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeDataProgressTrackerFn.class);

  private String spannerProjectId;
  private String spannerInstance;
  private String spannerDatabase;
  private String tableSuffix;
  private String runId;
  private DataSeenTracker dataSeenTracker;
  private transient Set<String> windowSeen;

  public ChangeDataProgressTrackerFn(
      String spannerProjectId,
      String spannerInstance,
      String spannerDatabase,
      String tableSuffix,
      String runId) {
    this.spannerProjectId = spannerProjectId;
    this.spannerInstance = spannerInstance;
    this.spannerDatabase = spannerDatabase;
    this.tableSuffix = tableSuffix;
    this.runId = runId;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    dataSeenTracker =
        new DataSeenTracker(spannerProjectId, spannerInstance, spannerDatabase, tableSuffix, runId);
    windowSeen = new HashSet<>();
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    dataSeenTracker.close();
  }

  /**
   * Captures the window end timestamp of the change data record in the data_seen table.We need to
   * do this, so that the gcs-to-sourcedb pipeline can decide whether or not to skip a file in case
   * it is not there in GCS.Basically data_seen gives an indication that data exists for a given
   * window and consequently file should exist too.
   */
  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    TrimmedShardedDataChangeRecord dataChangeRecord = c.element();
    String shardId = dataChangeRecord.getShard();

    // This is an interval window in all cases
    // so it should ideally not go in else
    if (window instanceof IntervalWindow) {
      IntervalWindow iw = (IntervalWindow) window;
      String endTimestamp = iw.end().toString();
      String windowKey = shardId + "_" + endTimestamp;
      if (!windowSeen.contains(windowKey)) {
        LOG.info("Writing window {} for shard {} ", endTimestamp, shardId);
        dataSeenTracker.updateDataSeen(shardId, endTimestamp);
        // we do not want to keep updating data_seen for every record that has the same window
        windowSeen.add(windowKey);
      }

    } else {
      // Ideally we should not reach here, but adding this else for any unknown scenario
      String endTimestamp = window.maxTimestamp().toString();
      String windowKey = shardId + "_" + endTimestamp;
      if (!windowSeen.contains(windowKey)) {
        LOG.info("Writing bundled window {} for shard {} ", endTimestamp, shardId);
        dataSeenTracker.updateDataSeen(shardId, endTimestamp);
        windowSeen.add(windowKey);
      }
    }
    c.output(dataChangeRecord);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    windowSeen.clear();
  }
}
