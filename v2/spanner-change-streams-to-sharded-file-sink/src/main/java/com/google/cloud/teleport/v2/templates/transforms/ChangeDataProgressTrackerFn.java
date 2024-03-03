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

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.utils.DataSeenTracker;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
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

  private String tableSuffix;
  private String runId;
  private final SpannerConfig spannerConfig;
  private boolean isMetadataDbPostgres;
  private transient DataSeenTracker dataSeenTracker;

  public ChangeDataProgressTrackerFn(
      SpannerConfig spannerConfig, String tableSuffix, String runId, boolean isMetadataDbPostgres) {
    this.spannerConfig = spannerConfig;
    this.tableSuffix = tableSuffix;
    this.runId = runId;
    this.isMetadataDbPostgres = isMetadataDbPostgres;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    boolean retry = true;
    while (retry) {
      try {
        dataSeenTracker =
            new DataSeenTracker(spannerConfig, tableSuffix, runId, isMetadataDbPostgres);
        retry = false;
      } catch (SpannerException e) {
        LOG.info("Exception in setup of ChangeDataProgressTrackerFn {}", e.getMessage());
        if (e.getMessage().contains("RESOURCE_EXHAUSTED")) {
          try {
            Thread.sleep(2000);
          } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } catch (Exception e) {
        throw e;
      }
    }
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
      dataSeenTracker.updateDataSeen(shardId, endTimestamp);

    } else {
      // Ideally we should not reach here, but adding this else for any unknown scenario
      String endTimestamp = window.maxTimestamp().toString();
      String windowKey = shardId + "_" + endTimestamp;
      LOG.info("Writing bundled window {} for shard {} ", endTimestamp, shardId);
      dataSeenTracker.updateDataSeen(shardId, endTimestamp);
    }
    c.output(dataChangeRecord);
  }
}
