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
import com.google.cloud.teleport.v2.templates.utils.FileCreationTracker;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Captures the progress of the files created per shard. */
public class FileProgressTrackerFn extends DoFn<KV<String, String>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(FileProgressTrackerFn.class);

  private final SpannerConfig spannerConfig;

  private String tableSuffix;
  private String runId;
  private transient FileCreationTracker fileCreationTracker;
  private boolean isMetadataDbPostgres;

  public FileProgressTrackerFn(
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
        fileCreationTracker =
            new FileCreationTracker(spannerConfig, tableSuffix, runId, isMetadataDbPostgres);
        retry = false;
      } catch (SpannerException e) {
        LOG.info("Exception in setup of ChangeDataProgressTrackerFn {}", e.getMessage());
        if (e.getMessage().contains("RESOURCE_EXHAUSTED")) {
          try {
            Thread.sleep(10000);
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
    fileCreationTracker.close();
  }

  /**
   * This captures the file creation progress per shard in the shard_file_create_progress table.This
   * method captures the latest window, per shard, successfully written up to this point in GCS.The
   * table shard_file_create_progress is checked by the gcs-to-sourcedb pipeline to check
   * progress.It ensures that the gcs-to-sourcedb pipeline will wait for files until this pipeline
   * has progressed.
   */
  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    KV<String, String> element = c.element();
    String fileName = element.getValue();
    String shardId = element.getKey();
    Counter numFilesWrittenMetric = Metrics.counter(shardId, "num_files_written_" + shardId);
    numFilesWrittenMetric.inc();

    if (window instanceof IntervalWindow) {
      IntervalWindow iw = (IntervalWindow) window;
      fileCreationTracker.updateProgress(shardId, iw.end().toString());

    } else {
      // Ideally we should not reach here, but adding this else for any unknown scenario
      fileCreationTracker.updateProgress(shardId, window.maxTimestamp().toString());
    }
  }
}
