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

import com.google.cloud.teleport.v2.templates.utils.FileCreationTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Captures the progress of the files created per shard. */
public class FileProgressTracker extends DoFn<KV<String, String>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(FileProgressTracker.class);

  private String spannerProjectId;
  private String spannerInstance;
  private String spannerDatabase;
  private FileCreationTracker fileCreationTracker;

  public FileProgressTracker(
      String spannerProjectId, String spannerInstance, String spannerDatabase) {
    this.spannerProjectId = spannerProjectId;
    this.spannerInstance = spannerInstance;
    this.spannerDatabase = spannerDatabase;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    fileCreationTracker =
        new FileCreationTracker(spannerProjectId, spannerInstance, spannerDatabase);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    fileCreationTracker.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    KV<String, String> element = c.element();
    String fileName = element.getValue();
    String shardId = element.getKey();

    if (window instanceof IntervalWindow) {
      IntervalWindow iw = (IntervalWindow) window;
      String windowStr = String.format("%s-%s", iw.start().toString(), iw.end().toString());
      LOG.info(
          "The shard {} and  file name is: {} and interval is {}", shardId, fileName, windowStr);
      fileCreationTracker.updateProgress(shardId, iw.end().toString());

    } else {
      fileCreationTracker.updateProgress(shardId, window.maxTimestamp().toString());
    }
  }
}
