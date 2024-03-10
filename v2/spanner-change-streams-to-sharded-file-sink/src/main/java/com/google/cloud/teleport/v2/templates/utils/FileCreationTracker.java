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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Tracks the shard file creation progress. */
public class FileCreationTracker {

  private SpannerDao spannerDao;
  private String runId;

  public FileCreationTracker(
      SpannerConfig spannerConfig, String tableSuffix, String runId, boolean isPostgres) {
    this.spannerDao = new SpannerDao(spannerConfig, tableSuffix, isPostgres);
    this.runId = runId;
  }

  public FileCreationTracker(SpannerDao spannerDao, String runId) {
    this.spannerDao = spannerDao;
    this.runId = runId;
  }

  public void init(List<Shard> shards) {
    spannerDao.initShardProgress(shards, runId);
  }

  public void updateProgress(String shard, String endTime) {
    boolean retry = true;
    while (retry) {
      try {
        this.spannerDao.updateProgress(shard, endTime, this.runId);
        retry = false;
      } catch (SpannerException e) {
        if (e.getMessage().contains("DEADLINE_EXCEEDED")) {
          try {
            Thread.sleep(500);
          } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
          }
          continue;
        } else {
          throw e;
        }
      }
    }
  }

  public void close() {
    this.spannerDao.close();
  }
}
