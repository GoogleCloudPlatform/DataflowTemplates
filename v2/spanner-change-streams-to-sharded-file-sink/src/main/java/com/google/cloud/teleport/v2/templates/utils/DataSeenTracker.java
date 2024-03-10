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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Tracks the shard file creation progress. */
public class DataSeenTracker {

  private SpannerDao spannerDao;
  private String runId;

  public DataSeenTracker(
      SpannerConfig spannerConfig, String tableSuffix, String runId, boolean isPostgres) {
    this.spannerDao = new SpannerDao(spannerConfig, tableSuffix, isPostgres);
    this.runId = runId;
  }

  /**
   * Writes the data_seen table with the end time of the window encountered. The primary key is
   * generated so that there is no hotspotting.
   */
  public void updateDataSeen(String shard, String endTime) {
    // primary key is end time in epoch, convert to string,reverse string then append run id and
    // shard id, this gives a randomness to the primary key
    Timestamp endTimestamp = Timestamp.parseTimestamp(endTime);
    long seconds = endTimestamp.getSeconds();
    int nanos = endTimestamp.getNanos();
    String orig = String.valueOf(nanos) + String.valueOf(seconds);
    StringBuilder reversedString = new StringBuilder(orig);
    String id = reversedString.reverse() + "_" + runId + "_" + shard;
    boolean retry = true;
    while (retry) {
      try {
        if (!this.spannerDao.doesIdExist(id)) {
          this.spannerDao.updateDataSeen(id, shard, endTimestamp, runId);
        }
        retry = false;
      } catch (SpannerException e) {
        if (e.getMessage().contains("Already exists")
            || e.getMessage().contains("ALREADY_EXISTS")) {
          return;
        } else if (e.getMessage().contains("DEADLINE_EXCEEDED")) {
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
