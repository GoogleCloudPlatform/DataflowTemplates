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

/** Utility class to handle shard progress related logic. */
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardProgressTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ShardProgressTracker.class);

  private SpannerDao spannerDao;
  private String runId;

  public ShardProgressTracker(
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String tableSuffix,
      String runId,
      boolean isMetadataDbPostgres) {

    this.spannerDao =
        new SpannerDao(
            spannerProjectId,
            metadataInstance,
            metadataDatabase,
            tableSuffix,
            isMetadataDbPostgres);
    this.runId = runId;
  }

  public ShardProgressTracker(SpannerDao spannerDao, String runId) {
    this.spannerDao = spannerDao;
    this.runId = runId;
  }

  public void init() {
    spannerDao.checkAndCreateShardProgressTable();
  }

  public Map<String, ShardProgress> getShardProgressByStatus(String status) {
    boolean retry = true;
    Map<String, ShardProgress> response = null;
    while (retry) {
      try {
        response = spannerDao.getShardProgressByRunIdAndStatus(runId, status);
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

    return response;
  }

  public Map<String, ShardProgress> getAllShardProgress() {
    boolean retry = true;
    Map<String, ShardProgress> response = null;
    while (retry) {
      try {
        response = spannerDao.getAllShardProgressByRunId(runId);
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

    return response;
  }

  public void writeShardProgress(ShardProgress shardProgress) {
    boolean retry = true;
    while (retry) {
      try {
        spannerDao.writeShardProgress(shardProgress, runId);
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
    spannerDao.close();
  }
}
