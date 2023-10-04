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
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Checks the file creation progress of the Spanner to GCS job. */
public class ShardFileCreationTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ShardFileCreationTracker.class);
  private SpannerDao spannerDao;
  private String shardId;

  public ShardFileCreationTracker(
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String shardId,
      String tableSuffix) {

    this.spannerDao =
        new SpannerDao(spannerProjectId, metadataInstance, metadataDatabase, tableSuffix);
    this.shardId = shardId;
  }

  public Timestamp getShardFileCreationProgressTimestamp() {
    Timestamp response = spannerDao.getShardFileCreationProgressTimestamp(shardId);
    return response;
  }

  public void close() {
    spannerDao.close();
  }
}
