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
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardProgressTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ShardProgressTracker.class);

  private SpannerDao spannerDao;

  public ShardProgressTracker(
      String spannerProjectId, String metadataInstance, String metadataDatabase) {

    this.spannerDao = new SpannerDao(spannerProjectId, metadataInstance, metadataDatabase);
  }

  public void init() {
    // TODO:: add PostgreSQL dialect handling
    spannerDao.checkAndcreateShardProgressTable();
  }

  public Map<String, ShardProgress> getShardProgress() {
    return spannerDao.getShardProgress();
  }

  public void writeShardProgress(ShardProgress shardProgress) {
    spannerDao.writeShardProgress(shardProgress);
  }

  public void close() {
    spannerDao.close();
  }
}
