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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import java.util.List;

/** Tracks the shard file creation progress. */
public class FileCreationTracker {

  private SpannerDao spannerDao;

  public FileCreationTracker(
      String spannerProjectId, String metadataInstance, String metadataDatabase) {
    this.spannerDao = new SpannerDao(spannerProjectId, metadataInstance, metadataDatabase);
  }

  public void init(String shardFileLocation) {
    List<Shard> shards = ShardFileReader.getOrderedShardDetails(shardFileLocation);
    spannerDao.initShardProgress(shards);
  }

  public void updateProgress(String shard, String endTime) {
    this.spannerDao.updateProgress(shard, endTime);
  }

  public void close() {
    this.spannerDao.close();
  }
}
