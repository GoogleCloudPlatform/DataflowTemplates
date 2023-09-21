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

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.Shard;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.common.SpannerToGcsJobMetadata;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create the processing context. */
public class ProcessingContextGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessingContextGenerator.class);

  public static Map<String, ProcessingContext> getProcessingContextForGCS(
      String sourceShardsFilePath,
      String sourceType,
      String sessionFilePath,
      String sourceDbTimezoneOffset,
      String startTimestamp,
      String windowDuration,
      String gcsInputDirectoryPath,
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String runMode) {

    LOG.info(" In getProcessingContextForGCS");

    List<Shard> shards = InputFileReader.getOrderedShardDetails(sourceShardsFilePath, sourceType);
    Schema schema = SessionFileReader.read(sessionFilePath);

    Map<String, ProcessingContext> response = new HashMap<>();

    ShardProgressTracker shardProgressTracker =
        new ShardProgressTracker(spannerProjectId, metadataInstance, metadataDatabase);
    shardProgressTracker.init();

    /*
    In regular mode, we process all shards.
    In reprocess mode, we only process shards marked for REPROCESS in shard_progress table.*/
    if ("regular".equals(runMode)) {

      if (startTimestamp == null
          || startTimestamp.isEmpty()
          || windowDuration == null
          || windowDuration.isEmpty()) {
        // Get the start time and duration from spanner_to_gcs_metadata table
        try {
          SpannerToGcsJobMetadata spannerToGcsJobMetadata =
              SpannerToGcsJobMetadataFetcher.getSpannerToGcsJobMetadata(
                  spannerProjectId, metadataInstance, metadataDatabase);
          windowDuration = spannerToGcsJobMetadata.getWindowDuration();
          startTimestamp = spannerToGcsJobMetadata.getStartTimestamp();
          LOG.info("The start timestamp  from Spanner to GCS job is : {}", startTimestamp);
          LOG.info("The window duration from Spanner to GCS job is : {}", windowDuration);
        } catch (Exception e) {
          LOG.error("Unable to get data from spanner_to_gcs_metadata");
          throw new RuntimeException("Unable to get data from spanner_to_gcs_metadata.", e);
        }
      }

      for (Shard shard : shards) {
        LOG.info(" The sorted shard is: {}", shard);

        Duration duration = DurationUtils.parseDuration(windowDuration);
        ProcessingContext taskContext =
            new ProcessingContext(
                shard,
                schema,
                sourceDbTimezoneOffset,
                startTimestamp,
                duration,
                gcsInputDirectoryPath,
                spannerProjectId,
                metadataInstance,
                metadataDatabase);
        response.put(shard.getLogicalShardId(), taskContext);
      }
    } else if ("reprocess".equals(runMode)) {
      Map<String, ShardProgress> shardProgressList = shardProgressTracker.getShardProgress();

      for (Shard shard : shards) {
        LOG.info(" The sorted shard is: {}", shard);
        ShardProgress shardProgress = shardProgressList.get(shard.getLogicalShardId());
        String shardStartTime = startTimestamp;
        if (shardProgress != null) {
          Instant shardStartTimeInst = new Instant(shardProgress.getStart().toSqlTimestamp());
          shardStartTime = shardStartTimeInst.toString();
          LOG.info(" The shard is startime is : {}", shardStartTime);
        } else {
          LOG.info(" Skipping non-reprocess shard: {}", shard);
          continue; // this shard was not marked for reprocess
        }
        Duration duration = DurationUtils.parseDuration(windowDuration);
        ProcessingContext taskContext =
            new ProcessingContext(
                shard,
                schema,
                sourceDbTimezoneOffset,
                shardStartTime,
                duration,
                gcsInputDirectoryPath,
                spannerProjectId,
                metadataInstance,
                metadataDatabase);
        response.put(shard.getLogicalShardId(), taskContext);
      }
    }
    shardProgressTracker.close();
    return response;
  }
}
