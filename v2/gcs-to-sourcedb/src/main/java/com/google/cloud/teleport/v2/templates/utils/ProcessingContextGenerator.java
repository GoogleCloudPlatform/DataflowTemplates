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

import com.google.cloud.teleport.v2.spanner.migrations.metadata.SpannerToGcsJobMetadata;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.constants.Constants;
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
      String runMode,
      String tableSuffix,
      String runId,
      boolean isMetadataDbPostgres) {

    LOG.info(" In getProcessingContextForGCS");

    if (!"mysql".equals(sourceType)) {
      LOG.error("Only mysql source type is supported.");
      throw new RuntimeException(
          "Input sourceType value : "
              + sourceType
              + " is unsupported. Supported values are : mysql");
    }

    Schema schema = SessionFileReader.read(sessionFilePath);
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails(sourceShardsFilePath);

    ShardProgressTracker shardProgressTracker =
        new ShardProgressTracker(
            spannerProjectId,
            metadataInstance,
            metadataDatabase,
            tableSuffix,
            runId,
            isMetadataDbPostgres);
    shardProgressTracker.init();

    Map<String, ProcessingContext> response = null;

    if (runMode.equals(Constants.RUN_MODE_REGULAR)) {
      response =
          getProcessingContextForRegularMode(
              sourceDbTimezoneOffset,
              startTimestamp,
              windowDuration,
              gcsInputDirectoryPath,
              spannerProjectId,
              metadataInstance,
              metadataDatabase,
              tableSuffix,
              runId,
              shards,
              schema,
              isMetadataDbPostgres);
    } else {
      response =
          getProcessingContextForReprocessOrResumeModes(
              sourceDbTimezoneOffset,
              gcsInputDirectoryPath,
              spannerProjectId,
              metadataInstance,
              metadataDatabase,
              tableSuffix,
              runId,
              shards,
              schema,
              shardProgressTracker,
              runMode,
              isMetadataDbPostgres);
    }

    shardProgressTracker.close();
    return response;
  }

  private static Map<String, ProcessingContext> getProcessingContextForRegularMode(
      String sourceDbTimezoneOffset,
      String startTimestamp,
      String windowDuration,
      String gcsInputDirectoryPath,
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String tableSuffix,
      String runId,
      List<Shard> shards,
      Schema schema,
      boolean isMetadataDbPostgres) {

    Map<String, ProcessingContext> response = new HashMap<>();
    if (startTimestamp == null
        || startTimestamp.isEmpty()
        || windowDuration == null
        || windowDuration.isEmpty()) {
      // Get the start time and duration from spanner_to_gcs_metadata table
      try {
        SpannerToGcsJobMetadata spannerToGcsJobMetadata =
            SpannerToGcsJobMetadataFetcher.getSpannerToGcsJobMetadata(
                spannerProjectId,
                metadataInstance,
                metadataDatabase,
                tableSuffix,
                runId,
                isMetadataDbPostgres);
        windowDuration = spannerToGcsJobMetadata.getWindowDuration();
        startTimestamp = spannerToGcsJobMetadata.getStartTimestamp();
        LOG.info("The start timestamp  from Spanner to GCS job is : {}", startTimestamp);
        LOG.info("The window duration from Spanner to GCS job is : {}", windowDuration);
      } catch (Exception e) {
        LOG.error("Unable to get data from spanner_to_gcs_metadata");
        throw new RuntimeException("Unable to get data from spanner_to_gcs_metadata.", e);
      }
    }

    Duration duration = DurationUtils.parseDuration(windowDuration);

    for (Shard shard : shards) {
      LOG.info(" The sorted shard is: {}", shard);
      ProcessingContext taskContext =
          new ProcessingContext(
              shard,
              schema,
              sourceDbTimezoneOffset,
              startTimestamp,
              duration,
              gcsInputDirectoryPath,
              runId);
      response.put(shard.getLogicalShardId(), taskContext);
    }

    return response;
  }

  private static Map<String, ProcessingContext> getProcessingContextForReprocessOrResumeModes(
      String sourceDbTimezoneOffset,
      String gcsInputDirectoryPath,
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String tableSuffix,
      String runId,
      List<Shard> shards,
      Schema schema,
      ShardProgressTracker shardProgressTracker,
      String runMode,
      boolean isMetadataDbPostgres) {

    String status = "";
    switch (runMode) {
      case Constants.RUN_MODE_RESUME_SUCCESS:
        status = Constants.SHARD_PROGRESS_STATUS_SUCCESS;
        break;
      case Constants.RUN_MODE_RESUME_FAILED:
        status = Constants.SHARD_PROGRESS_STATUS_ERROR;
        break;
      case Constants.RUN_MODE_REPROCESS:
        status = Constants.SHARD_PROGRESS_STATUS_REPROCESS;
        break;
    }

    Map<String, ShardProgress> shardProgressList = null;
    if (runMode.equals(Constants.RUN_MODE_RESUME_ALL)) {
      shardProgressList = shardProgressTracker.getAllShardProgress();
    } else {
      shardProgressList = shardProgressTracker.getShardProgressByStatus(status);
    }

    Map<String, ProcessingContext> response = new HashMap<>();
    String windowDuration = null;
    try {
      SpannerToGcsJobMetadata spannerToGcsJobMetadata =
          SpannerToGcsJobMetadataFetcher.getSpannerToGcsJobMetadata(
              spannerProjectId,
              metadataInstance,
              metadataDatabase,
              tableSuffix,
              runId,
              isMetadataDbPostgres);
      windowDuration = spannerToGcsJobMetadata.getWindowDuration();
      LOG.info("The window duration from Spanner to GCS job is : {}", windowDuration);
    } catch (Exception e) {
      LOG.error("Unable to get data from spanner_to_gcs_metadata");
      throw new RuntimeException("Unable to get data from spanner_to_gcs_metadata.", e);
    }

    Duration duration = DurationUtils.parseDuration(windowDuration);

    for (Shard shard : shards) {
      LOG.info(" The sorted shard is: {}", shard);
      ShardProgress shardProgress = shardProgressList.get(shard.getLogicalShardId());

      String shardStartTime = null;
      if (shardProgress != null) {
        Instant shardStartTimeInst =
            new Instant(shardProgress.getFileStartInterval().toSqlTimestamp());
        if ((runMode.equals(Constants.RUN_MODE_RESUME_SUCCESS)
                || runMode.equals(Constants.RUN_MODE_RESUME_ALL))
            && shardProgress.getStatus().equals(Constants.SHARD_PROGRESS_STATUS_SUCCESS)) {
          // Advance the start time by window duration for successful shards
          shardStartTimeInst = shardStartTimeInst.plus(duration);
        }
        shardStartTime = shardStartTimeInst.toString();

        LOG.info(" The startTime for shard {} is : {}", shard, shardStartTime);
      } else {
        LOG.info(
            " Skipping shard: {} as it does not qualify for given runMode {}.", shard, runMode);
        continue;
      }
      ProcessingContext taskContext =
          new ProcessingContext(
              shard,
              schema,
              sourceDbTimezoneOffset,
              shardStartTime,
              duration,
              gcsInputDirectoryPath,
              runId);
      response.put(shard.getLogicalShardId(), taskContext);
    }
    return response;
  }
}
