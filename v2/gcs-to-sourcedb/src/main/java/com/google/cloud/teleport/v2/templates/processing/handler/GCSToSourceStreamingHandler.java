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
package com.google.cloud.teleport.v2.templates.processing.handler;

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dao.DaoFactory;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import com.google.cloud.teleport.v2.templates.utils.GCSReader;
import com.google.cloud.teleport.v2.templates.utils.ShardProgressTracker;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads from GCS orders and writes to source. */
public class GCSToSourceStreamingHandler {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceStreamingHandler.class);

  public static String process(ProcessingContext taskContext, SpannerDao spannerDao) {
    String shardId = taskContext.getShard().getLogicalShardId();
    GCSReader inputFileReader = new GCSReader(taskContext, spannerDao);
    String fileProcessedStartInterval = taskContext.getStartTimestamp();

    try {
      Instant readStartTime = Instant.now();
      List<TrimmedShardedDataChangeRecord> records = inputFileReader.getRecords();
      Instant readEndTime = Instant.now();
      LOG.info(
          "Shard "
              + shardId
              + ": read "
              + records.size()
              + " records from the buffer in "
              + ChronoUnit.MILLIS.between(readStartTime, readEndTime)
              + " milliseconds");
      // This may have changed in case the interval did not have data
      fileProcessedStartInterval = inputFileReader.getCurrentIntervalStart();
      if (records.isEmpty()) {
        markShardSuccess(taskContext, spannerDao, fileProcessedStartInterval);
        return fileProcessedStartInterval;
      }

      String connectString =
          "jdbc:mysql://"
              + taskContext.getShard().getHost()
              + ":"
              + taskContext.getShard().getPort()
              + "/"
              + taskContext.getShard().getDbName();

      MySqlDao dao =
          new DaoFactory(
                  connectString,
                  taskContext.getShard().getUserName(),
                  taskContext.getShard().getPassword())
              .getMySqlDao(shardId);

      InputRecordProcessor.processRecords(
          records, taskContext.getSchema(), dao, shardId, taskContext.getSourceDbTimezoneOffset());
      markShardSuccess(taskContext, spannerDao, fileProcessedStartInterval);
      dao.cleanup();
      LOG.info(
          "Shard " + shardId + ": Successfully processed batch of " + records.size() + " records.");
    } catch (Exception e) {
      Metrics.counter(GCSToSourceStreamingHandler.class, "shard_failed_" + shardId).inc();
      markShardFailure(taskContext, spannerDao, fileProcessedStartInterval);
      throw new RuntimeException("Failure when processing records", e);
    }
    return fileProcessedStartInterval;
  }

  private static void markShardSuccess(
      ProcessingContext taskContext, SpannerDao spannerDao, String fileProcessedStartInterval) {
    markShardProgress(
        taskContext,
        Constants.SHARD_PROGRESS_STATUS_SUCCESS,
        spannerDao,
        fileProcessedStartInterval);
  }

  private static void markShardProgress(
      ProcessingContext taskContext,
      String status,
      SpannerDao spannerDao,
      String fileProcessedStartInterval) {
    ShardProgressTracker shardProgressTracker =
        new ShardProgressTracker(spannerDao, taskContext.getRunId());
    com.google.cloud.Timestamp startTs = null;
    startTs = com.google.cloud.Timestamp.parseTimestamp(fileProcessedStartInterval);

    ShardProgress shardProgress =
        new ShardProgress(taskContext.getShard().getLogicalShardId(), startTs, status);

    shardProgressTracker.writeShardProgress(shardProgress);
  }

  private static void markShardFailure(
      ProcessingContext taskContext, SpannerDao spannerDao, String fileProcessedStartInterval) {
    markShardProgress(
        taskContext, Constants.SHARD_PROGRESS_STATUS_ERROR, spannerDao, fileProcessedStartInterval);
  }
}
