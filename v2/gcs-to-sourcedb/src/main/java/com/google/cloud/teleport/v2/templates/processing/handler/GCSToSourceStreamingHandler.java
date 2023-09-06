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
import com.google.cloud.teleport.v2.templates.dao.DaoFactory;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.utils.GCSReader;
import com.google.cloud.teleport.v2.templates.utils.ShardProgressTracker;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads from GCS orders and writes to source. */
public class GCSToSourceStreamingHandler {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceStreamingHandler.class);

  public static void process(ProcessingContext taskContext) {
    String shardId = taskContext.getShard().getLogicalShardId();
    GCSReader inputFileReader = new GCSReader(taskContext);

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
      if (records.isEmpty()) {
        markShardSuccess(taskContext);
        return;
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
      markShardSuccess(taskContext);
      dao.cleanup();
      LOG.info(
          "Shard " + shardId + ": Successfully processed batch of " + records.size() + " records.");
    } catch (Exception e) {
      // TODO: Error handling and retry
      /*
      If we are here, it means we have exhausted all the retries
      At this stage we dump the error records to error topic
      and either halt the pipeline  or continue
      as per the configuration
      If writing to DLQ topic also fails - write to logs
      */
      markShardFailure(taskContext);
      throw new RuntimeException("Failure when processing records: " + e.getMessage());
    }
  }

  private static void markShardSuccess(ProcessingContext taskContext) {
    markShardProgress(taskContext, "SUCCESS");
  }

  private static void markShardProgress(ProcessingContext taskContext, String status) {
    ShardProgressTracker shardProgressTracker =
        new ShardProgressTracker(
            taskContext.getSpannerProjectId(),
            taskContext.getMetadataInstance(),
            taskContext.getMetadataDatabase());
    String fileStartTime = taskContext.getStartTimestamp();
    com.google.cloud.Timestamp startTs = com.google.cloud.Timestamp.parseTimestamp(fileStartTime);

    ShardProgress shardProgress =
        new ShardProgress(taskContext.getShard().getLogicalShardId(), startTs, status);

    shardProgressTracker.writeShardProgress(shardProgress);

    shardProgressTracker.close();
  }

  private static void markShardFailure(ProcessingContext taskContext) {
    markShardProgress(taskContext, "ERROR");
  }
}
