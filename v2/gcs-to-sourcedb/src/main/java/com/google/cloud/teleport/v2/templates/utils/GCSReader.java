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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.Metrics;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reader for GCS. */
public class GCSReader {

  private String fileName;
  private ShardFileCreationTracker shardFileCreationTracker;
  private Instant currentIntervalEnd;
  private String shardId;
  private boolean shouldRetryWhenFileNotFound;
  private boolean shouldFailWhenFileNotFound;

  private static final Logger LOG = LoggerFactory.getLogger(GCSReader.class);

  public GCSReader(ProcessingContext taskContext) {

    String fileStartTime = taskContext.getStartTimestamp();
    com.google.cloud.Timestamp startTs = com.google.cloud.Timestamp.parseTimestamp(fileStartTime);
    Instant startInst = new Instant(startTs.toSqlTimestamp());
    currentIntervalEnd = startInst.plus(taskContext.getWindowDuration());
    String gcsFileName =
        taskContext.getGCSPath()
            + "/"
            + taskContext.getShard().getLogicalShardId()
            + "/"
            + startInst
            + "-"
            + currentIntervalEnd
            + "-pane-0-last-0-of-1.txt";

    this.fileName = gcsFileName;
    this.shardFileCreationTracker =
        new ShardFileCreationTracker(
            taskContext.getSpannerProjectId(),
            taskContext.getMetadataInstance(),
            taskContext.getMetadataDatabase(),
            taskContext.getShard().getLogicalShardId(),
            taskContext.getTableSuffix());
    this.shardId = taskContext.getShard().getLogicalShardId();
    shouldRetryWhenFileNotFound = true;
    shouldFailWhenFileNotFound = false;
  }

  public List<TrimmedShardedDataChangeRecord> getRecords() {
    /*
    Call TextIO - read the file into PCollection
    Get a JSON transfrom of the PCollection
    Sort the Collection on commitTs,serverTrxId and record sequence
     */
    List<TrimmedShardedDataChangeRecord> changeStreamList = new ArrayList<>();
    LOG.info("Reading from file, {}", fileName);
    try (InputStream stream =
        Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(fileName, false)))) {

      BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      ObjectWriter ow = new ObjectMapper().writer();
      while (reader.ready()) {
        String line = reader.readLine();
        TrimmedShardedDataChangeRecord chrec =
            new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
                .create()
                .fromJson(line, TrimmedShardedDataChangeRecord.class);

        changeStreamList.add(chrec);
      }

      Collections.sort(
          changeStreamList,
          Comparator.comparing(TrimmedShardedDataChangeRecord::getCommitTimestamp)
              .thenComparing(TrimmedShardedDataChangeRecord::getServerTransactionId)
              .thenComparing(TrimmedShardedDataChangeRecord::getRecordSequence));

      Metrics.counter(shardId, "file_read_" + shardId).inc();

    } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
      throw new RuntimeException("Failed in processing the record : " + ex);
    } catch (IOException e) {

      LOG.warn("File not found : " + fileName);
      if (shouldRetryWhenFileNotFound) {
        return checkAndReturnIfFileExists();
      } else {
        if (shouldFailWhenFileNotFound) {
          Metrics.counter(GCSReader.class, "file_not_found_errors_" + shardId).inc();
          throw new RuntimeException("File  " + fileName + " expected but not found  : " + e);
        }
        LOG.warn("File not found : " + fileName + " skipping the file");
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed in GcsReader : " + e);
    }

    return changeStreamList;
  }

  private List<TrimmedShardedDataChangeRecord> checkAndReturnIfFileExists() {
    try {
      Timestamp firstPipelineProgress =
          shardFileCreationTracker.getShardFileCreationProgressTimestamp();
      Timestamp currentEndTimestamp = Timestamp.parseTimestamp(currentIntervalEnd.toString());

      /*
      This can be null in case the table is not yet initialized, just retry indefinitely.
      No one's fault here.*/
      while (firstPipelineProgress == null) {
        LOG.info(
            "No data in shard_file_create_progress for shard {}, will retry in 5 seconds", shardId);
        Thread.sleep(5000);
        firstPipelineProgress = shardFileCreationTracker.getShardFileCreationProgressTimestamp();
        Metrics.counter(GCSReader.class, "metadata_file_create_init_retry_" + shardId).inc();
      }

      // the Spanner to GCS job needs to catchup - wait and retry
      while (firstPipelineProgress.compareTo(currentEndTimestamp) < 0) {
        LOG.info(
            "Progress for shard {} in shard_file_create_progress is lagging {}, will retry in 5"
                + " seconds",
            shardId,
            firstPipelineProgress);
        Thread.sleep(5000);
        firstPipelineProgress = shardFileCreationTracker.getShardFileCreationProgressTimestamp();
        Metrics.counter(GCSReader.class, "metadata_file_create_lag_retry_" + shardId).inc();
      }

      shardFileCreationTracker.close();

      if (firstPipelineProgress.compareTo(currentEndTimestamp) > 0) {
        // the Spanner to GCS job has progressed past the current interval end timestamp
        // search for file again, if it exists process, else skip the file not found
        shouldRetryWhenFileNotFound = false;
        shouldFailWhenFileNotFound = false;
        return getRecords();
      } else {
        // the progress matches exactly,file should exist, if it is not found return error
        shouldRetryWhenFileNotFound = false;
        shouldFailWhenFileNotFound = true;
        return getRecords();
      }
    } catch (Exception e) {
      throw new RuntimeException(" Cannot determine file creation progress for shard : " + shardId);
    }
  }
}
