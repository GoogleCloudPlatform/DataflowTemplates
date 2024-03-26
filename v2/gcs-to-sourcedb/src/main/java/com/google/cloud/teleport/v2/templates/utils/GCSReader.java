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
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
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
  private boolean queriedDataSeenTable;

  private static final Logger LOG = LoggerFactory.getLogger(GCSReader.class);

  public GCSReader(ProcessingContext taskContext, SpannerDao spannerDao) {

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
            spannerDao, taskContext.getShard().getLogicalShardId(), taskContext.getRunId());
    this.shardId = taskContext.getShard().getLogicalShardId();
    shouldRetryWhenFileNotFound = true;
    shouldFailWhenFileNotFound = false;
    queriedDataSeenTable = false;
  }

  public List<TrimmedShardedDataChangeRecord> getRecords() {
    /*
    Call TextIO - read the file into PCollection
    Get a JSON transform of the PCollection
    Sort the Collection on commitTs,serverTrxId and record sequence
     */
    List<TrimmedShardedDataChangeRecord> changeStreamList = new ArrayList<>();
    LOG.info("Reading from file, {}", fileName);
    try (InputStream stream =
        Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(fileName, false)))) {

      BufferedReader reader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
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
      throw new RuntimeException("Failed in processing the record ", ex);
    } catch (IOException e) {

      LOG.warn("File not found : " + fileName);
      if (shouldRetryWhenFileNotFound) {
        if (!queriedDataSeenTable) {
          return checkAndReturnIfFileExists();
        } else {
          /* We do not need to call checkAndReturnIfFileExists again as it was called already
          as this will lead to stack overflow when the time taken to write file to GCS is large.
          GCS writing can take arbitrarty time in unforeseen scenario like Dataflow worker restart.
          So we just try to read the file in the same function call until found.*/
          return waitTillFileCreatedAndReturn();
        }
      } else {
        if (shouldFailWhenFileNotFound) {
          Metrics.counter(GCSReader.class, "file_not_found_errors_" + shardId).inc();
          throw new RuntimeException("File  " + fileName + " expected but not found  : " + e);
        }
        /* The logic for writing to skipped file table can generate load on the metadata database
        when the first file from the reader template comes very late.
        In this case, a lot of file intervals will be skipped since no file exists.
        This causes DEADLINE_EXCEEDED and hence can negatively harm the progress of
        both the pipelines as the metadata database is shared.
        Hence the code to store the file intervals skipped is removed
        and only warnings are logged. Since it was only for audit purpose anyway.*/
        LOG.warn("File not found : " + fileName + " skipping the file");
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed in GcsReader ", e);
    }

    return changeStreamList;
  }

  /**
   * We reached here since we did not find the file in GCS for the given interval. This can happen
   * if: 1. There was no data written to Spanner for that interval hence file does not exist in GCS
   * 2. There is data, but file is yet to be written to GCS
   *
   * <p>So we check if the first pipeline that writes to GCS has progressed sufficiently. For this,
   * we check the shard_file_create_progress table until the created_upto value is greater than or
   * equal to the current window.
   *
   * <p>If the created_upto is equal to current window - then it's indication that file for current
   * window is written and should exist in GCS. So we lookup the file again and fail if the file is
   * not found.
   *
   * <p>If the created_upto is greater than current window, we need to know if there was any data in
   * Spanner for the window we are checking. For this we query the date_seen table. If data was seen
   * for the current window, then file should exist in GCS and we lookup the file indefinitely until
   * is it found. If, however, there was no data for the current window in data_seen, then it means
   * file for the current interval is not there in GCS. We just simply skip the file.
   */
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
            "No data in shard_file_create_progress for shard {}, will retry in 2 seconds", shardId);
        Thread.sleep(2000);
        firstPipelineProgress = shardFileCreationTracker.getShardFileCreationProgressTimestamp();
        Metrics.counter(GCSReader.class, "metadata_file_create_init_retry_" + shardId).inc();
      }

      // the Spanner to GCS job needs to catchup - wait and retry
      while (firstPipelineProgress.compareTo(currentEndTimestamp) < 0) {
        LOG.info(
            "Progress for shard {} in shard_file_create_progress is lagging {}, will retry in 2"
                + " seconds",
            shardId,
            firstPipelineProgress);
        Thread.sleep(2000);
        firstPipelineProgress = shardFileCreationTracker.getShardFileCreationProgressTimestamp();
        Metrics.counter(GCSReader.class, "metadata_file_create_lag_retry_" + shardId).inc();
      }

      // the Spanner to GCS job has progressed past the current interval end timestamp
      // search for file again, if it exists process, else skip the file not found
      // if the file is expected to be present - retry until found
      if (shardFileCreationTracker.doesDataExistForTimestamp(currentEndTimestamp)) {
        LOG.info("Data exists for shard {} and time end {} ", shardId, currentEndTimestamp);
        shouldRetryWhenFileNotFound =
            true; // can happen due to out of order writes or the write to GCS was very slow
        shouldFailWhenFileNotFound = true;

      } else {
        shouldRetryWhenFileNotFound = false;
        shouldFailWhenFileNotFound = false;
      }
      queriedDataSeenTable = true;
      return getRecords();

    } catch (Exception e) {
      throw new RuntimeException(
          " Cannot determine file creation progress for shard : " + shardId, e);
    }
  }

  private List<TrimmedShardedDataChangeRecord> waitTillFileCreatedAndReturn() {
    boolean found = false;
    List<TrimmedShardedDataChangeRecord> changeStreamList = new ArrayList<>();
    while (!found) {
      try (InputStream stream =
          Channels.newInputStream(
              FileSystems.open(FileSystems.matchNewResource(fileName, false)))) {

        BufferedReader reader =
            new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
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
        found = true;
      } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
        throw new RuntimeException("Failed in processing the record ", ex);
      } catch (IOException e) {
        LOG.warn("Waiting for file : " + fileName);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ex) {
          continue;
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed in GcsReader ", e);
      }
    }
    return changeStreamList;
  }
}
