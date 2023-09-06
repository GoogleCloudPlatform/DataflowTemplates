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
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reader for GCS. */
public class GCSReader {

  private String fileName;

  private static final Logger LOG = LoggerFactory.getLogger(GCSReader.class);

  public GCSReader(ProcessingContext taskContext) {

    String fileStartTime = taskContext.getStartTimestamp();
    com.google.cloud.Timestamp startTs = com.google.cloud.Timestamp.parseTimestamp(fileStartTime);
    Instant startInst = new Instant(startTs.toSqlTimestamp());
    Instant endInst = startInst.plus(taskContext.getWindowDuration());
    String gcsFileName =
        taskContext.getGCSPath()
            + "/"
            + taskContext.getShard().getLogicalShardId()
            + "/"
            + startInst
            + "-"
            + endInst
            + "-pane-0-last-0-of-1.txt";

    this.fileName = gcsFileName;
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

      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      ObjectWriter ow = new ObjectMapper().writer();
      while (reader.ready()) {
        String line = reader.readLine();
        LOG.info("Read line , {}", line);
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

    } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
      throw new RuntimeException("Failed in processing the record : " + ex);
    } catch (IOException e) {
      // TODO: add logic to retry or check the progress of the spanner to gcs pipeline
      LOG.warn("File not found : " + fileName);
    } catch (Exception e) {
      throw new RuntimeException("Failed in GcsReader : " + e);
    }

    return changeStreamList;
  }
}
