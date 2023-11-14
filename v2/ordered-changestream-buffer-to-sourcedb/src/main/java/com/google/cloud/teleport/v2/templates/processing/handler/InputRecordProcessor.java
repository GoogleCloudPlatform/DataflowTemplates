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

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.processing.dml.DMLGenerator;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** For the list of input records, parses and commits them to source DB. */
public class InputRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(InputRecordProcessor.class);

  public static void processRecords(
      List<String> recordList,
      Schema schema,
      MySqlDao dao,
      String shardId,
      String sourceDbTimezoneOffset) {

    try {
      boolean capturedlagMetric = false;
      long replicationLag = 0L;
      Counter numRecProcessedMetric =
          Metrics.counter(shardId, "numberOfRecordsProcessed_" + shardId);
      Distribution lagMetric = Metrics.distribution(shardId, "replicationLagInSeconds_" + shardId);

      List<String> dmlBatch = new ArrayList<>();
      for (String s : recordList) {
        List<String> parsedValues = parseRecord(s);
        String tableName = parsedValues.get(0);
        String keysJsonStr = parsedValues.get(1);
        String newValuesJsonStr = parsedValues.get(2);
        String modType = parsedValues.get(3);
        String commitTs = parsedValues.get(4);

        JSONObject newValuesJson = new JSONObject(newValuesJsonStr);
        JSONObject keysJson = new JSONObject(keysJsonStr);

        String dmlStatement =
            DMLGenerator.getDMLStatement(
                modType, tableName, schema, newValuesJson, keysJson, sourceDbTimezoneOffset);
        if (!dmlStatement.isEmpty()) {
          dmlBatch.add(dmlStatement);
        }
        if (!capturedlagMetric) {
          /*
          The commit timestamp of the first record is chosen for lag calculation
          Since the last record may have commit timestamp which might repeat for the
          next iteration of messages */
          capturedlagMetric = true;
          Instant instTime = Instant.now();
          Instant commitTsInst = Timestamp.parseTimestamp(commitTs).toSqlTimestamp().toInstant();
          replicationLag = ChronoUnit.SECONDS.between(commitTsInst, instTime);
        }
      }

      Instant daoStartTime = Instant.now();
      dao.batchWrite(dmlBatch);
      Instant daoEndTime = Instant.now();
      LOG.info(
          "Shard "
              + shardId
              + ": Write to mysql for "
              + recordList.size()
              + " took : "
              + ChronoUnit.MILLIS.between(daoStartTime, daoEndTime)
              + " milliseconds ");

      numRecProcessedMetric.inc(recordList.size()); // update the number of records processed metric

      lagMetric.update(replicationLag); // update the lag metric

    } catch (Exception e) {
      throw new RuntimeException("Failed to process records: " + e.getMessage());
    }
  }

  public static List<String> parseRecord(String rec) {

    List<String> response = new ArrayList<>();
    try {
      JSONObject json = new JSONObject(rec);

      String tableName = json.getString("tableName");
      response.add(tableName);
      String keyValuesJson = json.getJSONArray("mods").getJSONObject(0).getString("keysJson");
      response.add(keyValuesJson);
      String newValuesJson = json.getJSONArray("mods").getJSONObject(0).getString("newValuesJson");
      response.add(newValuesJson);
      String modType = json.getString("modType");
      response.add(modType);
      long secs = json.getJSONObject("commitTimestamp").getBigDecimal("seconds").longValue();

      int nanosecs = json.getJSONObject("commitTimestamp").getBigDecimal("nanos").intValue();
      response.add(Timestamp.ofTimeSecondsAndNanos(secs, nanosecs).toString());
      return response;
    } catch (Exception e) {
      throw new RuntimeException("Input record is not as expected: " + rec);
    }
  }
}
