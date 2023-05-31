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
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.processing.dml.DMLGenerator;
import com.google.cloud.teleport.v2.templates.schema.Schema;
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

        String dmlStatment =
            DMLGenerator.getDMLStatement(
                modType, tableName, schema, newValuesJson, keysJson, sourceDbTimezoneOffset);
        if (!dmlStatment.isEmpty()) {
          dmlBatch.add(dmlStatment);
        }
        if (!capturedlagMetric) {
          /*
          The commit timstamp of the first record is chosen for lag calculation
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
          "Write to mysql for "
              + recordList.size()
              + " took : "
              + ChronoUnit.MILLIS.between(daoStartTime, daoEndTime)
              + " miliseconds ");

      numRecProcessedMetric.inc(recordList.size()); // update the number of records processed metric

      lagMetric.update(replicationLag); // update the lag metric

    } catch (Exception e) {
      throw new RuntimeException("Failure when processing records: " + e.getMessage());
    }
  }

  private static List<String> parseRecord(String rec) {

    List<String> response = new ArrayList<>();

    String tablesub = rec.substring(rec.indexOf("tableName=") + 10);

    String tablestr = tablesub.substring(1, tablesub.indexOf(",") - 1);
    response.add(tablestr);
    String keysub = tablesub.substring(tablesub.indexOf("keysJson=") + 9);

    String keystr = keysub.substring(0, keysub.indexOf(", oldValuesJson"));
    response.add(keystr);
    String newValueSub = keysub.substring(keysub.indexOf("newValuesJson=") + 15);

    String newValuesJsonStr = newValueSub.substring(0, newValueSub.indexOf("'}], modType"));
    response.add(newValuesJsonStr);
    String modvaluesub = newValueSub.substring(newValueSub.indexOf("'}], modType=") + 13);

    String modvaluestr =
        modvaluesub.substring(0, modvaluesub.indexOf(", numberOfRecordsInTransaction="));

    response.add(modvaluestr.trim());

    String commitTsSub = rec.substring(rec.indexOf("commitTimestamp=") + 16);

    String commitTs = commitTsSub.substring(0, commitTsSub.indexOf("serverTransactionId=") - 2);
    response.add(commitTs);

    return response;
  }
}
