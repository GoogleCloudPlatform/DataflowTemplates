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

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dao.MySqlDao;
import com.google.cloud.teleport.v2.templates.processing.dml.DMLGenerator;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** For the list of input records, parses and commits them to source DB. */
public class InputRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(InputRecordProcessor.class);

  private static List<TrimmedShardedDataChangeRecord> filteredEvents;
  private static final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          InputRecordProcessor.class, "apply_custom_transformation_impl_latency_ms");

  public static List<TrimmedShardedDataChangeRecord> getFilteredEvents() {
    return filteredEvents;
  }

  public static void setFilteredEvents(List<TrimmedShardedDataChangeRecord> filteredEvents) {
    InputRecordProcessor.filteredEvents = filteredEvents;
  }

  public static void processRecords(
      List<TrimmedShardedDataChangeRecord> recordList,
      Schema schema,
      MySqlDao dao,
      String shardId,
      String sourceDbTimezoneOffset,
      ISpannerMigrationTransformer spannerToSourceTransformer) {

    try {
      boolean capturedlagMetric = false;
      long replicationLag = 0L;
      Counter numRecProcessedMetric = Metrics.counter(shardId, "records_processed_" + shardId);
      Counter numRecReadFromGcsMetric =
          Metrics.counter(shardId, "records_read_from_gcs_" + shardId);
      // gives indication that records were read from GCS
      numRecReadFromGcsMetric.inc(recordList.size());
      Distribution lagMetric =
          Metrics.distribution(shardId, "replication_lag_in_seconds_" + shardId);
      filteredEvents = new ArrayList<>();
      List<String> dmlBatch = new ArrayList<>();
      for (TrimmedShardedDataChangeRecord chrec : recordList) {
        String tableName = chrec.getTableName();
        String modType = chrec.getModType().name();
        String keysJsonStr = chrec.getMods().get(0).getKeysJson();
        String newValueJsonStr = chrec.getMods().get(0).getNewValuesJson();
        JSONObject newValuesJson = new JSONObject(newValueJsonStr);
        JSONObject keysJson = new JSONObject(keysJsonStr);
        Map<String, Object> customTransformationResponse = null;
        if (spannerToSourceTransformer != null) {
          org.joda.time.Instant startTimestamp = org.joda.time.Instant.now();
          Map<String, Object> mapRequest =
              ChangeEventToMapConvertor.combineJsonObjects(keysJson, newValuesJson);
          MigrationTransformationRequest migrationTransformationRequest =
              new MigrationTransformationRequest(tableName, mapRequest, shardId, modType);
          MigrationTransformationResponse migrationTransformationResponse = null;
          try {
            migrationTransformationResponse =
                spannerToSourceTransformer.toSourceRow(migrationTransformationRequest);
          } catch (Exception e) {
            throw new InvalidTransformationException(e);
          }
          org.joda.time.Instant endTimestamp = org.joda.time.Instant.now();
          applyCustomTransformationResponseTimeMetric.update(
              new Duration(startTimestamp, endTimestamp).getMillis());
          if (migrationTransformationResponse.isEventFiltered()) {
            filteredEvents.add(chrec);
            Metrics.counter(InputRecordProcessor.class, "filtered_events_" + shardId).inc();
            continue;
          }
          if (migrationTransformationResponse != null) {
            customTransformationResponse = migrationTransformationResponse.getResponseRow();
          }
        }
        String dmlStatement =
            DMLGenerator.getDMLStatement(
                modType,
                tableName,
                schema,
                newValuesJson,
                keysJson,
                sourceDbTimezoneOffset,
                customTransformationResponse);
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
          Instant commitTsInst = chrec.getCommitTimestamp().toSqlTimestamp().toInstant();
          replicationLag = ChronoUnit.SECONDS.between(commitTsInst, instTime);
        }
      }
      setFilteredEvents(filteredEvents);

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

    } catch (InvalidTransformationException e) {
      Metrics.counter(InputRecordProcessor.class, "custom_transformation_exception").inc();
      LOG.error(
          "Invalid transformation exception occurred while processing shardId: {} is {} ",
          shardId,
          ExceptionUtils.getStackTrace(e));
      throw new RuntimeException("Failed to process records: ", e);
    } catch (Exception e) {
      LOG.error(
          "The exception while processing shardId: {} is {} ",
          shardId,
          ExceptionUtils.getStackTrace(e));
      throw new RuntimeException("Failed to process records: ", e);
    }
  }
}
