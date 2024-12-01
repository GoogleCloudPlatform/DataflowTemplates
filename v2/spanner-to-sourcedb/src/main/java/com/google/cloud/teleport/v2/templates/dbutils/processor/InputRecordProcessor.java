/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
  private static final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          InputRecordProcessor.class, "apply_custom_transformation_impl_latency_ms");

  public static boolean processRecord(
      TrimmedShardedDataChangeRecord spannerRecord,
      Schema schema,
      IDao dao,
      String shardId,
      String sourceDbTimezoneOffset,
      IDMLGenerator dmlGenerator,
      ISpannerMigrationTransformer spannerToSourceTransformer)
      throws Exception {

    try {

      String tableName = spannerRecord.getTableName();
      String modType = spannerRecord.getModType().name();
      String keysJsonStr = spannerRecord.getMod().getKeysJson();
      String newValueJsonStr = spannerRecord.getMod().getNewValuesJson();
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
          Metrics.counter(InputRecordProcessor.class, "filtered_events_" + shardId).inc();
          return true;
        }
        if (migrationTransformationResponse != null) {
          customTransformationResponse = migrationTransformationResponse.getResponseRow();
        }
      }
      DMLGeneratorRequest dmlGeneratorRequest =
          new DMLGeneratorRequest.Builder(
                  modType, tableName, newValuesJson, keysJson, sourceDbTimezoneOffset)
              .setSchema(schema)
              .setCustomTransformationResponse(customTransformationResponse)
              .build();

      DMLGeneratorResponse dmlGeneratorResponse = dmlGenerator.getDMLStatement(dmlGeneratorRequest);
      if (dmlGeneratorResponse.getDmlStatement().isEmpty()) {
        LOG.warn("DML statement is empty for table: " + tableName);
        return false;
      }
      dao.write(dmlGeneratorResponse.getDmlStatement());

      Counter numRecProcessedMetric =
          Metrics.counter(shardId, "records_written_to_source_" + shardId);

      numRecProcessedMetric.inc(1); // update the number of records processed metric
      Distribution lagMetric =
          Metrics.distribution(shardId, "replication_lag_in_seconds_" + shardId);

      Instant instTime = Instant.now();
      Instant commitTsInst = spannerRecord.getCommitTimestamp().toSqlTimestamp().toInstant();
      long replicationLag = ChronoUnit.SECONDS.between(commitTsInst, instTime);

      lagMetric.update(replicationLag); // update the lag metric
      return false;
    } catch (Exception e) {
      LOG.error(
          "The exception while processing shardId: {} is {} ",
          shardId,
          ExceptionUtils.getStackTrace(e));
      throw e; // throw the original exception since it needs to go to DLQ
    }
  }
}
