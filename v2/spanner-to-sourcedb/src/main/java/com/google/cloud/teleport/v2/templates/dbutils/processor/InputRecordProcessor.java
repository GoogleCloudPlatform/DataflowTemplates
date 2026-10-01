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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import java.util.Map;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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

  /**
   * Generates the {@link DMLGeneratorResponse} for the given record without writing to any DAO.
   * Returns {@code null} if the event is filtered by a custom transformation.
   *
   * <p>Use this when the write must be deferred to a point outside an enclosing transaction (e.g.
   * to avoid nested Spanner transactions for the {@code SOURCE_SPANNER} path).
   *
   * @throws InvalidDMLGenerationException if the generated DML is empty or cannot be generated.
   * @throws Exception for other processing errors.
   */
  public static DMLGeneratorResponse generateDMLResponse(
      TrimmedShardedDataChangeRecord spannerRecord,
      ISchemaMapper schemaMapper,
      Ddl ddl,
      SourceSchema sourceSchema,
      String shardId,
      String sourceDbTimezoneOffset,
      IDMLGenerator dmlGenerator,
      ISpannerMigrationTransformer spannerToSourceTransformer,
      String source)
      throws Exception {

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
        return null;
      }
      if (migrationTransformationResponse != null) {
        customTransformationResponse = migrationTransformationResponse.getResponseRow();
      }
    }

    DMLGeneratorRequest dmlGeneratorRequest =
        new DMLGeneratorRequest.Builder(
                modType, tableName, newValuesJson, keysJson, sourceDbTimezoneOffset)
            .setSchemaMapper(schemaMapper)
            .setCustomTransformationResponse(customTransformationResponse)
            .setCommitTimestamp(spannerRecord.getCommitTimestamp())
            .setDdl(ddl)
            .setSourceSchema(sourceSchema)
            .build();

    DMLGeneratorResponse dmlGeneratorResponse = dmlGenerator.getDMLStatement(dmlGeneratorRequest);

    if (dmlGeneratorResponse.isEmpty()) {
      throw new InvalidDMLGenerationException("DML statement is empty for table: " + tableName);
    }

    return dmlGeneratorResponse;
  }

  public static boolean processRecord(
      TrimmedShardedDataChangeRecord spannerRecord,
      ISchemaMapper schemaMapper,
      Ddl ddl,
      SourceSchema sourceSchema,
      IDao dao,
      String shardId,
      String sourceDbTimezoneOffset,
      IDMLGenerator dmlGenerator,
      ISpannerMigrationTransformer spannerToSourceTransformer,
      String source,
      TransactionalCheck check)
      throws Exception {

    try {
      DMLGeneratorResponse dmlGeneratorResponse =
          generateDMLResponse(
              spannerRecord,
              schemaMapper,
              ddl,
              sourceSchema,
              shardId,
              sourceDbTimezoneOffset,
              dmlGenerator,
              spannerToSourceTransformer,
              source);

      if (dmlGeneratorResponse == null) {
        return true; // filtered
      }

      dao.write(dmlGeneratorResponse, check);
      return false;
    } catch (Exception e) {
      // Not logging the error here since the error can be retryable error and high number of them
      // could have side effects on the pipeline execution.
      throw e; // throw the original exception since it needs to go to DLQ
    }
  }
}
