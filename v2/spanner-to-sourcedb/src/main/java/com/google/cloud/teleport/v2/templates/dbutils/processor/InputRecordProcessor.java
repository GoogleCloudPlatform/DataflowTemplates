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

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_CASSANDRA;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SpannerReadUtils;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
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
  private static final java.time.Duration LOOKBACK_DURATION_FOR_DELETE =
      java.time.Duration.ofNanos(1000);

  public static void updateChangeEnventToIncludeGeneratedColumns(
      TrimmedShardedDataChangeRecord spannerRecord,
      Key primaryKey,
      ISchemaMapper schemaMapper,
      Ddl ddl,
      SourceSchema sourceSchema,
      SpannerDao spannerDao,
      SpannerConfig spannerConfig,
      ObjectMapper objectMapper)
      throws Exception {

    String tableName = spannerRecord.getTableName();
    // Check for generated columns in Spanner that are not generated in Source
    // and fetch them if missing.
    List<String> spannerCols = schemaMapper.getSpannerColumns(null, tableName);
    List<String> columnsToFetch = new ArrayList<>();
    String sourceTableName = schemaMapper.getSourceTableName(null, tableName);
    SourceTable sourceTable = sourceSchema.table(sourceTableName);
    Set<String> pkColumns =
        ddl.table(tableName).primaryKeys().stream()
            .map(IndexColumn::name)
            .collect(Collectors.toSet());

    for (String col : spannerCols) {
      boolean isGeneratedInSpanner = schemaMapper.isGeneratedColumn(null, tableName, col);
      boolean existsAtSource = schemaMapper.colExistsAtSource(null, tableName, col);
      boolean isPk = pkColumns.contains(col);
      if (isGeneratedInSpanner && existsAtSource && !isPk) {
        String sourceColName = schemaMapper.getSourceColumnName(null, tableName, col);
        SourceColumn sourceColumn = sourceTable.column(sourceColName);
        // If source column is NOT generated, we need the value from Spanner
        if (sourceColumn != null && !sourceColumn.isGenerated()) {
          columnsToFetch.add(col);
        }
      }
    }

    LOG.error("Columns to fetch: " + columnsToFetch + " for table: " + tableName);
    if (!columnsToFetch.isEmpty()) {
      com.google.cloud.Timestamp commitTimestamp = spannerRecord.getCommitTimestamp();

      java.time.Instant commitInstant =
          java.time.Instant.ofEpochSecond(commitTimestamp.getSeconds(), commitTimestamp.getNanos());

      java.time.Instant staleInstant = commitInstant.plus(LOOKBACK_DURATION_FOR_DELETE);

      com.google.cloud.Timestamp staleReadTs =
          com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
              staleInstant.getEpochSecond(), staleInstant.getNano());
      LOG.error(
          "Stale read timestamp: "
              + staleReadTs
              + " for table: "
              + tableName
              + " and primary key: "
              + primaryKey
              + " and commit timestamp: "
              + commitTimestamp);
      LOG.error(
          "Spanner Config: "
              + spannerConfig
              + " Database Id: "
              + spannerConfig.getDatabaseId()
              + " Instance Id: "
              + spannerConfig.getInstanceId()
              + " Project Id: "
              + spannerConfig.getProjectId());
      Struct fetchedRow =
          SpannerReadUtils.readRowAsStruct(
              spannerDao.getDatabaseClient(),
              tableName,
              primaryKey,
              columnsToFetch,
              commitTimestamp,
              spannerConfig.getRpcPriority().get());
      if (fetchedRow == null) {
        LOG.warn("Failed to fetch row for primary key: " + primaryKey);
      } else {
        Map<String, Object> rowAsMap =
            SpannerReadUtils.getRowAsMap(fetchedRow, columnsToFetch, tableName, ddl);
        SpannerReadUtils.updateColumnValues(
            spannerRecord, sourceTableName, ddl, fetchedRow, rowAsMap, objectMapper);
      }
    }
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

    String tableName = spannerRecord.getTableName();
    try {

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
              .setSchemaMapper(schemaMapper)
              .setCustomTransformationResponse(customTransformationResponse)
              .setCommitTimestamp(spannerRecord.getCommitTimestamp())
              .setDdl(ddl)
              .setSourceSchema(sourceSchema)
              .build();

      DMLGeneratorResponse dmlGeneratorResponse = dmlGenerator.getDMLStatement(dmlGeneratorRequest);
      if (dmlGeneratorResponse.getDmlStatement().isEmpty()) {
        throw new InvalidDMLGenerationException("DML statement is empty for table: " + tableName);
      }
      // TODO we need to handle it as proper Interface Level as of now we have handle Prepared
      // TODO Statement and Raw Statement Differently
      /*
       * TODO:
       * Note: The `SOURCE_CASSANDRA` case not covered in the unit tests.
       * Answer: Currently, we have implemented unit tests for the Input Record Processor under the SourceWrittenFn.
       *         These tests cover the majority of scenarios, but they are tightly coupled with the existing code.
       *         Adding unit tests for SOURCE_CASSANDRA would require a significant refactoring of the entire unit test file.
       *         Given the current implementation, such refactoring is deemed unnecessary as it would not provide substantial value or impact.
       */
      LOG.error(
          "DML Table Name: "
              + tableName
              + " DML Generator Response: "
              + dmlGeneratorResponse.getDmlStatement());
      switch (source) {
        case SOURCE_CASSANDRA:
          dao.write(dmlGeneratorResponse, null);
          break;
        default:
          dao.write(dmlGeneratorResponse.getDmlStatement(), check);
          break;
      }

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
      LOG.error("Error processing input record for Table: " + tableName, e);
      // Not logging the error here since the error can be retryable error and high number of them
      // could have side effects on the pipeline execution.
      throw e; // throw the original exception since it needs to go to DLQ
    }
  }
}
