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
package com.google.cloud.teleport.v2.templates.transforms;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheckException;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.dbutils.processor.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessor;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessorFactory;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.io.Serializable;
import java.sql.SQLNonTransientConnectionException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class writes to source based on commit timestamp captured in shadow table. */
public class SourceWriterFn extends DoFn<KV<Long, TrimmedShardedDataChangeRecord>, String>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SourceWriterFn.class);
  private static Gson gson = new Gson();

  private transient ObjectMapper mapper;

  private final Counter successRecordCountMetric =
      Metrics.counter(SourceWriterFn.class, "success_record_count");

  private final Counter retryableRecordCountMetric =
      Metrics.counter(SourceWriterFn.class, "retryable_record_count");

  private final Counter skippedRecordCountMetric =
      Metrics.counter(SourceWriterFn.class, "skipped_record_count");

  private final Distribution lagMetric =
      Metrics.distribution(SourceWriterFn.class, "replication_lag_in_milli");

  private final Counter invalidTransformationException =
      Metrics.counter(SourceWriterFn.class, "custom_transformation_exception");

  private final SpannerToSourceDb.Options options;
  private transient ISchemaMapper schemaMapper;
  private final String sourceDbTimezoneOffset;
  private final List<Shard> shards;
  private final SpannerConfig spannerConfig;
  private transient SpannerDao spannerDao;
  private final SourceSchema sourceSchema;
  private final String shadowTablePrefix;
  private final String skipDirName;
  private final int maxThreadPerDataflowWorker;
  private final String source;
  private SourceProcessor sourceProcessor;
  private final CustomTransformation customTransformation;
  private ISpannerMigrationTransformer spannerToSourceTransformer;

  private final PCollectionView<Ddl> ddlView;
  private final PCollectionView<Ddl> shadowTableDdlView;

  public SourceWriterFn(
      List<Shard> shards,
      SpannerToSourceDb.Options options,
      SpannerConfig spannerConfig,
      String sourceDbTimezoneOffset,
      SourceSchema sourceSchema,
      String shadowTablePrefix,
      String skipDirName,
      int maxThreadPerDataflowWorker,
      String source,
      CustomTransformation customTransformation,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView) {

    this.options = options;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.shards = shards;
    this.spannerConfig = spannerConfig;
    this.sourceSchema = sourceSchema;
    this.shadowTablePrefix = shadowTablePrefix;
    this.skipDirName = skipDirName;
    this.maxThreadPerDataflowWorker = maxThreadPerDataflowWorker;
    this.source = source;
    this.customTransformation = customTransformation;
    this.ddlView = ddlView;
    this.shadowTableDdlView = shadowTableDdlView;
  }

  // for unit testing purposes
  public void setSpannerDao(SpannerDao spannerDao) {
    this.spannerDao = spannerDao;
  }

  // for unit testing purposes
  public void setObjectMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  // for unit testing purposes
  public void setSourceProcessor(SourceProcessor sourceProcessor) {
    this.sourceProcessor = sourceProcessor;
  }

  // for unit testing purposes
  public void setSpannerToSourceTransformer(
      ISpannerMigrationTransformer spannerToSourceTransformer) {
    this.spannerToSourceTransformer = spannerToSourceTransformer;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() throws UnsupportedSourceException {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceProcessor =
        SourceProcessorFactory.createSourceProcessor(source, shards, maxThreadPerDataflowWorker);
    spannerDao = new SpannerDao(spannerConfig);
    spannerToSourceTransformer =
        CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() throws Exception {
    if (spannerDao != null) {
      spannerDao.close();
    }
    if (sourceProcessor != null) {
      sourceProcessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    // Access DDLs from side inputs
    Ddl ddl = c.sideInput(ddlView);
    Ddl shadowTableDdl = c.sideInput(shadowTableDdlView);

    // Lazy initialization of schemaMapper, which depends on DDL
    if (this.schemaMapper == null) {
      this.schemaMapper = SpannerToSourceDb.getSchemaMapper(this.options, ddl);
    }

    KV<Long, TrimmedShardedDataChangeRecord> element = c.element();
    TrimmedShardedDataChangeRecord spannerRec = element.getValue();
    String shardId = spannerRec.getShard();
    if (shardId == null) {
      // no shard found, move to permanent error
      outputWithTag(
          c, Constants.PERMANENT_ERROR_TAG, Constants.SHARD_NOT_PRESENT_ERROR_MESSAGE, spannerRec);
    } else if (shardId.equals(skipDirName)) {
      // the record is skipped
      skippedRecordCountMetric.inc();
      outputWithTag(c, Constants.SKIPPED_TAG, Constants.SKIPPED_TAG_MESSAGE, spannerRec);
    } else {
      // Get the latest commit timestamp processed at source
      try {
        JsonNode keysJson = mapper.readTree(spannerRec.getMod().getKeysJson());
        String tableName = spannerRec.getTableName();
        com.google.cloud.spanner.Key primaryKey =
            ChangeEventSpannerConvertor.changeEventToPrimaryKey(
                tableName, ddl, keysJson, /* convertNameToLowerCase= */ false);
        String shadowTableName = shadowTablePrefix + tableName;
        spannerDao
            .getDatabaseClient()
            .readWriteTransaction(Options.priority(spannerConfig.getRpcPriority().get()))
            .run(
                (TransactionRunner.TransactionCallable<Void>)
                    shadowTransaction -> {
                      boolean isSourceAhead = false;
                      ShadowTableRecord shadowTableRecord =
                          spannerDao.readShadowTableRecordWithExclusiveLock(
                              shadowTableName, primaryKey, shadowTableDdl, shadowTransaction);
                      isSourceAhead =
                          shadowTableRecord != null
                              && ((shadowTableRecord
                              .getProcessedCommitTimestamp()
                              .compareTo(spannerRec.getCommitTimestamp())
                              > 0) // either the source already has record with greater
                              // commit
                              // timestamp
                              || (shadowTableRecord // or the source has the same commit
                              // timestamp but
                              // greater record sequence
                              .getProcessedCommitTimestamp()
                              .compareTo(spannerRec.getCommitTimestamp())
                              == 0
                              && shadowTableRecord.getRecordSequence()
                              > Long.parseLong(spannerRec.getRecordSequence())));

                      if (!isSourceAhead) {
                        IDao sourceDao = sourceProcessor.getSourceDao(shardId);
                        TransactionalCheck check =
                            () -> {
                              ShadowTableRecord newShadowTableRecord =
                                  spannerDao.readShadowTableRecordWithExclusiveLock(
                                      shadowTableName,
                                      primaryKey,
                                      shadowTableDdl,
                                      shadowTransaction);
                              if (!ShadowTableRecord.isEquals(
                                  shadowTableRecord, newShadowTableRecord)) {
                                throw new TransactionalCheckException(
                                    "Shadow table sequence changed during transaction");
                              }
                            };
                        boolean isEventFiltered =
                            InputRecordProcessor.processRecord(
                                spannerRec,
                                schemaMapper,
                                ddl, // Use local ddl
                                sourceSchema,
                                sourceDao,
                                shardId,
                                sourceDbTimezoneOffset,
                                sourceProcessor.getDmlGenerator(),
                                spannerToSourceTransformer,
                                this.source,
                                check);
                        if (isEventFiltered) {
                          outputWithTag(
                              c,
                              Constants.FILTERED_TAG,
                              Constants.FILTERED_TAG_MESSAGE,
                              spannerRec);
                        }

                        spannerDao.updateShadowTable(
                            getShadowTableMutation(
                                tableName,
                                shadowTableName,
                                keysJson,
                                spannerRec.getCommitTimestamp(),
                                spannerRec.getRecordSequence(),
                                ddl), // Pass local ddl
                            shadowTransaction);
                      }
                      return null;
                    });
        successRecordCountMetric.inc();
        if (spannerRec.isRetryRecord()) {
          retryableRecordCountMetric.dec();
        }
        com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
        c.output(Constants.SUCCESS_TAG, timestamp.toString());
        // Since we have wrapped the logic inside Spanner transaction, the exceptions would also be
        // wrapped inside a SpannerException.
        // We need to get and inspect the cause while handling the exception.
      } catch (SpannerException ex) {
        Throwable cause = ex.getCause();
        if (cause == null) {
          // If cause is null, then it is a plain Spanner exception
          outputWithTag(c, Constants.RETRYABLE_ERROR_TAG, ex.getMessage(), spannerRec);
        } else if (cause instanceof InvalidTransformationException) {
          invalidTransformationException.inc();
          outputWithTag(c, Constants.PERMANENT_ERROR_TAG, cause.getMessage(), spannerRec);
        } else if (cause instanceof ChangeEventConvertorException
            || cause instanceof CodecNotFoundException) {
          outputWithTag(c, Constants.PERMANENT_ERROR_TAG, cause.getMessage(), spannerRec);
        } else if (cause instanceof IllegalStateException
            || cause instanceof com.mysql.cj.jdbc.exceptions.CommunicationsException
            || cause instanceof java.sql.SQLIntegrityConstraintViolationException
            || cause instanceof java.sql.SQLTransientConnectionException
            || cause instanceof ConnectionInitException
            || cause instanceof DriverTimeoutException
            || cause instanceof AllNodesFailedException
            || cause instanceof BusyConnectionException
            || cause instanceof NodeUnavailableException
            || cause instanceof QueryExecutionException
            || cause instanceof ConnectionException) {
          outputWithTag(c, Constants.RETRYABLE_ERROR_TAG, cause.getMessage(), spannerRec);
        } else if (cause instanceof java.sql.SQLNonTransientConnectionException) {
          SQLNonTransientConnectionException e = (SQLNonTransientConnectionException) cause;
          // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
          // error codes 1053,1161 and 1159 can be retried
          if (e.getErrorCode() == 1053 || e.getErrorCode() == 1159 || e.getErrorCode() == 1161) {
            outputWithTag(c, Constants.RETRYABLE_ERROR_TAG, e.getMessage(), spannerRec);
          } else {
            outputWithTag(c, Constants.PERMANENT_ERROR_TAG, e.getMessage(), spannerRec);
          }
        } else {
          outputWithTag(c, Constants.PERMANENT_ERROR_TAG, cause.getMessage(), spannerRec);
        }
      } catch (Exception ex) {
        LOG.error("Failed to write to source: {}", ex);
        outputWithTag(c, Constants.PERMANENT_ERROR_TAG, ex.getMessage(), spannerRec);
      }
    }
  }

  private Mutation getShadowTableMutation(
      String tableName,
      String shadowTableName,
      JsonNode keysJson,
      com.google.cloud.Timestamp commitTimestamp,
      String recordSequence,
      Ddl ddl) // Added ddl parameter
      throws ChangeEventConvertorException {
    Mutation.WriteBuilder mutationBuilder = null;

    Table table = ddl.table(tableName);
    ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
    List<String> keyColumnNames =
        keyColumns.stream().map(k -> k.name()).collect(Collectors.toList());
    Set<String> keyColumnNamesSet = new HashSet<>(keyColumnNames);
    mutationBuilder =
        ChangeEventSpannerConvertor.mutationBuilderFromEvent(
            shadowTableName,
            table,
            keysJson,
            keyColumnNames,
            keyColumnNamesSet,
            /* convertNameToLowerCase= */ false);
    mutationBuilder.set(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME).to(commitTimestamp);
    mutationBuilder.set(Constants.RECORD_SEQ_COLUMN_NAME).to(Long.parseLong(recordSequence));

    return mutationBuilder.build();
  }

  void outputWithTag(
      ProcessContext c,
      TupleTag<String> tag,
      String message,
      TrimmedShardedDataChangeRecord record) {
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord = new ChangeStreamErrorRecord(jsonRec, message);

    // Permanent error metrics are inceremented differently based on regular or retryDLQ mode
    if (!record.isRetryRecord() && tag.equals(Constants.RETRYABLE_ERROR_TAG)) {
      retryableRecordCountMetric.inc();
    }
    c.output(tag, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }
}