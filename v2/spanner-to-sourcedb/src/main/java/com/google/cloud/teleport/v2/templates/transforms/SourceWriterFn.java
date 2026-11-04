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
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSpannerConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.SpannerDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheck;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.TransactionalCheckException;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.dbutils.processor.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessor;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessorFactory;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.utils.SchemaMapperUtils;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import com.google.cloud.teleport.v2.templates.utils.SpannerToSourceDbExceptionClassifier;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
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
  private static final Distribution SUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(SourceWriterFn.class, "successful_write_to_source_latency_ms");
  private static final Distribution UNSUCCESSFUL_WRITE_LATENCY_MS =
      Metrics.distribution(SourceWriterFn.class, "unsuccessful_write_to_source_latency_ms");

  // ---------------------------------------------------------------------------------------------
  // TEMPORARY DIAGNOSTICS for the SpannerToSourceDbLargeBacklogLT investigation. Revert this block
  // and the code that uses it once the root cause is confirmed. All names start with "diag_" or
  // "RR_DIAG" so they are easy to find and remove.
  // ---------------------------------------------------------------------------------------------
  public static final String DIAG_MARKER = "RR_DIAG_V1";
  // Max diagnostic log lines per JVM per minute, so 1B records cannot flood the logs.
  private static final int DIAG_LOG_BUDGET_PER_MINUTE = 300;
  private static final java.util.concurrent.atomic.AtomicLong diagLogWindowStartMs =
      new java.util.concurrent.atomic.AtomicLong();
  private static final java.util.concurrent.atomic.AtomicInteger diagLogsInWindow =
      new java.util.concurrent.atomic.AtomicInteger();
  private static final Counter DIAG_LOGS_SUPPRESSED =
      Metrics.counter(SourceWriterFn.class, "diag_logs_suppressed");
  private transient boolean diagMarkerCounted;
  private static final AtomicBoolean DIAG_SETUP_LOGGED = new AtomicBoolean(false);

  private final String sourceDbTimezoneOffset;
  private final List<Shard> shards;
  private final SpannerConfig spannerConfig;
  private transient SpannerDao spannerDao;
  private final SourceSchema sourceSchema;
  private final String shadowTablePrefix;
  private final String skipDirName;
  private final int maxThreadPerDataflowWorker;
  private final String source;
  private transient SourceProcessor sourceProcessor;
  private transient ISpToSrcSourceConnector sourceConnector;
  private final CustomTransformation customTransformation;
  private transient ISpannerMigrationTransformer spannerToSourceTransformer;

  private final PCollectionView<Ddl> ddlView;
  private final PCollectionView<Ddl> shadowTableDdlView;

  private final String sessionFilePath;
  private final String schemaOverridesFilePath;
  private final String tableOverrides;
  private final String columnOverrides;

  private transient Schema schema;
  private transient SchemaFileOverridesParser schemaFileOverridesParser;

  public SourceWriterFn(
      List<Shard> shards,
      SpannerConfig spannerConfig,
      String sourceDbTimezoneOffset,
      SourceSchema sourceSchema,
      String shadowTablePrefix,
      String skipDirName,
      int maxThreadPerDataflowWorker,
      String source,
      CustomTransformation customTransformation,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String sessionFilePath,
      String schemaOverridesFilePath,
      String tableOverrides,
      String columnOverrides) {

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
    this.sessionFilePath = sessionFilePath;
    this.schemaOverridesFilePath = schemaOverridesFilePath;
    this.tableOverrides = tableOverrides;
    this.columnOverrides = columnOverrides;
    try {
      this.sourceConnector = SourceProcessorFactory.getSource(source);
    } catch (com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException e) {
      throw new RuntimeException(e);
    }
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

  // for unit testing purposes
  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  // for unit testing purposes
  public void setSchemaFileOverridesParser(SchemaFileOverridesParser schemaFileOverridesParser) {
    this.schemaFileOverridesParser = schemaFileOverridesParser;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() throws UnsupportedSourceException {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceProcessor =
        SourceProcessorFactory.createSourceProcessor(source, shards, maxThreadPerDataflowWorker);
    sourceConnector = SourceProcessorFactory.getSource(source);
    spannerDao = new SpannerDao(spannerConfig);
    spannerToSourceTransformer =
        CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation);

    if (sessionFilePath != null && !sessionFilePath.isEmpty()) {
      schema = SessionFileReader.read(sessionFilePath);
    }
    if (schemaOverridesFilePath != null && !schemaOverridesFilePath.isEmpty()) {
      schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFilePath);
    }
    if (DIAG_SETUP_LOGGED.compareAndSet(false, true)) {
      LOG.info("{} SourceWriterFn setup (logged once per worker JVM)", DIAG_MARKER);
    }
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
    Ddl ddl = c.sideInput(ddlView);
    Ddl shadowTableDdl = c.sideInput(shadowTableDdlView);

    // SchemaMapper depends on Ddl side input, which is only available in processElement.
    ISchemaMapper schemaMapper =
        SchemaMapperUtils.getSchemaMapper(
            schema, schemaFileOverridesParser, tableOverrides, columnOverrides, ddl);

    KV<Long, TrimmedShardedDataChangeRecord> element = c.element();
    TrimmedShardedDataChangeRecord spannerRec = element.getValue();
    String shardId = spannerRec.getShard();
    // DIAG: count every input record, split by first attempt vs retry from the DLQ.
    if (!diagMarkerCounted) {
      diagMarkerCounted = true;
      Metrics.counter(SourceWriterFn.class, "diag_marker_rr_diag_v1").inc();
    }
    String diagAttempt = spannerRec.isRetryRecord() ? "retry" : "first_attempt";
    Metrics.counter(SourceWriterFn.class, "diag_input_" + diagAttempt).inc();
    if (shardId == null || shardId.equals(Constants.SEVERE_ERROR_SHARD_ID)) {
      Metrics.counter(SourceWriterFn.class, "diag_severe_shard_sentinel").inc();
      // if no shard or permanent error shard id found, move to permanent error
      outputWithTag(
          c, Constants.PERMANENT_ERROR_TAG, Constants.SHARD_NOT_PRESENT_ERROR_MESSAGE, spannerRec);
    } else if (shardId.equals(Constants.RETRYABLE_ERROR_SHARD_ID)) {
      Metrics.counter(SourceWriterFn.class, "diag_retryable_shard_sentinel_" + diagAttempt).inc();
      // if retryable error shard id found, move to retryable error
      outputWithTag(
          c, Constants.RETRYABLE_ERROR_TAG, Constants.SHARD_NOT_PRESENT_ERROR_MESSAGE, spannerRec);
    } else if (shardId.equals(skipDirName)) {
      // the record is skipped
      skippedRecordCountMetric.inc();
      outputWithTag(c, Constants.SKIPPED_TAG, Constants.SKIPPED_TAG_MESSAGE, spannerRec);
    } else {
      Stopwatch timer = Stopwatch.createStarted();
      // DIAG: the transaction step reached, how the record ended, and how often the Spanner client
      // re-ran the callable (it re-runs it on ABORTED, which repeats the source write).
      AtomicReference<String> diagStage = new AtomicReference<>("before_transaction");
      AtomicReference<String> diagOutcome = new AtomicReference<>("none");
      AtomicInteger diagCallableRuns = new AtomicInteger();
      // Get the latest commit timestamp processed at source
      try {
        JsonNode keysJson = mapper.readTree(spannerRec.getMod().getKeysJson());
        String tableName = spannerRec.getTableName();
        com.google.cloud.spanner.Key primaryKey =
            ChangeEventSpannerConvertor.changeEventToPrimaryKey(
                tableName, ddl, keysJson, /* convertNameToLowerCase= */ false);
        String shadowTableName = shadowTablePrefix + tableName;

        Boolean transactionResult =
            spannerDao
                .getDatabaseClient()
                .readWriteTransaction(Options.priority(spannerConfig.getRpcPriority().get()))
                .allowNestedTransaction()
                .run(
                    (TransactionRunner.TransactionCallable<Boolean>)
                        shadowTransaction -> {
                          diagCallableRuns.incrementAndGet();
                          diagStage.set("read_shadow");
                          diagOutcome.set("none");
                          boolean isSourceAhead = false;
                          // Boolean reference to capture if the record was written in the
                          // transaction
                          AtomicBoolean isRecordWritten = new AtomicBoolean(false);
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
                                              >= Long.parseLong(spannerRec.getRecordSequence())));

                          if (isSourceAhead) {
                            // DIAG: "already_applied" means the shadow row holds exactly this
                            // record, so an earlier attempt for it already committed.
                            boolean sameRecord =
                                shadowTableRecord
                                            .getProcessedCommitTimestamp()
                                            .compareTo(spannerRec.getCommitTimestamp())
                                        == 0
                                    && shadowTableRecord.getRecordSequence()
                                        == Long.parseLong(spannerRec.getRecordSequence());
                            diagOutcome.set(sameRecord ? "already_applied" : "source_ahead_newer");
                          }

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
                            diagStage.set("write_source");
                            boolean isEventFiltered =
                                InputRecordProcessor.processRecord(
                                    spannerRec,
                                    schemaMapper,
                                    ddl,
                                    sourceSchema,
                                    sourceDao,
                                    shardId,
                                    sourceDbTimezoneOffset,
                                    sourceProcessor.getDmlGenerator(),
                                    spannerToSourceTransformer,
                                    this.source,
                                    check);
                            isRecordWritten.set(!isEventFiltered);
                            diagStage.set("buffer_shadow");
                            diagOutcome.set(isEventFiltered ? "filtered" : "written");
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
                                    ddl),
                                shadowTransaction);
                          }
                          // DIAG: anything thrown after this point comes from the Spanner commit.
                          diagStage.set("commit");
                          return isRecordWritten.get();
                        });
        diagStage.set("done");
        diagRecordOutcome(
            spannerRec, diagAttempt, diagOutcome.get(), diagCallableRuns.get(), timer);

        if (Boolean.TRUE.equals(transactionResult)) {
          successRecordCountMetric.inc();
          Counter recordsWrittenToSource =
              Metrics.counter(shardId, "records_written_to_source_" + shardId);
          recordsWrittenToSource.inc(1);
          Distribution lagMetric =
              Metrics.distribution(shardId, "replication_lag_in_seconds_" + shardId);
          Instant instTime = Instant.now();
          Instant commitTsInst = spannerRec.getCommitTimestamp().toSqlTimestamp().toInstant();
          long replicationLag = ChronoUnit.SECONDS.between(commitTsInst, instTime);
          lagMetric.update(replicationLag);
          SUCCESSFUL_WRITE_LATENCY_MS.update(timer.elapsed(TimeUnit.MILLISECONDS));
        }
        if (spannerRec.isRetryRecord()) {
          retryableRecordCountMetric.dec();
        }
        com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
        c.output(Constants.SUCCESS_TAG, timestamp.toString());
        // Since we have wrapped the logic inside Spanner transaction, the exceptions would also be
        // wrapped inside a SpannerException.
        // We need to get and inspect the cause while handling the exception.
      } catch (Exception ex) {
        Throwable cause = ex.getCause();
        String message = ex.getMessage();
        if (cause != null) {
          message += ", Caused by: " + cause.getMessage();
        }
        TupleTag<String> errorTag =
            SpannerToSourceDbExceptionClassifier.classify(ex, sourceConnector);
        diagRecordError(
            spannerRec,
            diagAttempt,
            errorTag,
            ex,
            diagStage.get(),
            diagOutcome.get(),
            diagCallableRuns.get(),
            timer);
        outputWithTag(c, errorTag, message, spannerRec);
        UNSUCCESSFUL_WRITE_LATENCY_MS.update(timer.elapsed(TimeUnit.MILLISECONDS));
      }
    }
  }

  private Mutation getShadowTableMutation(
      String tableName,
      String shadowTableName,
      JsonNode keysJson,
      com.google.cloud.Timestamp commitTimestamp,
      String recordSequence,
      Ddl ddl)
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

  // ---------------------------------------------------------------------------------------------
  // TEMPORARY DIAGNOSTICS helpers (see the DIAG block at the top of the class).
  // ---------------------------------------------------------------------------------------------

  /** Counts how a record that finished the transaction ended, and logs the unusual endings. */
  private void diagRecordOutcome(
      TrimmedShardedDataChangeRecord rec,
      String attempt,
      String outcome,
      int callableRuns,
      Stopwatch timer) {
    Metrics.counter(SourceWriterFn.class, "diag_outcome_" + outcome + "_" + attempt).inc();
    if (callableRuns > 1) {
      Metrics.counter(SourceWriterFn.class, "diag_tx_callable_reruns").inc(callableRuns - 1);
    }
    // Every retry outcome, and first attempts that find their own record already applied, are the
    // events that can make success_record_count disagree with the rows in the source database.
    boolean unusual = rec.isRetryRecord() || !outcome.equals("written");
    if (unusual && diagShouldLog()) {
      LOG.info(
          "{} outcome: outcome={} attempt={} key={} table={} shard={} commitTs={} recordSeq={}"
              + " retryCount={} callableRuns={} elapsedMs={}",
          DIAG_MARKER,
          outcome,
          attempt,
          rec.getMod().getKeysJson(),
          rec.getTableName(),
          rec.getShard(),
          rec.getCommitTimestamp(),
          rec.getRecordSequence(),
          rec.getMetadataRetryCount(),
          callableRuns,
          timer.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  /** Counts and logs a failed record with everything needed to trace it later. */
  private void diagRecordError(
      TrimmedShardedDataChangeRecord rec,
      String attempt,
      TupleTag<String> errorTag,
      Exception ex,
      String stage,
      String outcome,
      int callableRuns,
      Stopwatch timer) {
    String kind = Constants.PERMANENT_ERROR_TAG.equals(errorTag) ? "permanent" : "retryable";
    String code = diagErrorCode(ex);
    // True when the source write was committed and only the Spanner commit of the shadow table row
    // failed. The Spanner commit may still have been applied on the server.
    boolean sourceWriteDone = stage.equals("commit") && outcome.equals("written");
    Metrics.counter(SourceWriterFn.class, "diag_error_" + kind + "_" + attempt).inc();
    Metrics.counter(SourceWriterFn.class, "diag_error_stage_" + stage).inc();
    Metrics.counter(SourceWriterFn.class, "diag_error_code_" + code).inc();
    if (sourceWriteDone) {
      Metrics.counter(SourceWriterFn.class, "diag_error_after_source_write_" + attempt).inc();
    }
    if (diagShouldLog()) {
      Throwable cause = ex.getCause();
      String message = String.valueOf(ex.getMessage());
      LOG.warn(
          "{} error: kind={} attempt={} key={} table={} shard={} commitTs={} recordSeq={}"
              + " retryCount={} stage={} outcomeBeforeError={} sourceWriteDone={} callableRuns={}"
              + " elapsedMs={} exClass={} code={} message={} causeClass={} causeMessage={}",
          DIAG_MARKER,
          kind,
          attempt,
          rec.getMod().getKeysJson(),
          rec.getTableName(),
          rec.getShard(),
          rec.getCommitTimestamp(),
          rec.getRecordSequence(),
          rec.getMetadataRetryCount(),
          stage,
          outcome,
          sourceWriteDone,
          callableRuns,
          timer.elapsed(TimeUnit.MILLISECONDS),
          ex.getClass().getName(),
          code,
          message.length() > 500 ? message.substring(0, 500) : message,
          cause == null ? "none" : cause.getClass().getName(),
          cause == null ? "none" : String.valueOf(cause.getMessage()));
    }
  }

  /** Spanner error code, else the SQL error code of the cause, else the exception class name. */
  static String diagErrorCode(Exception ex) {
    if (ex instanceof SpannerException) {
      Throwable cause = ex.getCause();
      if (cause instanceof java.sql.SQLException) {
        return "spanner_"
            + ((SpannerException) ex).getErrorCode()
            + "_sql_"
            + ((java.sql.SQLException) cause).getErrorCode();
      }
      return "spanner_" + ((SpannerException) ex).getErrorCode();
    }
    return ex.getClass().getSimpleName();
  }

  /** Allows at most DIAG_LOG_BUDGET_PER_MINUTE diagnostic log lines per minute per JVM. */
  static boolean diagShouldLog() {
    long now = System.currentTimeMillis();
    long windowStart = diagLogWindowStartMs.get();
    if (now - windowStart >= 60_000 && diagLogWindowStartMs.compareAndSet(windowStart, now)) {
      diagLogsInWindow.set(0);
    }
    if (diagLogsInWindow.incrementAndGet() <= DIAG_LOG_BUDGET_PER_MINUTE) {
      return true;
    }
    DIAG_LOGS_SUPPRESSED.inc();
    return false;
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
