/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.SHARD_ID_COLUMN_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.CONVERSION_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.OTHER_PERMANENT_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERRORS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SKIPPED_EVENTS_COUNTER_NAME;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.SUCCESSFUL_EVENTS_COUNTER_NAME;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerExceptionParser;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerMigrationException;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag;
import com.google.cloud.teleport.v2.templates.utils.WatchdogRunnable;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Change events from DataStream into Cloud Spanner.
 *
 * <p>Change events are individually processed. Shadow tables store the version information(that
 * specifies the commit order) for each primary key. Shadow tables are consulted before actual
 * writes to Cloud Spanner to preserve the correctness and consistency of data.
 *
 * <p>Change events written successfully will be pushed onto the primary output with their commit
 * timestamps.
 *
 * <p>Change events that failed to be written will be pushed onto the secondary output tagged with
 * PERMANENT_ERROR_TAG/RETRYABLE_ERROR_TAG along with the exception that caused the failure.
 */
class SpannerTransactionWriterDoFn
    extends DoFn<KV<Long, FailsafeElement<String, String>>, Timestamp> implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTransactionWriterDoFn.class);

  private final PCollectionView<Ddl> ddlView;
  private final PCollectionView<Ddl> shadowTableDdlView;

  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;
  /* SpannerAccessor for shadow table database must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor shadowTableSpannerAccessor;

  // Number of events successfully processed.
  private final Counter successfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SUCCESSFUL_EVENTS_COUNTER_NAME);

  // Number of events that have a schema incompatible with the spanner schema.
  private final Counter invalidEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Invalid events");

  // Number of events skipped from being written to spanner because the events were stale.
  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, SKIPPED_EVENTS_COUNTER_NAME);

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, OTHER_PERMANENT_ERRORS_COUNTER_NAME);

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, CONVERSION_ERRORS_COUNTER_NAME);

  private final Counter retryableErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, RETRYABLE_ERRORS_COUNTER_NAME);

  // Number of events that were successfully read from dlq/retry and written to spanner.
  private final Counter successfulEventRetries =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Successful event retries");

  // Distribution of retries done for any event.
  private final Distribution eventRetries =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Event retries");

  // Time taken from events being read by Datastream to being written to Cloud Spanner. Time
  // duration between datastream_read_timestamp and write_timestamp
  private final Distribution systemLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag system latency");

  // Time taken for events to get processed by the Dataflow pipeline. Time duration between
  // dataflow_read_timestamp and write_timestamp.
  private final Distribution dataflowLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag dataflow latency");

  // Time duration between source_timestamp and write_timestamp.
  private final Distribution totalLatency =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "Replication lag total latency");

  // Latency of creating and writing mutations from change events to spanner.
  private final Distribution spannerWriterLatencyMs =
      Metrics.distribution(SpannerTransactionWriterDoFn.class, "spanner_writer_latency_ms");

  private final Counter droppedTableExceptions =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Dropped table exceptions");

  // The max length of tag allowed in Spanner Transaction tags.
  private static final int MAX_TXN_TAG_LENGTH = 50;

  /* The run mode, whether it is regular or retry. */
  private final Boolean isRegularRunMode;

  /*
   * The watchdog thread monitors the progress of Spanner transactions and ensures that they
   * are not stuck for an extended period of time. This is important because in load testing there were
   * instances where transactions were stuck, causing bottlenecks in the Dataflow pipeline.
   *
   * The WatchdogRunnable is designed to track if a transaction is making progress by comparing
   * the number of transaction attempts (`transactionAttemptCount`) over time. The `isInTransaction`
   * flag indicates whether a transaction is currently active. If the number of attempts remains
   * the same for a period of 15 minutes while the transaction is active, the watchdog logs a warning
   * and terminates the process by calling `System.exit(1)`.
   *
   * By running in the background, this watchdog thread ensures that long-running transactions
   * do not stall indefinitely, providing a safeguard mechanism for transaction processing in
   * the pipeline.
   */
  private transient AtomicLong transactionAttemptCount;
  private transient AtomicBoolean isInTransaction;
  private transient AtomicBoolean keepWatchdogRunning;
  private transient Thread watchdogThread;

  private final boolean usesSeparateShadowTableDb;

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.ddlView = ddlView;
    this.shadowTableDdlView = shadowTableDdlView;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
    this.isRegularRunMode = isRegularRunMode;
    this.usesSeparateShadowTableDb =
        !(spannerConfig.getInstanceId().equals(shadowTableSpannerConfig.getInstanceId())
            && spannerConfig.getDatabaseId().equals(shadowTableSpannerConfig.getDatabaseId()));
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    shadowTableSpannerAccessor =
        usesSeparateShadowTableDb
            ? SpannerAccessor.getOrCreate(shadowTableSpannerConfig)
            : spannerAccessor;
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    // Setup and start the watchdog thread.
    transactionAttemptCount = new AtomicLong(0);
    isInTransaction = new AtomicBoolean(false);
    keepWatchdogRunning = new AtomicBoolean(true);
    watchdogThread =
        new Thread(
            new WatchdogRunnable(transactionAttemptCount, isInTransaction, keepWatchdogRunning),
            "SpannerTransactionWriterDoFn.WatchdogThread");
    watchdogThread.setDaemon(true);
    watchdogThread.start();
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerAccessor.close();
    if (usesSeparateShadowTableDb) {
      shadowTableSpannerAccessor.close();
    }
    // Stop the watchdog thread.
    keepWatchdogRunning.set(false);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element().getValue();
    Ddl ddl = c.sideInput(ddlView);
    // TODO: pass shadow table ddl to shdaow tble mutaiton generator and sequence reader.
    Ddl shadowTableDdl = c.sideInput(shadowTableDdlView);
    Instant startTimestamp = Instant.now();
    String migrationShardId = null;
    boolean isRetryRecord = false;
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      migrationShardId =
          Optional.ofNullable(changeEvent.get(SHARD_ID_COLUMN_NAME))
              .map(shardIdNode -> changeEvent.get(shardIdNode.asText()).asText())
              .orElse(null);
      JsonNode retryCount = changeEvent.get("_metadata_retry_count");

      if (retryCount != null) {
        eventRetries.update(retryCount.asLong());
        isRetryRecord = true;
      }
      ChangeEventContext changeEventContext =
          ChangeEventContextFactory.createChangeEventContext(
              changeEvent, ddl, shadowTableDdl, shadowTablePrefix, sourceType);

      // Sequence information for the current change event.
      ChangeEventSequence currentChangeEventSequence =
          ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(
              changeEventContext);

      if (usesSeparateShadowTableDb) {
        processCrossDatabaseTransaction(
            c, changeEventContext, currentChangeEventSequence, shadowTableDdl, ddl);
      } else {
        processSingleDatabaseTransaction(
            c, changeEventContext, currentChangeEventSequence, shadowTableDdl, ddl);
      }
      com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
      c.output(timestamp);
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class,
                migrationShardId + " : " + SUCCESSFUL_EVENTS_COUNTER_NAME)
            .inc();
      }
      successfulEvents.inc();
      updateLatencyMetrics(changeEvent, startTimestamp);

      // increment the successful retry count if this was retry attempt
      if (isRegularRunMode && isRetryRecord) {
        successfulEventRetries.inc();
      }

    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion. We do not output any
      // errors to dlq for this.
      // Note that this is not loogged to DLQ!!
      LOG.error("Table dropped during migration for changeEventMessage {}", msg, e.getMessage());
      droppedTableExceptions.inc();
    } catch (InvalidChangeEventException e) {
      LOG.error("Invalid Change Exception", e);
      // Errors that result from invalid change events.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      invalidEvents.inc();
      if (migrationShardId != null) {
        Metrics.counter(SpannerTransactionWriterDoFn.class, migrationShardId + " : Invalid events")
            .inc();
      }
    } catch (ChangeEventConvertorException e) {
      LOG.error("Conversion Error", e);
      // Errors that result during Event conversions are not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class,
                migrationShardId + " : " + CONVERSION_ERRORS_COUNTER_NAME)
            .inc();
      }
      conversionErrors.inc();
    } catch (IllegalStateException ex) {
      /*
       * IllegalStateException can occur due to conditions like spanner pool being closed,
       * in which case if this event is requed to same or different node at a later point in time,
       * a retry might work.
       */
      outputWithErrorTag(c, msg, ex, DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG);
      // do not increment the retry error count if this was retry attempt
      if (!isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (SpannerException ex) {
      /*
       * There are many SpannerExceptions which can occur. Some of them are retryable and some of them are non-retryable.
       * Examples:
       * 1. Deadline exceeded errors from Cloud Spanner - Retryable error
       * 2. Failures due to foreign key/interleaved table constraints - Retryable error
       * 3. Unique index violation - Permanent error
       */
      SpannerMigrationException spannerMigrationException = SpannerExceptionParser.parse(ex);
      ErrorTag outputTag =
          DatastreamToSpannerExceptionClassifier.classify(spannerMigrationException);
      switch (outputTag) {
        case PERMANENT_ERROR:
          LOG.error(
              "A severe error occurred while processing the event.", spannerMigrationException);
          outputWithErrorTag(c, msg, ex, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
          break;
        case RETRYABLE_ERROR:
          LOG.warn(
              "A retryable error occurred while processing the event, the event will be retried again.",
              spannerMigrationException);
          outputWithErrorTag(c, msg, ex, DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG);
      }
      // do not increment the retry error count if this was retry attempt
      if (ErrorTag.RETRYABLE_ERROR.equals(outputTag) && !isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (Exception e) {
      LOG.error("Unhandled Exception", e);
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      failedEvents.inc();
      if (migrationShardId != null) {
        Metrics.counter(
                SpannerTransactionWriterDoFn.class, migrationShardId + " : Permanent errors")
            .inc();
      }
    }
  }

  private void processSingleDatabaseTransaction(
      ProcessContext c,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl ddl) {

    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag(getTxnTag(c.getPipelineOptions())),
            Options.excludeTxnFromChangeStreams(),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                transaction -> {
                  isInTransaction.set(true);
                  transactionAttemptCount.incrementAndGet();
                  // Sequence information for the last change event.
                  ChangeEventSequence previousChangeEventSequence =
                      ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
                          transaction, changeEventContext, shadowDdl, false);
                  /* There was a previous event recorded with a greater sequence information
                   * than current. Hence, skip the current event.
                   */
                  if (previousChangeEventSequence != null
                      && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                    skippedEvents.inc();
                    return null;
                  }
                  // Execute DML if applicable
                  Statement dataDml = changeEventContext.getDataDmlStatement(ddl);

                  if (dataDml != null) {
                    transaction.executeUpdate(dataDml);
                  }

                  // Apply shadow and data table mutations (only if they exist)
                  transaction.buffer(changeEventContext.getMutations());
                  isInTransaction.set(false);
                  return null;
                });
  }

  /**
   * This method does a cross database transaction across the shadow table db and the main db. It
   * performs the following steps:
   *
   * <ul>
   *   <li>Start shadow table transaction - tx1
   *   <li>Read the shadow table row with exclusive lock - tx1.read()
   *   <li>Start main table transaction - tx2
   *   <li>Read the main table row with exclusive lock - tx2.read()
   *   <li>Before committing tx2, read the shadow table row again using tx1 inside tx2.
   *   <li>Write to main table - tx2.write()
   *   <li>Commit main transaction - tx2.commit()
   *   <li>Update shadow table - tx1.write()
   *   <li>Commit shadow table transaction - tx1.commit()
   * </ul>
   *
   * <p>Step 5 acts as a lock validation ensuring that no other process is writing this row. If due
   * to some network partition/machine failure, the lock is lost during main table write, Spanner
   * will release the exclusive lock while the thread still continues its attempt to write. This
   * validation ensures that main table write also fails in such cases and the event goes for
   * retrial. This ensures no 2 processes are updating the same row together.
   */
  void processCrossDatabaseTransaction(
      ProcessContext c,
      ChangeEventContext changeEventContext,
      ChangeEventSequence currentChangeEventSequence,
      Ddl shadowDdl,
      Ddl dataTableDdl) {

    shadowTableSpannerAccessor
        .getDatabaseClient()
        .readWriteTransaction(
            Options.tag(getTxnTag(c.getPipelineOptions())),
            Options.excludeTxnFromChangeStreams(),
            Options.priority(spannerConfig.getRpcPriority().get()))
        .allowNestedTransaction()
        .run(
            (TransactionRunner.TransactionCallable<Void>)
                shadowTxn -> {
                  isInTransaction.set(true);
                  transactionAttemptCount.incrementAndGet();

                  // Build lock query based on source type
                  ChangeEventSequence previousChangeEventSequence =
                      ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
                          shadowTxn, changeEventContext, shadowDdl, /* useSqlStatments= */ true);

                  if (previousChangeEventSequence != null
                      && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                    skippedEvents.inc();
                    return null;
                  }

                  // Start main table transaction
                  spannerAccessor
                      .getDatabaseClient()
                      .readWriteTransaction(
                          Options.tag(getTxnTag(c.getPipelineOptions())),
                          Options.excludeTxnFromChangeStreams(),
                          Options.priority(spannerConfig.getRpcPriority().get()))
                      .run(
                          (TransactionRunner.TransactionCallable<Void>)
                              mainTxn -> {
                                // Read row from main table with lock scanned ranges to acquire
                                // exclusive lock on the main table row.
                                changeEventContext.readDataTableRowWithExclusiveLock(
                                    mainTxn, dataTableDdl);

                                // Validate the row still holds the exclusive lock. In case of
                                // network
                                // partitions, it could happen that Spanner releases the lock while
                                // this
                                // thread gets killed.
                                ChangeEventSequence validationSequence =
                                    ChangeEventSequenceFactory
                                        .createChangeEventSequenceFromShadowTable(
                                            shadowTxn,
                                            changeEventContext,
                                            shadowDdl,
                                            /* useSqlStatments= */ true);

                                if (validationSequence != null
                                    && validationSequence.compareTo(previousChangeEventSequence)
                                        != 0) {
                                  // This code path should never execute since Spanner automatically
                                  // aborts transactions when locks are released.
                                  LOG.error(
                                      "Sequence mismatch: validation sequence {}, previous sequence {}",
                                      validationSequence,
                                      previousChangeEventSequence);
                                  throw new Exception(
                                      "Shadow table sequence changed during transaction");
                                }

                                // Execute Data DML if applicable
                                Statement dataDml =
                                    changeEventContext.getDataDmlStatement(dataTableDdl);
                                if (dataDml != null) {
                                  mainTxn.executeUpdate(dataDml);
                                }

                                // Write to main table
                                if (changeEventContext.getDataMutation() != null) {
                                  mainTxn.buffer(changeEventContext.getDataMutation());
                                }
                                return null;
                              });

                  // Update shadow table if main transaction succeeded
                  shadowTxn.buffer(changeEventContext.getShadowMutation());
                  isInTransaction.set(false);
                  return null;
                });
  }

  void updateLatencyMetrics(JsonNode changeEvent, Instant startTimestamp) {
    Instant endTimestamp = Instant.now();
    spannerWriterLatencyMs.update(new Duration(startTimestamp, endTimestamp).getMillis());
    long currentTimestampInSeconds = System.currentTimeMillis() / 1000L;
    long sourceTimestamp = changeEvent.get("_metadata_timestamp").asLong();
    long datastreamTimestamp = changeEvent.get("_metadata_read_timestamp").asLong();
    long dataflowTimestamp = changeEvent.get("_metadata_dataflow_timestamp").asLong();
    totalLatency.update(currentTimestampInSeconds - sourceTimestamp);
    systemLatency.update(currentTimestampInSeconds - datastreamTimestamp);
    dataflowLatency.update(currentTimestampInSeconds - dataflowTimestamp);
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }

  String getTxnTag(PipelineOptions options) {
    String jobId = "datastreamToSpanner";
    try {
      DataflowWorkerHarnessOptions harnessOptions = options.as(DataflowWorkerHarnessOptions.class);
      jobId = harnessOptions.getJobId();
    } catch (Exception e) {
      LOG.warn(
          "Unable to find Dataflow job id. Spanner transaction tags will not contain the dataflow"
              + " job id.",
          e);
    }
    // Spanner transaction tags have a limit of 50 characters. Dataflow job id is 40 chars in
    // length.
    String txnTag = "txBy=" + jobId;
    if (txnTag.length() > MAX_TXN_TAG_LENGTH) {
      txnTag = txnTag.substring(0, MAX_TXN_TAG_LENGTH);
    }
    return txnTag;
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public void setSpannerAccessor(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
  }

  public void setShadowTableSpannerAccessor(SpannerAccessor shadowTableSpannerAccessor) {
    this.shadowTableSpannerAccessor = shadowTableSpannerAccessor;
  }

  public void setTransactionAttemptCount(AtomicLong transactionAttemptCount) {
    this.transactionAttemptCount = transactionAttemptCount;
  }

  public void setIsInTransaction(AtomicBoolean isInTransaction) {
    this.isInTransaction = isInTransaction;
  }
}
