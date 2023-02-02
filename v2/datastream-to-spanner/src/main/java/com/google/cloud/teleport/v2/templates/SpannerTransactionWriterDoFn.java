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

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_UUID_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import com.google.cloud.teleport.v2.templates.datastream.InvalidChangeEventException;
import com.google.cloud.teleport.v2.templates.session.NameAndCols;
import com.google.cloud.teleport.v2.templates.session.Session;
import com.google.cloud.teleport.v2.templates.session.SyntheticPKey;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.ExposedSpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
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
class SpannerTransactionWriterDoFn extends DoFn<FailsafeElement<String, String>, Timestamp>
    implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTransactionWriterDoFn.class);

  private final PCollectionView<Ddl> ddlView;

  private final Session session;

  private final SpannerConfig spannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient ExposedSpannerAccessor spannerAccessor;

  private final Counter processedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Total Events in Spanner Sink");

  private final Counter sucessfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Successful events");

  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Skipped events");

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Other errors");

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Conversion errors");

  private final Counter retryableErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Retryable errors");

  // The max length of tag allowed in Spanner Transaction tags.
  private static final int MAX_TXN_TAG_LENGTH = 50;

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      PCollectionView<Ddl> ddlView,
      Session session,
      String shadowTablePrefix,
      String sourceType) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
    this.session = session;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = ExposedSpannerAccessor.create(spannerConfig);
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    Ddl ddl = c.sideInput(ddlView);
    processedEvents.inc();

    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {
      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      changeEvent = transformChangeEvent(changeEvent, session);
      ChangeEventContext changeEventContext =
          ChangeEventContextFactory.createChangeEventContext(
              changeEvent, ddl, shadowTablePrefix, sourceType);

      // Sequence information for the current change event.
      ChangeEventSequence currentChangeEventSequence =
          ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(
              changeEventContext);

      // Start transaction
      spannerAccessor
          .getDatabaseClient()
          .readWriteTransaction(Options.tag(getTxnTag(c.getPipelineOptions())))
          .run(
              (TransactionCallable<Void>)
                  transaction -> {

                    // Sequence information for the last change event.
                    ChangeEventSequence previousChangeEventSequence =
                        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
                            transaction, changeEventContext);

                    /* There was a previous event recorded with a greater sequence information
                     * than current. Hence skip the current event.
                     */
                    if (previousChangeEventSequence != null
                        && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                      return null;
                    }

                    // Apply shadow and data table mutations.
                    transaction.buffer(changeEventContext.getMutations());
                    return null;
                  });
      com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
      c.output(timestamp);
      sucessfulEvents.inc();
    } catch (InvalidChangeEventException e) {
      // Errors that result from invalid change events.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      skippedEvents.inc();
    } catch (ChangeEventConvertorException e) {
      // Errors that result during Event conversions are not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      conversionErrors.inc();
    } catch (SpannerException se) {
      /* Errors that happen when writing to Cloud Spanner are considered retryable.
       * Since all event convertion errors are caught beforehand as permanent errors,
       * any other errors encountered while writing to Cloud Spanner can be retried.
       * Examples include:
       * 1. Deadline exceeded errors from Cloud Spanner.
       * 2. Failures due to foreign key/interleaved table constraints.
       * 3. Any transient errors in Cloud Spanner.
       */
      outputWithErrorTag(c, msg, se, SpannerTransactionWriter.RETRYABLE_ERROR_TAG);
      retryableErrors.inc();
    } catch (Exception e) {
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      failedEvents.inc();
    }
  }

  JsonNode transformChangeEvent(JsonNode changeEvent, Session session) {
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    if (!session.isEmpty()) {
      NameAndCols nameAndCols = session.getToSpanner().get(tableName);
      String spTableName = nameAndCols.getName();
      HashMap<String, String> cols = nameAndCols.getCols();

      // Convert the table name to corresponding Spanner table name.
      ((ObjectNode) changeEvent).put(EVENT_TABLE_NAME_KEY, spTableName);
      // Convert the column names to corresponding Spanner column names.
      for (Map.Entry<String, String> col : cols.entrySet()) {
        String srcCol = col.getKey(), spCol = col.getValue();
        if (!srcCol.equals(spCol)) {
          ((ObjectNode) changeEvent).set(spCol, changeEvent.get(srcCol));
          ((ObjectNode) changeEvent).remove(srcCol);
        }
      }
      HashMap<String, SyntheticPKey> synthPks = session.getSyntheticPks();
      if (synthPks.containsKey(spTableName)) {
        ((ObjectNode) changeEvent)
            .put(synthPks.get(spTableName).getCol(), changeEvent.get(EVENT_UUID_KEY).asText());
      }
    }
    return changeEvent;
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    StringWriter errors = new StringWriter();
    e.printStackTrace(new PrintWriter(errors));
    output.setErrorMessage(errors.toString());
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
}
