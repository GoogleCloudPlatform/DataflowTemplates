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

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_METADATA_KEY_PREFIX;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_UUID_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventTypeConvertor;
import com.google.cloud.teleport.v2.templates.datastream.InvalidChangeEventException;
import com.google.cloud.teleport.v2.templates.session.ColumnDef;
import com.google.cloud.teleport.v2.templates.session.CreateTable;
import com.google.cloud.teleport.v2.templates.session.NameAndCols;
import com.google.cloud.teleport.v2.templates.session.Session;
import com.google.cloud.teleport.v2.templates.session.SrcSchema;
import com.google.cloud.teleport.v2.templates.session.SyntheticPKey;
import com.google.cloud.teleport.v2.templates.spanner.common.Type;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.json.JSONObject;
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

  private final JsonObject transformationContext;

  private final SpannerConfig spannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // If set to true, round decimals inside jsons.
  private final Boolean roundJsonDecimals;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

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
      JsonObject transformationContext,
      String shadowTablePrefix,
      String sourceType,
      Boolean roundJsonDecimals) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
    this.session = session;
    this.transformationContext = transformationContext;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
    this.roundJsonDecimals = roundJsonDecimals;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
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
      if (!session.isEmpty()) {
        verifyTableInSession(changeEvent.get(EVENT_TABLE_NAME_KEY).asText());
        changeEvent = transformChangeEventViaSessionFile(changeEvent);
      }
      changeEvent = transformChangeEventData(changeEvent, spannerAccessor.getDatabaseClient(), ddl);
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
    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion. We do not output any
      // errors to dlq for this.
      LOG.warn(e.getMessage());
      skippedEvents.inc();
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

  void verifyTableInSession(String tableName)
      throws IllegalArgumentException, DroppedTableException {
    if (!session.getSrcToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableName + " in srcToId map , provide a valid session file.");
    }
    if (!session.getToSpanner().containsKey(tableName)) {
      throw new DroppedTableException(
          "Cannot find entry for "
              + tableName
              + " in toSpanner map, it is likely this table was dropped");
    }
    String tableId = session.getSrcToID().get(tableName).getName();
    if (!session.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
    }
  }

  /**
   * This function modifies the change event using transformations based on the session file. This
   * includes column/table name changes and adding of synthetic Primary Keys.
   */
  JsonNode transformChangeEventViaSessionFile(JsonNode changeEvent) {
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    String tableId = session.getSrcToID().get(tableName).getName();

    // Convert table and column names in change event.
    changeEvent = convertTableAndColumnNames(changeEvent, tableName);

    // Add shard id to change event
    changeEvent = populateShardId(changeEvent, tableId);

    // Add synthetic PK to change event.
    changeEvent = addSyntheticPKs(changeEvent, tableId);

    // Remove columns present in change event that were dropped in Spanner.
    changeEvent = removeDroppedColumns(changeEvent, tableId);

    return changeEvent;
  }

  JsonNode convertTableAndColumnNames(JsonNode changeEvent, String tableName) {
    NameAndCols nameAndCols = session.getToSpanner().get(tableName);
    String spTableName = nameAndCols.getName();
    Map<String, String> cols = nameAndCols.getCols();

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
    return changeEvent;
  }

  JsonNode populateShardId(JsonNode changeEvent, String tableId) {
    if (transformationContext.asMap().isEmpty()) {
      return changeEvent; // Nothing to do
    }
    CreateTable table = session.getSpSchema().get(tableId);
    ColumnDef shardIdColDef = table.getColDefs().get(table.getShardIdColumn());

    if (shardIdColDef == null) {
      return changeEvent;
    }
    JsonElement schemaToShardId = transformationContext.asMap().get("schemaToShardId");
    if(schemaToShardId==null || !schemaToShardId.isJsonObject()) {
      return changeEvent;
    }
    String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
    String shardId = ((JsonObject)schemaToShardId).get(schemaName).getAsString();
    ((ObjectNode) changeEvent).put(shardIdColDef.getName(), shardId);
    return changeEvent;
  }

  JsonNode addSyntheticPKs(JsonNode changeEvent, String tableId) {
    Map<String, ColumnDef> spCols = session.getSpSchema().get(tableId).getColDefs();
    Map<String, SyntheticPKey> synthPks = session.getSyntheticPks();
    if (synthPks.containsKey(tableId)) {
      String colID = synthPks.get(tableId).getColId();
      if (!spCols.containsKey(colID)) {
        throw new IllegalArgumentException(
            "Missing entry for "
                + colID
                + " in colDefs for tableId: "
                + tableId
                + ", provide a valid session file.");
      }
      ((ObjectNode) changeEvent)
          .put(spCols.get(colID).getName(), changeEvent.get(EVENT_UUID_KEY).asText());
    }
    return changeEvent;
  }

  JsonNode removeDroppedColumns(JsonNode changeEvent, String tableId) {
    Map<String, ColumnDef> spCols = session.getSpSchema().get(tableId).getColDefs();
    SrcSchema srcSchema = session.getSrcSchema().get(tableId);
    Map<String, ColumnDef> srcCols = srcSchema.getColDefs();
    for (String colId : srcSchema.getColIds()) {
      // If spanner columns do not contain this column Id, drop from change event.
      if (!spCols.containsKey(colId)) {
        ((ObjectNode) changeEvent).remove(srcCols.get(colId).getName());
      }
    }
    return changeEvent;
  }

  /**
   * This function changes the modifies and data of the change event. Currently, only supports a
   * single transformation set by roundJsonDecimals.
   */
  JsonNode transformChangeEventData(JsonNode changeEvent, DatabaseClient dbClient, Ddl ddl)
      throws Exception {
    if (!roundJsonDecimals) {
      return changeEvent;
    }
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    if (ddl.table(tableName) == null) {
      throw new Exception("Table from change event does not exist in Spanner. table=" + tableName);
    }
    Iterator<String> fieldNames = changeEvent.fieldNames();
    List<String> columnNames =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(fieldNames, Spliterator.ORDERED), false)
            .filter(f -> !f.startsWith(EVENT_METADATA_KEY_PREFIX))
            .collect(Collectors.toList());
    for (String columnName : columnNames) {
      Type columnType = ddl.table(tableName).column(columnName).type();
      if (columnType.getCode() == Type.Code.JSON || columnType.getCode() == Type.Code.PG_JSONB) {
        // JSON type cannot be a key column, hence setting requiredField to false.
        String jsonStr =
            ChangeEventTypeConvertor.toString(
                changeEvent, columnName.toLowerCase(), /* requiredField= */ false);
        if (jsonStr != null) {
          Statement statement =
              Statement.newBuilder(
                      "SELECT PARSE_JSON(@jsonStr, wide_number_mode=>'round') as newJson")
                  .bind("jsonStr")
                  .to(jsonStr)
                  .build();
          ResultSet resultSet = dbClient.singleUse().executeQuery(statement);
          while (resultSet.next()) {
            // We want to send the errors to the severe error queue, hence we do not catch any error
            // here.
            String val = resultSet.getJson("newJson");
            ((ObjectNode) changeEvent).put(columnName.toLowerCase(), val);
          }
        }
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
