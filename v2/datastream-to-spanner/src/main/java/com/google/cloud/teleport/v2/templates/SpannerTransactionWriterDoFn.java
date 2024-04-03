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

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_UUID_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.MYSQL_SOURCE_TYPE;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import com.google.cloud.teleport.v2.templates.datastream.InvalidChangeEventException;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
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
class SpannerTransactionWriterDoFn extends DoFn<FailsafeElement<String, String>, Timestamp>
    implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTransactionWriterDoFn.class);

  private final PCollectionView<Ddl> ddlView;

  // The mapping information read from the session file generated by HarbourBridge.
  private final Schema schema;

  /* The context used to populate transformation information */
  private final TransformationContext transformationContext;

  private final SpannerConfig spannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  private final String advancedCustomParameters;

  private final Counter processedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Total events processed");

  private final Counter successfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Successful events");

  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Skipped events");

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Other permanent errors");

  private final Counter filteredEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Filtered events");

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Conversion errors");

  private final Counter retryableErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Retryable errors");

  // The max length of tag allowed in Spanner Transaction tags.
  private static final int MAX_TXN_TAG_LENGTH = 50;

  /* The run mode, whether it is regular or retry. */
  private final Boolean isRegularRunMode;

  private final String customJarPath;

  private final String customClassName;

  private ISpannerMigrationTransformer datastreamToSpannerTransformation;

  SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      PCollectionView<Ddl> ddlView,
      Schema schema,
      TransformationContext transformationContext,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode,
      String advancedCustomParameters,
      String customJarPath,
      String customClassName) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
    this.schema = schema;
    this.transformationContext = transformationContext;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
    this.isRegularRunMode = isRegularRunMode;
    this.advancedCustomParameters = advancedCustomParameters;
    this.customClassName = customClassName;
    this.customJarPath = customJarPath;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    datastreamToSpannerTransformation = getApplyTransformationImpl(customJarPath, customClassName);
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

    boolean isRetryRecord = false;
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());

      JsonNode retryCount = changeEvent.get("_metadata_retry_count");
      String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
      Map<String, Object> sourceRecord = convertJsonNodeToMap(changeEvent);
      LOG.info("Source record", sourceRecord);

      if (retryCount != null) {
        isRetryRecord = true;
      }
      if (!schema.isEmpty()) {
        verifyTableInSession(tableName);
        changeEvent = transformChangeEventViaSessionFile(changeEvent);
      }
      if (datastreamToSpannerTransformation != null) {
        MigrationTransformationRequest migrationTransformationRequest =
            new MigrationTransformationRequest(tableName, sourceRecord, "");
        MigrationTransformationResponse migrationTransformationResponse =
            datastreamToSpannerTransformation.toSpannerRow(migrationTransformationRequest);
        if (migrationTransformationResponse.isEventFiltered()) {
          filteredEvents.inc();
          outputWithFilterTag(c, c.element());
          return;
        }
        LOG.info("Spanner transformed record: " + migrationTransformationResponse.getResponseRow());
        changeEvent =
            transformChangeEventViaAdvancedTransformation(
                changeEvent, migrationTransformationResponse.getResponseRow(), tableName, ddl);
      }
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
          .readWriteTransaction(
              Options.tag(getTxnTag(c.getPipelineOptions())),
              Options.priority(spannerConfig.getRpcPriority().get()))
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
      successfulEvents.inc();

      // decrement the retry error count if this was retry attempt
      if (isRegularRunMode && isRetryRecord) {
        retryableErrors.dec();
      }

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
    } catch (SpannerException | IllegalStateException ex) {
      /* Errors that happen when writing to Cloud Spanner are considered retryable.
       * Since all event conversion errors are caught beforehand as permanent errors,
       * any other errors encountered while writing to Cloud Spanner can be retried.
       * Examples include:
       * 1. Deadline exceeded errors from Cloud Spanner.
       * 2. Failures due to foreign key/interleaved table constraints.
       * 3. Any transient errors in Cloud Spanner.
       * IllegalStateException can occur due to conditions like spanner pool being closed,
       * in which case if this event is requed to same or different node at a later point in time,
       * a retry might work.
       */
      outputWithErrorTag(c, msg, ex, SpannerTransactionWriter.RETRYABLE_ERROR_TAG);
      // do not increment the retry error count if this was retry attempt
      if (!isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (Exception e) {
      LOG.info("Exception " + e);
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      failedEvents.inc();
    }
  }

  private JsonNode transformChangeEventViaAdvancedTransformation(
      JsonNode changeEvent, Map<String, Object> spannerRecord, String tableName, Ddl ddl)
      throws Exception {
    Table table = ddl.table(tableName);
    for (Map.Entry<String, Object> entry : spannerRecord.entrySet()) {
      String columnName = entry.getKey();
      if (!changeEvent.has(columnName)) {
        LOG.info("Column " + columnName + " doesn't exist in change event: " + changeEvent);
        continue;
      }
      Object columnValue = entry.getValue();
      Type columnType = table.column(columnName).type();
      switch (columnType.getCode()) {
        case BOOL:
        case PG_BOOL:
          if (columnValue instanceof Boolean) {
            ((ObjectNode) changeEvent).put(columnName, (Boolean) columnValue);
            break;
          } else {
            throw new Exception("Column type not boolean");
          }
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
          if (columnValue instanceof String) {
            ((ObjectNode) changeEvent).put(columnName, (String) columnValue);
            break;
          } else {
            throw new Exception("Column type not string");
          }
        default:
          throw new IllegalArgumentException(
              "Column name("
                  + columnName
                  + ") has unsupported column type("
                  + columnType.getCode()
                  + ")");
      }
    }
    return changeEvent;
  }

  private Map<String, Object> convertJsonNodeToMap(JsonNode changeEvent)
      throws ChangeEventConvertorException {

    Map<String, Object> sourceRecord = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = changeEvent.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String key = field.getKey();
      JsonNode value = field.getValue();

      // Recursively convert nested JSONNodes
      if (value.isObject()) {
        sourceRecord.put(key, convertJsonNodeToMap(value));
      } else if (value.isArray()) {
        sourceRecord.put(key, value.toString());
      } else if (value.isTextual()) {
        sourceRecord.put(key, value.asText());
      } else if (value.isBoolean()) {
        sourceRecord.put(key, value.asBoolean());
      } else if (value.isDouble()) {
        sourceRecord.put(key, value.asDouble());
      } else if (value.isLong()) {
        sourceRecord.put(key, value.asLong());
      } else if (value.isNull()) {
        sourceRecord.put(key, null);
      }
    }
    return sourceRecord;
  }

  public ISpannerMigrationTransformer getApplyTransformationImpl(
      String customJarPath, String customClassName) {
    if (!customJarPath.isEmpty() && !customClassName.isEmpty()) {
      LOG.info(
          "Getting custom sharding fetcher : " + customJarPath + " with class: " + customClassName);
      try {
        // Get the start time of loading the custom class
        Instant startTime = Instant.now();

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = JarFileReader.saveFilesLocally(customJarPath);

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> advancedTransformationClass = urlClassLoader.loadClass(customClassName);

        // Create a new instance from the loaded class
        Constructor<?> constructor = advancedTransformationClass.getConstructor();
        ISpannerMigrationTransformer datastreamToSpannerTransformation =
            (ISpannerMigrationTransformer) constructor.newInstance();
        // Get the end time of loading the custom class
        Instant endTime = Instant.now();
        LOG.info(
            "Custom jar "
                + customJarPath
                + ": Took "
                + (new Duration(startTime, endTime)).toString()
                + " to load");
        LOG.info("Invoking init of the custom class with input as {}", advancedCustomParameters);
        datastreamToSpannerTransformation.init(advancedCustomParameters);
        return datastreamToSpannerTransformation;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    return null;
  }

  void verifyTableInSession(String tableName)
      throws IllegalArgumentException, DroppedTableException {
    if (!schema.getSrcToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableName + " in srcToId map, provide a valid session file.");
    }
    if (!schema.getToSpanner().containsKey(tableName)) {
      throw new DroppedTableException(
          "Cannot find entry for "
              + tableName
              + " in toSpanner map, it is likely this table was dropped");
    }
    String tableId = schema.getSrcToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
    }
  }

  /**
   * This function modifies the change event using transformations based on the session file (stored
   * in the Schema object). This includes column/table name changes and adding of synthetic Primary
   * Keys.
   */
  JsonNode transformChangeEventViaSessionFile(JsonNode changeEvent) {
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    String tableId = schema.getSrcToID().get(tableName).getName();

    // Convert table and column names in change event.
    changeEvent = convertTableAndColumnNames(changeEvent, tableName);

    // Add synthetic PK to change event.
    changeEvent = addSyntheticPKs(changeEvent, tableId);

    // Remove columns present in change event that were dropped in Spanner.
    changeEvent = removeDroppedColumns(changeEvent, tableId);

    // Add shard id to change event.
    changeEvent = populateShardId(changeEvent, tableId);

    return changeEvent;
  }

  JsonNode populateShardId(JsonNode changeEvent, String tableId) {
    if (!MYSQL_SOURCE_TYPE.equals(this.sourceType)
        || transformationContext.getSchemaToShardId() == null
        || transformationContext.getSchemaToShardId().isEmpty()) {
      return changeEvent; // Nothing to do
    }

    SpannerTable table = schema.getSpSchema().get(tableId);
    String shardIdColumn = table.getShardIdColumn();
    if (shardIdColumn == null) {
      return changeEvent;
    }
    SpannerColumnDefinition shardIdColDef = table.getColDefs().get(table.getShardIdColumn());
    if (shardIdColDef == null) {
      return changeEvent;
    }
    Map<String, String> schemaToShardId = transformationContext.getSchemaToShardId();
    String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
    String shardId = schemaToShardId.get(schemaName);
    ((ObjectNode) changeEvent).put(shardIdColDef.getName(), shardId);
    return changeEvent;
  }

  JsonNode convertTableAndColumnNames(JsonNode changeEvent, String tableName) {
    NameAndCols nameAndCols = schema.getToSpanner().get(tableName);
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

  JsonNode addSyntheticPKs(JsonNode changeEvent, String tableId) {
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    Map<String, SyntheticPKey> synthPks = schema.getSyntheticPks();
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
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    SourceTable srcTable = schema.getSrcSchema().get(tableId);
    Map<String, SourceColumnDefinition> srcCols = srcTable.getColDefs();
    for (String colId : srcTable.getColIds()) {
      // If spanner columns do not contain this column Id, drop from change event.
      if (!spCols.containsKey(colId)) {
        ((ObjectNode) changeEvent).remove(srcCols.get(colId).getName());
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
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }

  void outputWithFilterTag(ProcessContext c, FailsafeElement<String, String> changeEvent) {
    c.output(SpannerTransactionWriter.FILTERED_EVENT_TAG, changeEvent.getPayload());
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
