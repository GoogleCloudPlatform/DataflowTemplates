/*
 * Copyright (C) 2025 Google LLC
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

import static com.mongodb.client.model.Filters.eq;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.PubSubNotifiedDlqIO;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.datastream.sources.DataStreamIO;
import com.google.cloud.teleport.v2.templates.DataStreamMongoDBToFirestore.Options;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.transforms.CreateMongoDbChangeEventContextFn;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.transforms.MongoDbEventDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.transforms.ProcessChangeEventFn;
import com.google.cloud.teleport.v2.transforms.Utils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.mongodb.MongoClientSettings;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests DataStream data from GCS. The data is then transformed to JSON documents
 * and added to the target database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-mongodb-to-mongodb/README_Cloud_Datastream_MongoDB_to_MongoDB.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Datastream_MongoDB_to_Firestore",
    category = TemplateCategory.STREAMING,
    displayName = "Datastream to Firestore",
    description = {
      "The Datastream MongoDB to Firestore template is a streaming pipeline that reads <a"
          + " href=\"https://cloud.google.com/datastream/docs\">Datastream</a> events from a Cloud"
          + " Storage bucket and writes them to a Firestore with MongoDB compatibility database. It is intended for data"
          + " migration from Datastream sources to Firestore with MongoDB compatibility.\n",
      "Data consistency is guaranteed only at the end of migration when all data has been written"
          + " to the destination database. To store ordering information for each record written to"
          + " the destination database, this template creates an additional collection (called a"
          + " shadow collection) for each collection in the source database. This is used to ensure"
          + " consistency at the end of migration. By default the shadow collection is used only on"
          + " cdc events, it is configurable to be used on backfill events via setting"
          + " `useShadowTablesForBackfill` to true. The shadow collections by default uses prefix"
          + " `shadow_`, if it can cause collection name collision with the source database, please"
          + " configure that by setting `shadowCollectionPrefix`. The shadow collections are not"
          + " deleted after migration and can be used for validation purposes at the end of the"
          + " migration.\n",
      "The pipeline by default processes backfill events first with batch write, which is"
          + " optimized for performance, followed by cdc events. This is configurable via setting"
          + " `processBackfillFirst` to false to process backfill and cdc events together.\n",
      "Any errors that occur during operation are recorded in error queues. The error"
          + " queue is a Cloud Storage folder which stores all the Datastream events that had"
          + " encountered errors."
    },
    flexContainerName = "datastream-mongodb-to-firestore",
    optionsClass = Options.class)
public class DataStreamMongoDBToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamMongoDBToFirestore.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";
  public static final Set<String> MAPPER_IGNORE_FIELDS =
      new HashSet<String>(
          Arrays.asList(
              "_metadata_stream",
              "_metadata_schema",
              "_metadata_table",
              "_metadata_source",
              "_metadata_ssn",
              "_metadata_rs_id",
              "_metadata_tx_id",
              "_metadata_uuid",
              "_metadata_dlq_reconsumed",
              "_metadata_error",
              "_metadata_retry_count",
              "_metadata_timestamp",
              "_metadata_read_timestamp",
              "_metadata_read_method",
              "_metadata_deleted",
              "_metadata_primary_keys",
              "_metadata_log_file",
              "_metadata_log_position",
              "_metadata_dataflow_timestamp",
              "data",
              "_metadata_timestamp_seconds",
              "_metadata_timestamp_nanos"));

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends StreamingOptions, DataflowPipelineWorkerPoolOptions {
    @TemplateParameter.Text(
        order = 10,
        optional = true,
        description = "Shadow collection prefix",
        helpText = "The prefix used to name shadow collections. Default: `shadow_`.")
    @Default.String(DatastreamConstants.DEFAULT_SHADOW_COLLECTION_PREFIX)
    String getShadowCollectionPrefix();

    void setShadowCollectionPrefix(String value);

    @TemplateParameter.Boolean(
        order = 18,
        optional = true,
        description = "Process backfill events before CDC events",
        helpText =
            "When true, all backfill events are processed before any CDC events, otherwise the backfill and cdc events are processed together. Default: false")
    @Default.Boolean(false)
    Boolean getProcessBackfillFirst();

    void setProcessBackfillFirst(Boolean value);

    @TemplateParameter.Boolean(
        order = 19,
        optional = true,
        description = "Use shadow tables for backfill events",
        helpText =
            "When false, backfill events are processed without shadow tables. This only takes effect when processBackfillFirst is set to true. Default: false")
    @Default.Boolean(false)
    Boolean getUseShadowTablesForBackfill();

    void setUseShadowTablesForBackfill(Boolean value);

    @TemplateParameter.Enum(
        order = 20,
        optional = true,
        description = "Run mode - currently supported are : regular or retryDLQ",
        enumOptions = {@TemplateEnumOption("regular"), @TemplateEnumOption("retryDLQ")},
        helpText = "This is the run mode type, whether regular or with retryDLQ.")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "Cloud Storage Input File(s)",
        groupName = "Source",
        helpText = "Path of the file pattern glob to read from.",
        example = "gs://your-bucket/path/")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {@TemplateEnumOption("avro"), @TemplateEnumOption("json")},
        optional = false,
        description = "The GCS input format avro/json",
        helpText = "The file format of the desired input files. Can be avro or json.")
    @Default.String("avro")
    String getInputFileFormat();

    void setInputFileFormat(String value);

    @TemplateParameter.PubsubSubscription(
        order = 24,
        optional = true,
        description =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode. For the name, use the format"
                + " `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`. When set, the"
                + " deadLetterQueueDirectory and dlqRetryMinutes are ignored.")
    String getDlqGcsPubSubSubscription();

    void setDlqGcsPubSubSubscription(String value);

    @TemplateParameter.PubsubSubscription(
        order = 8,
        optional = true,
        description = "The Pub/Sub subscription being used in a Cloud Storage notification policy.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy. For the name,"
                + " use the format `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.")
    String getGcsPubSubSubscription();

    void setGcsPubSubSubscription(String value);

    @TemplateParameter.Integer(
        order = 22,
        optional = true,
        description = "Directory watch duration in minutes. Default: 10 minutes",
        helpText =
            "The Duration for which the pipeline should keep polling a directory in GCS. Datastream"
                + "output files are arranged in a directory structure which depicts the timestamp "
                + "of the event grouped by minutes. This parameter should be approximately equal to"
                + "maximum delay which could occur between event occurring in source database and "
                + "the same event being written to GCS by Datastream. 99.9 percentile = 10 minutes")
    @Default.Integer(10)
    Integer getDirectoryWatchDurationInMinutes();

    void setDirectoryWatchDurationInMinutes(Integer value);

    @TemplateParameter.Text(
        order = 23,
        groupName = "Source",
        optional = true,
        description = "Datastream stream name.",
        helpText =
            "The name or template for the stream to poll for schema information and source type.")
    String getStreamName();

    void setStreamName(String value);

    @TemplateParameter.DateTime(
        order = 5,
        optional = true,
        description =
            "The starting DateTime used to fetch from Cloud Storage "
                + "(https://tools.ietf.org/html/rfc3339).",
        helpText =
            "The starting DateTime used to fetch from Cloud Storage "
                + "(https://tools.ietf.org/html/rfc3339).")
    @Default.String("1970-01-01T00:00:00.00Z")
    String getRfcStartDateTime();

    void setRfcStartDateTime(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The file path used when storing the error queue output. "
                + "The default file path is a directory under the Dataflow job's temp location.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 15,
        optional = true,
        description = "Dead letter queue retry minutes",
        helpText = "The number of minutes between dead letter queue retries. Defaults to `10`.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);

    @TemplateParameter.Integer(
        order = 16,
        optional = true,
        description = "Dead letter queue maximum retry count",
        helpText =
            "The max number of times temporary errors can be retried through DLQ. Defaults to `500`.")
    @Default.Integer(500)
    Integer getDlqMaxRetryCount();

    void setDlqMaxRetryCount(Integer value);

    @TemplateParameter.Integer(
        order = 6,
        optional = true,
        description = "File read concurrency",
        helpText = "The number of concurrent DataStream files to read.")
    @Default.Integer(10)
    Integer getFileReadConcurrency();

    void setFileReadConcurrency(Integer value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 7,
        description = "Connection URI for the target project",
        helpText =
            "URI to connect to the target project. It should start with either "
                + "'mongodb://' or 'mongodb+srv://'. If OIDC authentication mechanism is used and "
                + "no TOKEN_RESOURCE is provided, it will automatically use FIRESTORE.")
    String getConnectionUri();

    void setConnectionUri(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 8,
        description = "Database name",
        helpText = "The database to write to.",
        example = "(default)")
    @Default.String("(default)")
    String getDatabaseName();

    void setDatabaseName(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 9,
        description = "Database collection filter (optional)",
        helpText =
            "If specified, only replicate this collection. If not specified, replicate all collections.",
        example = "my-collection",
        optional = true)
    String getDatabaseCollection();

    void setDatabaseCollection(String value);

    @TemplateParameter.Integer(
        order = 11,
        optional = true,
        description = "Batch size",
        helpText = "The batch size for writing to Database.")
    @Default.Integer(500)
    Integer getBatchSize();

    void setBatchSize(Integer value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting DataStream to MongoDB pipeline...");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    LOG.info("Pipeline options parsed and validated");

    options.setStreaming(true);
    LOG.info("Pipeline set to streaming mode");

    validateOptions(options);
    LOG.info("Options validated successfully");

    run(options);
  }

  private static void validateOptions(Options options) {
    String inputFileFormat = options.getInputFileFormat();
    if (!(inputFileFormat.equals(AVRO_SUFFIX) || inputFileFormat.equals(JSON_SUFFIX))) {
      throw new IllegalArgumentException(
          "Input file format must be one of: avro, json or left empty - found " + inputFileFormat);
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * <p>This pipeline processes all events (backfill/CDC) together, ordered by the timestamp field
   * from the datastream records. Shadow collections are used to track event ordering and prevent
   * duplicate processing.
   *
   * @param options The execution parameters to the pipeline.
   */
  public static void run(Options options) {
    try {
      LOG.info(
          "Starting pipeline execution with options: inputFilePattern={}, fileType={}, databaseName={}",
          options.getInputFilePattern(),
          options.getInputFileFormat(),
          options.getDatabaseName());

      // Decode the connection string
      String connectionString = options.getConnectionUri();
      if (!connectionString.startsWith("mongodb://")
          && !connectionString.startsWith("mongodb+srv://")) {
        LOG.error(
            "Invalid URL: {}, Must be in pattern of 'mongodb://<host1>:<port1>,<host2>:<port2>/database?options', or 'mongodb+srv://<host>/database?options'",
            connectionString);
        throw new IllegalArgumentException("Invalid connectionUri: " + connectionString);
      }
      if (connectionString.contains("MONGODB-OIDC")
          && !connectionString.contains("TOKEN_RESOURCE")) {
        connectionString += ",TOKEN_RESOURCE:FIRESTORE";
      }
      LOG.info("Creating MongoDB client with connection string: {}", connectionString);
      MongoClientSettings settings =
          MongoClientSettings.builder()
              .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
              .applyToSocketSettings(
                  builder -> {
                    // How long the driver will wait to establish a connection
                    builder.connectTimeout(60, TimeUnit.SECONDS);
                    builder.readTimeout(60, TimeUnit.SECONDS); // Example: 60 seconds
                  })
              .applyToClusterSettings(
                  builder -> builder.serverSelectionTimeout(10, TimeUnit.MINUTES))
              .uuidRepresentation(UuidRepresentation.STANDARD)
              .build();
      MongoClient mongoClient = MongoClients.create(settings);
      LOG.info("MongoDB client created successfully");

      // Choose processing mode based on options
      LOG.info("Starting pipeline execution");
      PipelineResult result;
      if (options.getProcessBackfillFirst()) {
        LOG.info("Using backfill-first processing mode");
        runWithBackfillFirst(options, connectionString);
      } else {
        LOG.info("Using unified processing mode");
        runAllEventsTogether(options, connectionString);
      }

      mongoClient.close();
      LOG.info("MongoDB client closed");
    } catch (Exception e) {
      LOG.error("Failed to run pipeline: {}", e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Runs the pipeline with backfill events processed before CDC events.
   *
   * <p>This pipeline first processes all backfill events, then processes CDC events. This ensures
   * that the initial state of the database is established before any changes are applied. Failures
   * in backfill will be sent over to dlq and be processed with conflict resolving.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult runWithBackfillFirst(Options options, String connectionString) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Convert json strings to MongoDbChangeEventContext
     *   3) Split the MongoDbChangeEventContext into backfill and cdc events
     *   4) Process backfill events with bulk writes, failed backfill will be sent to dlq and later processed with transactions and conflict resolving.
     *   5) Process the cdc events with transactions
     */
    LOG.info("Creating pipeline with backfill-first processing");
    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Building Dead Letter Queue manager");
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    // Stage 1: Ingest data from GCS
    LOG.info("Ingesting data from GCS");
    PCollection<FailsafeElement<String, String>> jsonRecords =
        ingestAndNormalizeJson(options, dlqManager, pipeline)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Stage 2: Create MongoDbChangeEventContext objects
    LOG.info("Creating MongoDbChangeEventContext objects");
    PCollectionTuple changeEventContexts =
        jsonRecords.apply(
            "Create MongoDbChangeEventContext objects",
            ParDo.of(new CreateMongoDbChangeEventContextFn(options.getShadowCollectionPrefix()))
                .withOutputTags(
                    CreateMongoDbChangeEventContextFn.successfulCreationTag,
                    TupleTagList.of(CreateMongoDbChangeEventContextFn.failedCreationTag)));

    // Set coders
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.successfulCreationTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.failedCreationTag)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Handle failed creation with DLQ
    writeFailedJsonToDlq(
        options,
        changeEventContexts,
        dlqManager,
        CreateMongoDbChangeEventContextFn.failedCreationTag);

    // Stage 3: Split events into backfill and CDC streams
    LOG.info("Splitting events into backfill and CDC streams");
    PCollectionTuple splitEvents =
        changeEventContexts
            .get(CreateMongoDbChangeEventContextFn.successfulCreationTag)
            .apply(
                "Split Backfill and CDC",
                ParDo.of(new SplitBackfillAndCdcEventsFn())
                    .withOutputTags(
                        SplitBackfillAndCdcEventsFn.backfillTag,
                        TupleTagList.of(SplitBackfillAndCdcEventsFn.cdcTag)));

    // Set coders for split events
    splitEvents
        .get(SplitBackfillAndCdcEventsFn.backfillTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
    splitEvents
        .get(SplitBackfillAndCdcEventsFn.cdcTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));

    // Stage 4: Process backfill events
    LOG.info("Processing backfill events");
    PCollectionTuple backfillResult;
    if (options.getUseShadowTablesForBackfill()) {
      // Use shadow tables for backfill (same as CDC processing)
      backfillResult =
          splitEvents
              .get(SplitBackfillAndCdcEventsFn.backfillTag)
              .apply(
                  "Process Backfill with Shadow Tables",
                  ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                      .withOutputTags(
                          ProcessChangeEventFn.successfulWriteTag,
                          TupleTagList.of(ProcessChangeEventFn.failedWriteTag)));
    } else {
      // Process backfill without shadow tables
      backfillResult =
          splitEvents
              .get(SplitBackfillAndCdcEventsFn.backfillTag)
              .apply(
                  "Process Backfill without Shadow Tables",
                  ParDo.of(
                          new ProcessBackfillEventFn(
                              connectionString, options.getDatabaseName(), options.getBatchSize()))
                      .withOutputTags(
                          ProcessBackfillEventFn.successfulWriteTag,
                          TupleTagList.of(ProcessBackfillEventFn.failedWriteTag)));
    }

    // Set coders for backfill results
    backfillResult
        .get(
            options.getUseShadowTablesForBackfill()
                ? ProcessChangeEventFn.successfulWriteTag
                : ProcessBackfillEventFn.successfulWriteTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
    backfillResult
        .get(
            options.getUseShadowTablesForBackfill()
                ? ProcessChangeEventFn.failedWriteTag
                : ProcessBackfillEventFn.failedWriteTag)
        .setCoder(
            FailsafeElementCoder.of(
                SerializableCoder.of(MongoDbChangeEventContext.class),
                SerializableCoder.of(MongoDbChangeEventContext.class)));

    // Handle failed backfill writes with DLQ
    writeFailedEventsToDlq(
        options,
        backfillResult,
        dlqManager,
        options.getUseShadowTablesForBackfill()
            ? ProcessChangeEventFn.failedWriteTag
            : ProcessBackfillEventFn.failedWriteTag);

    // Stage 5: Process CDC events
    LOG.info("Processing CDC events");
    PCollectionTuple cdcResult =
        splitEvents
            .get(SplitBackfillAndCdcEventsFn.cdcTag)
            .apply(
                "Process CDC Events",
                ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                    .withOutputTags(
                        ProcessChangeEventFn.successfulWriteTag,
                        TupleTagList.of(ProcessChangeEventFn.failedWriteTag)));

    // Set coders for CDC results
    cdcResult
        .get(ProcessChangeEventFn.successfulWriteTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
    cdcResult
        .get(ProcessChangeEventFn.failedWriteTag)
        .setCoder(
            FailsafeElementCoder.of(
                SerializableCoder.of(MongoDbChangeEventContext.class),
                SerializableCoder.of(MongoDbChangeEventContext.class)));

    // Handle failed CDC writes with DLQ
    writeFailedEventsToDlq(options, cdcResult, dlqManager, ProcessChangeEventFn.failedWriteTag);

    // Execute the pipeline
    LOG.info("Executing pipeline");
    return pipeline.run();
  }

  /**
   * Runs the pipeline with all events processed together.
   *
   * <p>This pipeline processes both backfill and CDC events in a unified flow, ordered primarily by
   * their timestamps. Events with the same timestamp are ordered by type, with backfill events
   * processed before CDC events. Shadow collections are used to track event ordering and prevent
   * duplicate processing.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult runAllEventsTogether(Options options, String connectionString) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Convert json strings to MongoDbChangeEvents
     *   3) Write the change events with transactions
     */

    LOG.info("Creating pipeline");
    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Building Dead Letter Queue manager");
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    /*
     * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)
     */
    LOG.info("Stage 1: Starting ingestion of data from GCS");
    PCollection<FailsafeElement<String, String>> jsonRecords =
        ingestAndNormalizeJson(options, dlqManager, pipeline)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    LOG.info("Stage 1: Completed ingestion of data from GCS");

    /*
     * Stage 2: Create MongoDbChangeEventContext objects with error handling
     */
    LOG.info("Stage 2: Creating MongoDbChangeEventContext objects");
    PCollectionTuple changeEventContexts =
        jsonRecords.apply(
            "Create MongoDbChangeEventContext objects",
            ParDo.of(new CreateMongoDbChangeEventContextFn(options.getShadowCollectionPrefix()))
                .withOutputTags(
                    CreateMongoDbChangeEventContextFn.successfulCreationTag,
                    TupleTagList.of(CreateMongoDbChangeEventContextFn.failedCreationTag)));

    /* Set coder for successful creation */
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.successfulCreationTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));

    /* Set coder for failed creation */
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.failedCreationTag)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Handle failed creation with DLQ
    LOG.info("Setting up DLQ handling for failed event creation");
    writeFailedJsonToDlq(
        options,
        changeEventContexts,
        dlqManager,
        CreateMongoDbChangeEventContextFn.failedCreationTag);

    /* Stage 3: Iterate through the success events and write with transactions */
    LOG.info("Stage 3: Processing change events and writing to the destination database");
    PCollectionTuple writeResult =
        changeEventContexts
            .get(CreateMongoDbChangeEventContextFn.successfulCreationTag)
            .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class))
            .apply(
                "Transactional write events",
                ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                    .withOutputTags(
                        ProcessChangeEventFn.successfulWriteTag,
                        TupleTagList.of(ProcessChangeEventFn.failedWriteTag)));

    /* Set coder for successful writes */
    writeResult
        .get(ProcessChangeEventFn.successfulWriteTag)
        .setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));

    /* Set coder for failed writes */
    writeResult
        .get(ProcessChangeEventFn.failedWriteTag)
        .setCoder(
            FailsafeElementCoder.of(
                SerializableCoder.of(MongoDbChangeEventContext.class),
                SerializableCoder.of(MongoDbChangeEventContext.class)));

    /* Handle failed writes with DLQ */
    LOG.info("Setting up DLQ handling for failed writes");
    writeFailedEventsToDlq(options, writeResult, dlqManager, ProcessChangeEventFn.failedWriteTag);

    // Execute the pipeline and return the result.
    LOG.info("Executing pipeline");
    return pipeline.run();
  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    LOG.info("Building Dead Letter Queue manager");
    String tempLocation = null;
    try {
      tempLocation = options.as(DataflowPipelineOptions.class).getTempLocation();
      if (tempLocation != null) {
        tempLocation = tempLocation.endsWith("/") ? tempLocation : tempLocation + "/";
        LOG.info("Using temp location from pipeline options: {}", tempLocation);
      } else {
        // If tempLocation is null, use a default location
        tempLocation = "/tmp/";
        LOG.warn("TempLocation is null, using default location: {}", tempLocation);
      }
    } catch (Exception e) {
      // If we can't get the temp location, use a default
      tempLocation = "/tmp/";
      LOG.warn("Error getting tempLocation, using default location: {}", tempLocation, e);
    }

    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();
    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    options.setDeadLetterQueueDirectory(dlqDirectory);

    if ("regular".equals(options.getRunMode())) {
      LOG.info(
          "Creating DLQ manager in regular mode with max retry count: {}",
          options.getDlqMaxRetryCount());
      return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetryCount());
    } else {
      String retryDlqUri = FileSystems.matchNewResource(dlqDirectory, true).toString();
      LOG.info("Creating DLQ manager in retry mode with retry directory: {}", retryDlqUri);
      return DeadLetterQueueManager.create(
          dlqDirectory, retryDlqUri, options.getDlqMaxRetryCount());
    }
  }

  /** Read from input path and dlq to collect objects to process. */
  private static PCollection<FailsafeElement<String, String>> ingestAndNormalizeJson(
      Options options, DeadLetterQueueManager dlqManager, Pipeline pipeline) {
    LOG.info("Starting ingestion and normalization of JSON data");
    PCollection<FailsafeElement<String, String>> jsonRecords;
    // Elements sent to the Dead Letter Queue are to be reconsumed.
    // A DLQManager is to be created using PipelineOptions, and it is in charge
    // of building pieces of the DLQ.
    PCollectionTuple reconsumedElements;
    boolean isRegularMode = "regular".equals(options.getRunMode());

    LOG.info("Setting up DLQ reconsumption, mode: {}", isRegularMode ? "regular" : "retry");
    if (isRegularMode && (!Strings.isNullOrEmpty(options.getDlqGcsPubSubSubscription()))) {
      LOG.info(
          "Using PubSub notification for DLQ reconsumption with subscription: {}",
          options.getDlqGcsPubSubSubscription());
      reconsumedElements =
          dlqManager.getReconsumerDataTransformForFiles(
              pipeline.apply(
                  "Read retry from PubSub",
                  new PubSubNotifiedDlqIO(
                      options.getDlqGcsPubSubSubscription(),
                      // file paths to ignore when re-consuming for retry
                      new ArrayList<String>(
                          Arrays.asList("/severe/", "/tmp_retry", "/tmp_severe/", ".temp")))));
    } else {
      LOG.info(
          "Using periodic polling for DLQ reconsumption with retry minutes: {}",
          options.getDlqRetryMinutes());
      reconsumedElements =
          dlqManager.getReconsumerDataTransform(
              pipeline.apply(
                  "Periodically polling from DLQ",
                  dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    }

    LOG.info("Processing retryable errors from DLQ");
    PCollection<FailsafeElement<String, String>> dlqJsonRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Write non-retryable errors to DLQ
    reconsumedElements
        .get(DeadLetterQueueManager.PERMANENT_ERRORS)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "Write new non-retryable errors To DLQ",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp_severe/")
                .setIncludePaneInfo(true)
                .build());

    if (isRegularMode) {
      LOG.info("Regular Datastream flow - reading from GCS: {}", options.getInputFilePattern());
      PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
          pipeline.apply(
              "Read from GCS bucket",
              new DataStreamIO(
                      options.getStreamName(),
                      options.getInputFilePattern(),
                      options.getInputFileFormat(),
                      options.getGcsPubSubSubscription(),
                      options.getRfcStartDateTime())
                  .withFileReadConcurrency(options.getFileReadConcurrency())
                  .withoutDatastreamRecordsReshuffle()
                  .withDirectoryWatchDuration(
                      Duration.standardMinutes(options.getDirectoryWatchDurationInMinutes())));
      LOG.info(
          "DataStreamIO configured with fileReadConcurrency: {}, directoryWatchDuration: {} minutes",
          options.getFileReadConcurrency(),
          options.getDirectoryWatchDurationInMinutes());

      int maxNumWorkers = options.getMaxNumWorkers() != 0 ? options.getMaxNumWorkers() : 1;
      LOG.info("Flattening and reshuffling records with maxNumWorkers: {}", maxNumWorkers);
      jsonRecords =
          PCollectionList.of(datastreamJsonRecords)
              .and(dlqJsonRecords)
              .apply("Flattern collections", Flatten.pCollections())
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply(
                  "Reshuffle records",
                  Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                      .withNumBuckets(maxNumWorkers * DatastreamConstants.MAX_DOFN_PER_WORKER));
    } else {
      LOG.info("DLQ retry flow - processing only DLQ records");
      jsonRecords =
          PCollectionList.of(dlqJsonRecords)
              .apply("Flattern collections", Flatten.pCollections())
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply("Reshuffle records", Reshuffle.viaRandomKey());
    }
    LOG.info("Completed ingestion and normalization of JSON data");
    return jsonRecords;
  }

  private static void writeFailedJsonToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<String, String>> failedTag) {
    LOG.info("Setting up DLQ for failed JSON processing");
    // Write failed writes to DLQ
    results
        .get(failedTag)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "DLQ: Write Retryable Json Failures to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write Failed Json To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_non_retry_json/")
                .setIncludePaneInfo(true)
                .build());
    LOG.info("DLQ setup completed for failed JSON processing");
  }

  private static void writeFailedEventsToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> failedTag) {
    LOG.info("Setting up DLQ for failed MongoDB event processing");
    // Write failed writes to DLQ
    results
        .get(failedTag)
        .setCoder(
            FailsafeElementCoder.of(
                SerializableCoder.of(MongoDbChangeEventContext.class),
                SerializableCoder.of(MongoDbChangeEventContext.class)))
        .apply(
            "DLQ: Write Retryable Events Failures to GCS",
            MapElements.via(new MongoDbEventDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write Events Failures To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getRetryDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_retry_mongo_event/")
                .setIncludePaneInfo(true)
                .build());
    LOG.info("DLQ setup completed for failed MongoDB event processing");
  }

  /** DoFn to split events into backfill and CDC streams. */
  public static class SplitBackfillAndCdcEventsFn
      extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

    private static final Logger LOG = LoggerFactory.getLogger(SplitBackfillAndCdcEventsFn.class);

    public static TupleTag<MongoDbChangeEventContext> backfillTag = new TupleTag<>("backfill");
    public static TupleTag<MongoDbChangeEventContext> cdcTag = new TupleTag<>("cdc");

    @ProcessElement
    public void processElement(ProcessContext c, MultiOutputReceiver out) {
      MongoDbChangeEventContext event = c.element();

      if (isNonDlqBackfillEvent(event)) {
        LOG.debug("Classified event as backfill for document ID: {}", event.getDocumentId());
        out.get(backfillTag).output(event);
      } else {
        LOG.debug("Classified event as CDC for document ID: {}", event.getDocumentId());
        out.get(cdcTag).output(event);
      }
    }

    private boolean isNonDlqBackfillEvent(MongoDbChangeEventContext event) {
      if (event.getIsDlqReconsumed()) {
        return false;
      }
      JsonNode jsonNode = event.getChangeEvent();

      // Check for CDC-specific fields
      boolean hasCdcFields =
          jsonNode.has("_metadata_log_file")
              || jsonNode.has("_metadata_log_position")
              || jsonNode.has("_metadata_scn")
              || jsonNode.has("_metadata_ssn")
              || jsonNode.has("_metadata_rs_id");

      // Check for change type
      String changeType = null;
      if (jsonNode.has(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)) {
        changeType = jsonNode.get(DatastreamConstants.EVENT_CHANGE_TYPE_KEY).asText();
      }

      // If it has CDC fields or a specific change type (not READ), it's a CDC event
      return !hasCdcFields && (changeType == null || "READ".equals(changeType));
    }
  }

  /** DoFn to process backfill events using MongoDB bulk writes. */
  public static class ProcessBackfillEventFn
      extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessBackfillEventFn.class);

    public static TupleTag<MongoDbChangeEventContext> successfulWriteTag =
        new TupleTag<>("backfillSuccessfulWrite");
    public static TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        failedWriteTag = new TupleTag<>("backfillFailedWrite");

    private final String connectionString;
    private final String targetDatabaseName;
    private final int batchSize;

    // Maps to store buffered operations by collection
    private transient Map<String, List<MongoDbChangeEventContext>> bufferedEvents;
    private transient Map<String, MongoCollection<Document>> collectionMap;
    private transient MongoClient client;

    public ProcessBackfillEventFn(String connectionString, String databaseName, int batchSize) {
      this.connectionString = connectionString;
      this.targetDatabaseName = databaseName;
      this.batchSize = batchSize;
    }

    @Setup
    public void setup() {
      LOG.info("Setting up MongoDB client for backfill processing with batch size: {}", batchSize);
      MongoClientSettings settings =
          MongoClientSettings.builder()
              .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
              .applyToSocketSettings(
                  builder -> {
                    // How long the driver will wait to establish a connection
                    builder.connectTimeout(60, TimeUnit.SECONDS);
                    builder.readTimeout(60, TimeUnit.SECONDS); // Example: 60 seconds
                  })
              .applyToClusterSettings(
                  builder -> builder.serverSelectionTimeout(10, TimeUnit.MINUTES))
              .uuidRepresentation(UuidRepresentation.STANDARD)
              .build();
      client = MongoClients.create(settings);
      bufferedEvents = new HashMap<>();
      collectionMap = new HashMap<>();
    }

    @StartBundle
    public void startBundle() {
      LOG.debug("Starting new bundle for backfill processing");
      bufferedEvents.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext context, MultiOutputReceiver out) {
      MongoDbChangeEventContext element = context.element();
      String collectionName = element.getDataCollection();

      // Buffer the event
      if (!bufferedEvents.containsKey(collectionName)) {
        LOG.debug("Creating new buffer for collection: {}", collectionName);
        bufferedEvents.put(collectionName, new ArrayList<>());

        // Initialize collection reference if needed
        if (!collectionMap.containsKey(collectionName)) {
          MongoDatabase database = client.getDatabase(targetDatabaseName);
          collectionMap.put(collectionName, database.getCollection(collectionName));
        }
      }

      bufferedEvents.get(collectionName).add(element);

      // If we've reached batch size for this collection, process the batch
      if (bufferedEvents.get(collectionName).size() >= batchSize) {
        LOG.debug(
            "Batch size reached for collection {}, processing {} events",
            collectionName,
            bufferedEvents.get(collectionName).size());
        processBatch(collectionName, out);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
      // Process any remaining batches
      for (String collectionName : bufferedEvents.keySet()) {
        if (!bufferedEvents.get(collectionName).isEmpty()) {
          LOG.debug(
              "Processing remaining {} events for collection {} at bundle finish",
              bufferedEvents.get(collectionName).size(),
              collectionName);
          processBatchFinish(collectionName, context);
        }
      }
    }

    private void processBatch(String collectionName, MultiOutputReceiver out) {
      List<MongoDbChangeEventContext> events = bufferedEvents.get(collectionName);
      MongoCollection<Document> collection = collectionMap.get(collectionName);

      if (events.isEmpty()) {
        return;
      }

      try {
        // Create bulk operation
        List<WriteModel<Document>> bulkOperations = new ArrayList<>(events.size());

        // Add operations to bulk
        for (MongoDbChangeEventContext event : events) {
          Object docId = event.getDocumentId();
          Bson lookupById = eq("_id", docId);

          if (event.isDeleteEvent()) {
            // Add delete operation
            bulkOperations.add(new DeleteOneModel<>(lookupById));
          } else {
            // Add upsert operation
            bulkOperations.add(
                new ReplaceOneModel<>(
                    lookupById,
                    Utils.jsonToDocument(event.getDataAsJsonString(), event.getDocumentId()),
                    new ReplaceOptions().upsert(true)));
          }
        }

        // Execute bulk write
        BulkWriteResult result = collection.bulkWrite(bulkOperations);
        LOG.debug(
            "Bulk write completed for collection {}: {} inserts/updates, {} deletes",
            collectionName,
            result.getInsertedCount() + result.getModifiedCount() + result.getUpserts().size(),
            result.getDeletedCount());

        // Output successful events
        for (MongoDbChangeEventContext event : events) {
          out.get(successfulWriteTag).output(event);
        }
      } catch (Exception e) {
        LOG.error(
            "Error processing backfill batch for collection {}: {}",
            collectionName,
            e.getMessage(),
            e);

        // On error, output all events as failed
        for (MongoDbChangeEventContext event : events) {
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Arrays.deepToString(e.getStackTrace()));
          out.get(failedWriteTag).output(failedElement);
        }
      }

      // Clear the processed batch
      events.clear();
    }

    private void processBatchFinish(String collectionName, FinishBundleContext context) {
      List<MongoDbChangeEventContext> events = bufferedEvents.get(collectionName);
      MongoCollection<Document> collection = collectionMap.get(collectionName);

      if (events.isEmpty()) {
        return;
      }

      try {
        // Create bulk operation
        List<WriteModel<Document>> bulkOperations = new ArrayList<>(events.size());

        // Add operations to bulk
        for (MongoDbChangeEventContext event : events) {
          Object docId = event.getDocumentId();
          Bson lookupById = eq("_id", docId);

          if (event.isDeleteEvent()) {
            // Add delete operation
            bulkOperations.add(new DeleteOneModel<>(lookupById));
          } else {
            // Add upsert operation
            bulkOperations.add(
                new ReplaceOneModel<>(
                    lookupById,
                    Utils.jsonToDocument(event.getDataAsJsonString(), event.getDocumentId()),
                    new ReplaceOptions().upsert(true)));
          }
        }

        // Execute bulk write
        BulkWriteResult result = collection.bulkWrite(bulkOperations);
        LOG.debug(
            "Bulk write completed for collection {}: {} inserts/updates, {} deletes",
            collectionName,
            result.getInsertedCount() + result.getModifiedCount() + result.getUpserts().size(),
            result.getDeletedCount());

        // Output successful events
        for (MongoDbChangeEventContext event : events) {
          context.output(successfulWriteTag, event, Instant.now(), GlobalWindow.INSTANCE);
        }
      } catch (Exception e) {
        LOG.error(
            "Error processing backfill batch for collection {}: {}",
            collectionName,
            e.getMessage(),
            e);

        // On error, output all events as failed
        for (MongoDbChangeEventContext event : events) {
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Arrays.deepToString(e.getStackTrace()));
          context.output(failedWriteTag, failedElement, Instant.now(), GlobalWindow.INSTANCE);
        }
      }

      // Clear the processed batch
      events.clear();
    }

    @Teardown
    public void teardown() {
      if (client != null) {
        LOG.info("Closing MongoDB client for backfill processing");
        client.close();
        client = null;
      }
    }
  }
}
