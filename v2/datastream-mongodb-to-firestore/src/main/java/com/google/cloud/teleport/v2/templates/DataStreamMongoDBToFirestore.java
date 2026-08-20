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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.transforms.MongoDbBulkTransforms;
import com.google.cloud.teleport.v2.transforms.MongoDbChangeEventContextCoder;
import com.google.cloud.teleport.v2.transforms.MongoDbEventDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.transforms.ProcessChangeEventFn;
import com.google.cloud.teleport.v2.transforms.StatefulDeduplicationFn;
import com.google.cloud.teleport.v2.transforms.TimestampSortKey;
import com.google.cloud.teleport.v2.transforms.TimestampSortKeyCoder;
import com.google.cloud.teleport.v2.transforms.Utils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.bson.Document;
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
          + " Storage bucket and writes them to a Firestore with MongoDB compatibility database. It"
          + " is intended for data migration from Datastream sources to Firestore with MongoDB"
          + " compatibility.\n",
      "By default, the template runs in high-throughput shadowless mode without shadow collections"
          + " or distributed multi-document transactions. When legacy mode is explicitly selected by"
          + " setting `useShadowTables` to true, the template creates an additional shadow collection"
          + " for each collection to track event ordering.\n",
      "Any errors that occur during operation are recorded in error queues. The error"
          + " queue is a Cloud Storage folder which stores all the Datastream events that had"
          + " encountered errors."
    },
    flexContainerName = "datastream-mongodb-to-firestore",
    optionsClass = Options.class)
public class DataStreamMongoDBToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamMongoDBToFirestore.class);
  static final TupleTag<FailsafeElement<String, String>> UDF_SUCCESS_TAG = new TupleTag<>();
  static final TupleTag<FailsafeElement<String, String>> UDF_FAILURE_TAG = new TupleTag<>();
  static final TupleTag<FailsafeElement<String, String>> PREPARE_FAILURE_TAG = new TupleTag<>();
  static final TupleTag<FailsafeElement<String, String>> RESTORE_FAILURE_TAG = new TupleTag<>();
  static final TupleTag<FailsafeElement<String, String>> BYPASS_UDF_TAG = new TupleTag<>();
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";
  public static final Set<String> MAPPER_IGNORE_FIELDS = DatastreamConstants.MAPPER_IGNORE_FIELDS;

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends StreamingOptions,
          DataflowPipelineWorkerPoolOptions,
          JavascriptTextTransformerOptions {

    @TemplateParameter.Boolean(
        order = 10,
        optional = true,
        description = "Use shadow tables for tracking event ordering",
        helpText =
            "When false (default), runs in high-throughput shadowless mode without shadow"
                + " collections.")
    @Default.Boolean(false)
    Boolean getUseShadowTables();

    void setUseShadowTables(Boolean value);

    @TemplateParameter.Integer(
        order = 11,
        optional = true,
        description = "Batch size for bulk database writes",
        helpText =
            "Number of documents per bulkWrite RPC. For Firestore MongoDB compatibility, max 500."
                + " Default: 500.")
    @Default.Integer(500)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @TemplateParameter.Integer(
        order = 13,
        optional = true,
        description = "Maximum concurrent asynchronous writes per worker",
        helpText =
            "Maximum concurrent in-flight bulk write operations per worker thread pool. Default: 10.")
    @Default.Integer(10)
    Integer getMaxConcurrentAsyncWrites();

    void setMaxConcurrentAsyncWrites(Integer value);

    @TemplateParameter.Integer(
        order = 14,
        optional = true,
        description = "Initial write rate per worker (docs/sec)",
        helpText =
            "Initial maximum write rate per worker during warm-up. Set <= 0 to disable. Default: 5000.")
    @Default.Integer(5000)
    Integer getInitialWriteRatePerWorker();

    void setInitialWriteRatePerWorker(Integer value);

    @TemplateParameter.Integer(
        order = 15,
        optional = true,
        description = "Write rate ramp-up duration in minutes",
        helpText =
            "Duration in minutes over which the write rate ramps up to target throughput. Default: 5.")
    @Default.Integer(5)
    Integer getWriteRateRampUpMinutes();

    void setWriteRateRampUpMinutes(Integer value);

    @TemplateParameter.Integer(
        order = 16,
        optional = true,
        description = "Max write rate per worker after ramp-up",
        helpText = "Target maximum write rate per worker after completing ramp-up. Default: 25000.")
    @Default.Integer(25000)
    Integer getMaxWriteRatePerWorker();

    void setMaxWriteRatePerWorker(Integer value);

    @TemplateParameter.Text(
        order = 17,
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
            "When true, all backfill events are processed before any CDC events, otherwise the"
                + " backfill and cdc events are processed together. Default: false")
    @Default.Boolean(false)
    Boolean getProcessBackfillFirst();

    void setProcessBackfillFirst(Boolean value);

    @TemplateParameter.Boolean(
        order = 19,
        optional = true,
        description = "Use shadow tables for backfill events",
        helpText =
            "When false, backfill events are processed without shadow tables. This only takes"
                + " effect when processBackfillFirst is set to true. Default: false")
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
            "The Pub/Sub subscription being used in a Cloud Storage notification policy. For the"
                + " name, use the format"
                + " `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`.")
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
            "The max number of times temporary errors can be retried through DLQ. Defaults to"
                + " `500`.")
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
            "If specified, only replicate this collection. If not specified, replicate all"
                + " collections.",
        example = "my-collection",
        optional = true)
    String getDatabaseCollection();

    void setDatabaseCollection(String value);
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

  public static void validateOptions(Options options) {
    String connectionUri = options.getConnectionUri();
    if (connectionUri == null || connectionUri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Connection URI (connectionUri) must be specified and non-empty. "
              + "Expected 'mongodb://...' or 'mongodb+srv://...'");
    }
    if (!connectionUri.startsWith("mongodb://") && !connectionUri.startsWith("mongodb+srv://")) {
      throw new IllegalArgumentException(
          "Invalid connectionUri: "
              + connectionUri
              + ". Must start with 'mongodb://' or 'mongodb+srv://'");
    }

    String databaseName = options.getDatabaseName();
    if (databaseName == null || databaseName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Database name (databaseName) must be specified and non-empty.");
    }

    String inputFileFormat = options.getInputFileFormat();
    if (inputFileFormat != null
        && !inputFileFormat.isEmpty()
        && !(inputFileFormat.equals(AVRO_SUFFIX) || inputFileFormat.equals(JSON_SUFFIX))) {
      throw new IllegalArgumentException(
          "Input file format must be one of: avro, json or left empty - found " + inputFileFormat);
    }

    if (options.getBatchSize() != null && options.getBatchSize() <= 0) {
      throw new IllegalArgumentException(
          "Batch size must be a positive integer - found " + options.getBatchSize());
    }

    if (options.getMaxConcurrentAsyncWrites() != null
        && options.getMaxConcurrentAsyncWrites() <= 0) {
      throw new IllegalArgumentException(
          "Max concurrent async writes must be a positive integer - found "
              + options.getMaxConcurrentAsyncWrites());
    }

    if (options.getInitialWriteRatePerWorker() != null
        && options.getMaxWriteRatePerWorker() != null
        && options.getInitialWriteRatePerWorker() > 0
        && options.getMaxWriteRatePerWorker() > 0
        && options.getInitialWriteRatePerWorker() > options.getMaxWriteRatePerWorker()) {
      throw new IllegalArgumentException(
          "Initial write rate per worker ("
              + options.getInitialWriteRatePerWorker()
              + ") cannot exceed max write rate per worker ("
              + options.getMaxWriteRatePerWorker()
              + ")");
    }

    if (options.getWriteRateRampUpMinutes() != null && options.getWriteRateRampUpMinutes() < 0) {
      throw new IllegalArgumentException(
          "Write rate ramp up minutes cannot be negative - found "
              + options.getWriteRateRampUpMinutes());
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   */
  public static void run(Options options) {
    try {
      validateOptions(options);

      LOG.info(
          "Starting pipeline execution with options: inputFilePattern={}, fileType={},"
              + " databaseName={}, useShadowTables={}",
          options.getInputFilePattern(),
          options.getInputFileFormat(),
          options.getDatabaseName(),
          options.getUseShadowTables());

      // Decode the connection string
      String connectionString = options.getConnectionUri();
      if (connectionString != null
          && connectionString.contains("MONGODB-OIDC")
          && !connectionString.contains("TOKEN_RESOURCE")) {
        connectionString += ",TOKEN_RESOURCE:FIRESTORE";
      }

      // Choose processing mode based on options
      LOG.info("Starting pipeline execution");
      if (!Boolean.TRUE.equals(options.getUseShadowTables())) {
        LOG.info("Using high-throughput shadowless processing mode");
        runShadowless(options, connectionString);
      } else if (Boolean.TRUE.equals(options.getProcessBackfillFirst())) {
        LOG.info("Using legacy backfill-first processing mode with shadow tables");
        runLegacyWithBackfillFirst(options, connectionString);
      } else {
        LOG.info("Using legacy unified processing mode with shadow tables");
        runLegacyAllEventsTogether(options, connectionString);
      }
    } catch (Exception e) {
      LOG.error("Failed to run pipeline: {}", e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Runs the pipeline in high-throughput shadowless mode with hierarchical stages.
   *
   * @param options The execution parameters to the pipeline.
   * @param connectionString The MongoDB/Firestore connection URI.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult runShadowless(Options options, String connectionString) {
    LOG.info("Creating shadowless pipeline DAG");
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(TimestampSortKey.class, TimestampSortKeyCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            MongoDbChangeEventContext.class, MongoDbChangeEventContextCoder.of());
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    /*
     * Stage 1: Read/
     *   - Read/DataStreamIO
     *   - Read/IngestAndNormalizeJson
     *   - Read/MergeWithReconsumedDlq
     */
    LOG.info("Setting up Read/ stage");
    PCollection<FailsafeElement<String, String>> jsonRecords =
        ingestAndNormalizeJsonShadowless(options, dlqManager, pipeline)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    /*
     * Stage 2: Process/
     *   - Process/ApplyUdfToDataField (optional)
     *   - Process/CreateMongoDbChangeEventContext
     *   - Process/KeyByCollectionAndDocId
     *   - Process/GlobalWindows
     *   - Process/StatefulDeduplication
     */
    LOG.info("Setting up Process/ stage");
    if (!Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
      LOG.info("Applying Javascript UDF in Process/ApplyUdfToDataField");
      jsonRecords =
          jsonRecords.apply(
              "Process/ApplyUdfToDataField", new ApplyUdfToDataField(options, dlqManager));
    }

    PCollectionTuple changeEventContexts =
        jsonRecords.apply(
            "Process/CreateMongoDbChangeEventContext",
            ParDo.of(new CreateMongoDbChangeEventContextFn(options.getShadowCollectionPrefix()))
                .withOutputTags(
                    CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG,
                    TupleTagList.of(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)));

    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    writeFailedJsonToDlq(
        options,
        changeEventContexts,
        dlqManager,
        CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG,
        "Process/WriteFailedContextCreationToDlq");

    PCollection<MongoDbChangeEventContext> contexts =
        changeEventContexts.get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG);

    LOG.info("Configuring shadowless stateful deduplication by collection and doc ID");
    PCollection<KV<String, MongoDbChangeEventContext>> keyedEvents =
        contexts.apply(
            "Process/KeyByCollectionAndDocId",
            WithKeys.of(
                    (MongoDbChangeEventContext event) ->
                        event.getDataCollection()
                            + "#"
                            + Utils.documentIdToString(event.getDocumentId()))
                .withKeyType(TypeDescriptors.strings()));

    PCollection<MongoDbChangeEventContext> dedupedEvents =
        keyedEvents
            .apply(
                "Process/GlobalWindows",
                Window.<KV<String, MongoDbChangeEventContext>>into(new GlobalWindows()))
            .apply("Process/StatefulDeduplication", ParDo.of(new StatefulDeduplicationFn()));

    /*
     * Stage 3: Write/
     *   - Write/AsyncBulkWriteToFirestore (MongoDbBulkTransforms.BulkWriteWithDlq)
     *   - Write/WriteToDlq_Retryable
     *   - Write/WriteToDlq_Severe
     */
    LOG.info("Setting up Write/ stage");
    PCollectionTuple writeResult =
        dedupedEvents.apply(
            "Write/AsyncBulkWriteToFirestore",
            MongoDbBulkTransforms.bulkWriteWithDlq()
                .withUri(connectionString)
                .withDatabase(options.getDatabaseName())
                .withBatchSize(options.getBatchSize())
                .withMaxConcurrentAsyncWrites(options.getMaxConcurrentAsyncWrites())
                .withInitialWriteRatePerWorker(options.getInitialWriteRatePerWorker())
                .withWriteRateRampUpMinutes(options.getWriteRateRampUpMinutes())
                .withMaxWriteRatePerWorker(options.getMaxWriteRatePerWorker()));

    writeResult
        .get(MongoDbBulkTransforms.SUCCESSFUL_WRITE_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    writeResult
        .get(MongoDbBulkTransforms.FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));
    writeResult
        .get(MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    writeFailedEventsToDlq(
        options,
        writeResult,
        dlqManager,
        MongoDbBulkTransforms.FAILED_WRITE_TAG,
        "Write/WriteToDlq_Retryable");

    writeSevereEventsToDlq(
        options,
        writeResult,
        dlqManager,
        MongoDbBulkTransforms.SEVERE_FAILED_WRITE_TAG,
        "Write/WriteToDlq_Severe");

    LOG.info("Executing shadowless pipeline");
    return pipeline.run();
  }

  /** Read from input path and dlq to collect objects to process without reshuffle. */
  private static PCollection<FailsafeElement<String, String>> ingestAndNormalizeJsonShadowless(
      Options options, DeadLetterQueueManager dlqManager, Pipeline pipeline) {
    LOG.info("Starting Read/ ingestion for shadowless mode");
    boolean isRegularMode = "regular".equals(options.getRunMode());
    PCollectionTuple reconsumedElements =
        pipeline.apply("Read/PollAndReconsumeDLQ", new ReconsumeDlqTransform(options, dlqManager));

    PCollection<FailsafeElement<String, String>> dlqJsonRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply(
                "Read/Count DLQ Retries",
                ParDo.of(
                    new DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>() {
                      private final Counter dlqRetries =
                          Metrics.counter(DataStreamMongoDBToFirestore.class, "dlqRetries");

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        dlqRetries.inc();
                        c.output(c.element());
                      }
                    }));

    reconsumedElements
        .get(DeadLetterQueueManager.PERMANENT_ERRORS)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "Read/Count Permanent Failures",
            ParDo.of(
                new DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>() {
                  private final Counter permanentFailures =
                      Metrics.counter(DataStreamMongoDBToFirestore.class, "permanentFailures");

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    permanentFailures.inc();
                    c.output(c.element());
                  }
                }))
        .apply(
            "Read/Write Permanent Failures To DLQ - Sanitize",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Read/Write Permanent Failures To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp_severe/")
                .setIncludePaneInfo(true)
                .build());

    if (isRegularMode) {
      PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
          pipeline.apply(
              "Read/DataStreamIO",
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

      return PCollectionList.of(datastreamJsonRecords)
          .and(dlqJsonRecords)
          .apply("Read/MergeWithReconsumedDlq", Flatten.pCollections())
          .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    } else {
      return PCollectionList.of(dlqJsonRecords)
          .apply("Read/MergeWithReconsumedDlq", Flatten.pCollections())
          .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    }
  }

  /** Legacy execution path: backfill events processed before CDC events with shadow tables. */
  public static PipelineResult runLegacyWithBackfillFirst(
      Options options, String connectionString) {
    return runWithBackfillFirst(options, connectionString);
  }

  /** Legacy execution path: all events processed together with shadow tables. */
  public static PipelineResult runLegacyAllEventsTogether(
      Options options, String connectionString) {
    return runAllEventsTogether(options, connectionString);
  }

  /**
   * Runs the pipeline with backfill events processed before CDC events.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult runWithBackfillFirst(Options options, String connectionString) {
    LOG.info("Creating pipeline with backfill-first processing");
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(TimestampSortKey.class, TimestampSortKeyCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            MongoDbChangeEventContext.class, MongoDbChangeEventContextCoder.of());

    LOG.info("Building Dead Letter Queue manager");
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    // Stage 1: Ingest data from GCS
    LOG.info("Configuring data ingestion from GCS");
    PCollection<FailsafeElement<String, String>> jsonRecords =
        ingestAndNormalizeJson(options, dlqManager, pipeline)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Optional Stage 1.5: Apply Javascript UDF for JSON transformation
    if (!Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
      LOG.info("Applying Javascript UDF for JSON transformation");
      jsonRecords =
          jsonRecords.apply(
              "Apply UDF To Data Field", new ApplyUdfToDataField(options, dlqManager));
    }

    // Stage 2: Create MongoDbChangeEventContext objects
    LOG.info("Configuring MongoDbChangeEventContext creation");
    PCollectionTuple changeEventContexts =
        jsonRecords.apply(
            "Create MongoDbChangeEventContext objects",
            ParDo.of(new CreateMongoDbChangeEventContextFn(options.getShadowCollectionPrefix()))
                .withOutputTags(
                    CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG,
                    TupleTagList.of(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)));

    // Set coders
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    // Handle failed creation with DLQ
    writeFailedJsonToDlq(
        options,
        changeEventContexts,
        dlqManager,
        CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG);

    // Stage 3: Split events into backfill and CDC streams
    LOG.info("Configuring event splitting into backfill and CDC streams");
    PCollectionTuple splitEvents =
        changeEventContexts
            .get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG)
            .apply(
                "Split Backfill and CDC",
                ParDo.of(new SplitBackfillAndCdcEventsFn())
                    .withOutputTags(
                        SplitBackfillAndCdcEventsFn.BACKFILL_TAG,
                        TupleTagList.of(SplitBackfillAndCdcEventsFn.CDC_TAG)));

    // Set coders for split events
    splitEvents
        .get(SplitBackfillAndCdcEventsFn.BACKFILL_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    splitEvents
        .get(SplitBackfillAndCdcEventsFn.CDC_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());

    // Stage 4: Process backfill events
    LOG.info("Configuring backfill event processing");
    PCollectionTuple backfillResult;
    if (options.getUseShadowTablesForBackfill()) {
      // Use shadow tables for backfill (same as CDC processing)
      backfillResult =
          splitEvents
              .get(SplitBackfillAndCdcEventsFn.BACKFILL_TAG)
              .apply(
                  "Process Backfill with Shadow Tables",
                  ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                      .withOutputTags(
                          ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG,
                          TupleTagList.of(ProcessChangeEventFn.FAILED_WRITE_TAG)
                              .and(ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG)));
    } else {
      // Process backfill without shadow tables
      backfillResult =
          splitEvents
              .get(SplitBackfillAndCdcEventsFn.BACKFILL_TAG)
              .apply(
                  "Process Backfill without Shadow Tables",
                  ParDo.of(
                          new ProcessBackfillEventFn(
                              connectionString, options.getDatabaseName(), options.getBatchSize()))
                      .withOutputTags(
                          ProcessBackfillEventFn.SUCCESSFUL_WRITE_TAG,
                          TupleTagList.of(ProcessBackfillEventFn.FAILED_WRITE_TAG)
                              .and(ProcessBackfillEventFn.SEVERE_FAILED_WRITE_TAG)));
    }

    // Set coders for backfill results
    backfillResult
        .get(
            options.getUseShadowTablesForBackfill()
                ? ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG
                : ProcessBackfillEventFn.SUCCESSFUL_WRITE_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    backfillResult
        .get(
            options.getUseShadowTablesForBackfill()
                ? ProcessChangeEventFn.FAILED_WRITE_TAG
                : ProcessBackfillEventFn.FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    backfillResult
        .get(
            options.getUseShadowTablesForBackfill()
                ? ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG
                : ProcessBackfillEventFn.SEVERE_FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    // Handle failed backfill writes with DLQ
    writeFailedEventsToDlq(
        options,
        backfillResult,
        dlqManager,
        options.getUseShadowTablesForBackfill()
            ? ProcessChangeEventFn.FAILED_WRITE_TAG
            : ProcessBackfillEventFn.FAILED_WRITE_TAG);

    // Write severe backfill failures directly to severe DLQ
    writeSevereEventsToDlq(
        options,
        backfillResult,
        dlqManager,
        options.getUseShadowTablesForBackfill()
            ? ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG
            : ProcessBackfillEventFn.SEVERE_FAILED_WRITE_TAG);

    // Stage 5: Process CDC events
    LOG.info("Configuring CDC event processing");
    PCollectionTuple cdcResult =
        splitEvents
            .get(SplitBackfillAndCdcEventsFn.CDC_TAG)
            .apply(
                "Process CDC Events",
                ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                    .withOutputTags(
                        ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG,
                        TupleTagList.of(ProcessChangeEventFn.FAILED_WRITE_TAG)
                            .and(ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG)));

    // Set coders for CDC results
    cdcResult
        .get(ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());
    cdcResult
        .get(ProcessChangeEventFn.FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    cdcResult
        .get(ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    // Handle failed CDC writes with DLQ
    writeFailedEventsToDlq(options, cdcResult, dlqManager, ProcessChangeEventFn.FAILED_WRITE_TAG);
    // Write severe CDC failures directly to severe DLQ
    writeSevereEventsToDlq(
        options, cdcResult, dlqManager, ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG);

    // Execute the pipeline
    LOG.info("Executing pipeline");
    return pipeline.run();
  }

  /**
   * Runs the pipeline with all events processed together using shadow tables.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult runAllEventsTogether(Options options, String connectionString) {
    LOG.info("Creating pipeline");
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(TimestampSortKey.class, TimestampSortKeyCoder.of());
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            MongoDbChangeEventContext.class, MongoDbChangeEventContextCoder.of());

    LOG.info("Building Dead Letter Queue manager");
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    LOG.info("Stage 1: Configuring data ingestion from GCS");
    PCollection<FailsafeElement<String, String>> jsonRecords =
        ingestAndNormalizeJson(options, dlqManager, pipeline)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    if (!Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
      jsonRecords =
          jsonRecords.apply(
              "Apply UDF To Data Field", new ApplyUdfToDataField(options, dlqManager));
    }

    LOG.info("Stage 2: Configuring MongoDbChangeEventContext creation");
    PCollectionTuple changeEventContexts =
        jsonRecords.apply(
            "Create MongoDbChangeEventContext objects",
            ParDo.of(new CreateMongoDbChangeEventContextFn(options.getShadowCollectionPrefix()))
                .withOutputTags(
                    CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG,
                    TupleTagList.of(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)));

    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());

    changeEventContexts
        .get(CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    LOG.info("Setting up DLQ handling for failed event creation");
    writeFailedJsonToDlq(
        options,
        changeEventContexts,
        dlqManager,
        CreateMongoDbChangeEventContextFn.FAILED_CREATION_TAG);

    LOG.info("Stage 3: Configuring change event processing and destination database writing");
    PCollectionTuple writeResult =
        changeEventContexts
            .get(CreateMongoDbChangeEventContextFn.SUCCESSFUL_CREATION_TAG)
            .setCoder(MongoDbChangeEventContextCoder.of())
            .apply(
                "Transactional write events",
                ParDo.of(new ProcessChangeEventFn(connectionString, options.getDatabaseName()))
                    .withOutputTags(
                        ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG,
                        TupleTagList.of(ProcessChangeEventFn.FAILED_WRITE_TAG)
                            .and(ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG)));

    writeResult
        .get(ProcessChangeEventFn.SUCCESSFUL_WRITE_TAG)
        .setCoder(MongoDbChangeEventContextCoder.of());

    writeResult
        .get(ProcessChangeEventFn.FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    writeResult
        .get(ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()));

    LOG.info("Setting up DLQ handling for failed writes");
    writeFailedEventsToDlq(options, writeResult, dlqManager, ProcessChangeEventFn.FAILED_WRITE_TAG);
    writeSevereEventsToDlq(
        options, writeResult, dlqManager, ProcessChangeEventFn.SEVERE_FAILED_WRITE_TAG);

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
        tempLocation = "/tmp/";
        LOG.warn("TempLocation is null, using default location: {}", tempLocation);
      }
    } catch (Exception e) {
      tempLocation = "/tmp/";
      LOG.warn("Error getting tempLocation, using default location: {}", tempLocation, e);
    }

    String dlqDirectory =
        Strings.isNullOrEmpty(options.getDeadLetterQueueDirectory())
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
    LOG.info("Configuring ingestion and normalization of JSON data");
    PCollection<FailsafeElement<String, String>> jsonRecords;
    PCollectionTuple reconsumedElements;
    boolean isRegularMode = "regular".equals(options.getRunMode());

    LOG.info("Setting up DLQ reconsumption, mode: {}", isRegularMode ? "regular" : "retry");
    reconsumedElements =
        pipeline.apply("PollAndReconsumeDLQ", new ReconsumeDlqTransform(options, dlqManager));

    LOG.info("Processing retryable errors from DLQ");
    PCollection<FailsafeElement<String, String>> dlqJsonRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
            .apply(
                "Count DLQ Retries",
                ParDo.of(
                    new DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>() {
                      private final Counter dlqRetries =
                          Metrics.counter(DataStreamMongoDBToFirestore.class, "dlqRetries");

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        dlqRetries.inc();
                        c.output(c.element());
                      }
                    }));

    // Write non-retryable errors to DLQ
    reconsumedElements
        .get(DeadLetterQueueManager.PERMANENT_ERRORS)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "Count Permanent Failures",
            ParDo.of(
                new DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>() {
                  private final Counter permanentFailures =
                      Metrics.counter(DataStreamMongoDBToFirestore.class, "permanentFailures");

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    permanentFailures.inc();
                    c.output(c.element());
                  }
                }))
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
          "DataStreamIO configured with fileReadConcurrency: {}, directoryWatchDuration: {}"
              + " minutes",
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
    return jsonRecords;
  }

  private static void writeFailedJsonToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<String, String>> failedTag) {
    writeFailedJsonToDlq(
        options, results, dlqManager, failedTag, "Write Failed Json To DLQ - " + failedTag.getId());
  }

  private static void writeFailedJsonToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<String, String>> failedTag,
      String stageName) {
    LOG.info("Setting up DLQ for failed JSON processing: {}", stageName);
    results
        .get(failedTag)
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(stageName + " - Sanitize", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            stageName,
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
    writeFailedEventsToDlq(options, results, dlqManager, failedTag, "Write Events Failures To DLQ");
  }

  private static void writeFailedEventsToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> failedTag,
      String stageName) {
    LOG.info("Setting up DLQ for failed MongoDB event processing: {}", stageName);
    results
        .get(failedTag)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()))
        .apply(
            stageName + " - Sanitize", MapElements.via(new MongoDbEventDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            stageName,
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getRetryDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_retry_mongo_event/")
                .setIncludePaneInfo(true)
                .build());
    LOG.info("DLQ setup completed for failed MongoDB event processing");
  }

  private static void writeSevereEventsToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> failedTag) {
    writeSevereEventsToDlq(
        options, results, dlqManager, failedTag, "Write Severe Events Failures To DLQ");
  }

  private static void writeSevereEventsToDlq(
      Options options,
      PCollectionTuple results,
      DeadLetterQueueManager dlqManager,
      TupleTag<FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>> failedTag,
      String stageName) {
    LOG.info("Setting up Severe DLQ for failed MongoDB event processing: {}", stageName);
    results
        .get(failedTag)
        .setCoder(
            FailsafeElementCoder.of(
                MongoDbChangeEventContextCoder.of(), MongoDbChangeEventContextCoder.of()))
        .apply(
            stageName + " - Sanitize", MapElements.via(new MongoDbEventDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            stageName,
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(
                    options.getDeadLetterQueueDirectory() + "/tmp_severe_mongo_event/")
                .setIncludePaneInfo(true)
                .build());
    LOG.info("Severe DLQ setup completed");
  }

  /** DoFn to split events into backfill and CDC streams. */
  public static class SplitBackfillAndCdcEventsFn
      extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

    private static final Logger LOG = LoggerFactory.getLogger(SplitBackfillAndCdcEventsFn.class);

    public static final TupleTag<MongoDbChangeEventContext> BACKFILL_TAG =
        new TupleTag<>("backfill");
    public static final TupleTag<MongoDbChangeEventContext> CDC_TAG = new TupleTag<>("cdc");

    @ProcessElement
    public void processElement(ProcessContext c, MultiOutputReceiver out) {
      MongoDbChangeEventContext event = c.element();

      if (isNonDlqBackfillEvent(event)) {
        LOG.debug("Classified event as backfill for document ID: {}", event.getDocumentId());
        out.get(BACKFILL_TAG).output(event);
      } else {
        LOG.debug("Classified event as CDC for document ID: {}", event.getDocumentId());
        out.get(CDC_TAG).output(event);
      }
    }

    private boolean isNonDlqBackfillEvent(MongoDbChangeEventContext event) {
      if (event.getIsDlqReconsumed()) {
        return false;
      }
      JsonNode jsonNode = event.getChangeEvent();

      boolean hasCdcFields =
          jsonNode.has("_metadata_log_file")
              || jsonNode.has("_metadata_log_position")
              || jsonNode.has("_metadata_scn")
              || jsonNode.has("_metadata_ssn")
              || jsonNode.has("_metadata_rs_id");

      String changeType = null;
      if (jsonNode.has(DatastreamConstants.EVENT_CHANGE_TYPE_KEY)) {
        changeType = jsonNode.get(DatastreamConstants.EVENT_CHANGE_TYPE_KEY).asText();
      }

      return !hasCdcFields && (changeType == null || "READ".equals(changeType));
    }
  }

  /** DoFn to process backfill events using MongoDB bulk writes. */
  public static class ProcessBackfillEventFn
      extends DoFn<MongoDbChangeEventContext, MongoDbChangeEventContext> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessBackfillEventFn.class);

    public static final TupleTag<MongoDbChangeEventContext> SUCCESSFUL_WRITE_TAG =
        new TupleTag<>("backfillSuccessfulWrite");
    public static final TupleTag<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        FAILED_WRITE_TAG = new TupleTag<>("backfillFailedWrite");
    public static final TupleTag<
            FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext>>
        SEVERE_FAILED_WRITE_TAG = new TupleTag<>("backfillSevereFailedWrite");

    private final String connectionString;
    private final String targetDatabaseName;
    private final int batchSize;

    private transient Map<String, List<MongoDbChangeEventContext>> bufferedEvents;
    private transient Map<String, MongoCollection<Document>> collectionMap;
    private transient MongoClient client;

    private final Counter successfulWrites =
        Metrics.counter(ProcessBackfillEventFn.class, "successfulWrites");
    private final Counter retriableFailedWrites =
        Metrics.counter(ProcessBackfillEventFn.class, "retriableFailedWrites");
    private final Counter severeFailedWrites =
        Metrics.counter(ProcessBackfillEventFn.class, "severeFailedWrites");

    public ProcessBackfillEventFn(String connectionString, String databaseName, int batchSize) {
      this.connectionString = connectionString;
      this.targetDatabaseName = databaseName;
      this.batchSize = batchSize;
    }

    @com.google.common.annotations.VisibleForTesting
    public ProcessBackfillEventFn(
        com.mongodb.client.MongoClient client, String databaseName, int batchSize) {
      this.client = client;
      this.targetDatabaseName = databaseName;
      this.batchSize = batchSize;
      this.connectionString = "";
    }

    @Setup
    public void setup() {
      LOG.info("Setting up MongoDB client for backfill processing with batch size: {}", batchSize);
      if (client == null) {
        com.mongodb.MongoClientSettings settings =
            com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
                .applyToSocketSettings(
                    builder -> {
                      builder.connectTimeout(60, TimeUnit.SECONDS);
                      builder.readTimeout(60, TimeUnit.SECONDS);
                    })
                .applyToClusterSettings(
                    builder -> builder.serverSelectionTimeout(10, TimeUnit.MINUTES))
                .uuidRepresentation(org.bson.UuidRepresentation.STANDARD)
                .build();
        client = com.mongodb.client.MongoClients.create(settings);
      }
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

      if (!bufferedEvents.containsKey(collectionName)) {
        LOG.debug("Creating new buffer for collection: {}", collectionName);
        bufferedEvents.put(collectionName, new ArrayList<>());

        if (!collectionMap.containsKey(collectionName)) {
          MongoDatabase database = client.getDatabase(targetDatabaseName);
          collectionMap.put(collectionName, database.getCollection(collectionName));
        }
      }

      bufferedEvents.get(collectionName).add(element);

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
        List<WriteModel<Document>> bulkOperations = new ArrayList<>(events.size());

        for (MongoDbChangeEventContext event : events) {
          Object docId = event.getDocumentId();
          Bson lookupById = eq("_id", docId);

          if (event.isDeleteEvent()) {
            bulkOperations.add(new DeleteOneModel<>(lookupById));
          } else {
            bulkOperations.add(
                new ReplaceOneModel<>(
                    lookupById,
                    Utils.jsonToDocument(event.getDataAsJsonString(), event.getDocumentId()),
                    new ReplaceOptions().upsert(true)));
          }
        }

        BulkWriteResult result =
            collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(false));
        LOG.debug(
            "Bulk write completed for collection {}: {} inserts/updates, {} deletes",
            collectionName,
            result.getInsertedCount() + result.getModifiedCount() + result.getUpserts().size(),
            result.getDeletedCount());

        for (MongoDbChangeEventContext event : events) {
          out.get(SUCCESSFUL_WRITE_TAG).output(event);
          successfulWrites.inc();
        }
      } catch (MongoBulkWriteException e) {
        LOG.warn(
            "Bulk write partially failed for collection {}: {}", collectionName, e.getMessage());

        Set<Integer> failedIndices = new HashSet<>();
        for (BulkWriteError error : e.getWriteErrors()) {
          failedIndices.add(error.getIndex());
          MongoDbChangeEventContext event = events.get(error.getIndex());
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(error.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));

          if (error.getCode() == ProcessChangeEventFn.INVALID_ARGUMENT) {
            out.get(SEVERE_FAILED_WRITE_TAG).output(failedElement);
            severeFailedWrites.inc();
          } else {
            out.get(FAILED_WRITE_TAG).output(failedElement);
            retriableFailedWrites.inc();
          }
        }

        for (int i = 0; i < events.size(); i++) {
          if (!failedIndices.contains(i)) {
            out.get(SUCCESSFUL_WRITE_TAG).output(events.get(i));
            successfulWrites.inc();
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Error processing backfill batch for collection {}: {}",
            collectionName,
            e.getMessage(),
            e);

        for (MongoDbChangeEventContext event : events) {
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
          out.get(FAILED_WRITE_TAG).output(failedElement);
          retriableFailedWrites.inc();
        }
      }

      events.clear();
    }

    private void processBatchFinish(String collectionName, FinishBundleContext context) {
      List<MongoDbChangeEventContext> events = bufferedEvents.get(collectionName);
      MongoCollection<Document> collection = collectionMap.get(collectionName);

      if (events.isEmpty()) {
        return;
      }

      try {
        List<WriteModel<Document>> bulkOperations = new ArrayList<>(events.size());

        for (MongoDbChangeEventContext event : events) {
          Object docId = event.getDocumentId();
          Bson lookupById = eq("_id", docId);

          if (event.isDeleteEvent()) {
            bulkOperations.add(new DeleteOneModel<>(lookupById));
          } else {
            bulkOperations.add(
                new ReplaceOneModel<>(
                    lookupById,
                    Utils.jsonToDocument(event.getDataAsJsonString(), event.getDocumentId()),
                    new ReplaceOptions().upsert(true)));
          }
        }

        BulkWriteResult result =
            collection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(false));
        LOG.debug(
            "Bulk write completed for collection {}: {} inserts/updates, {} deletes",
            collectionName,
            result.getInsertedCount() + result.getModifiedCount() + result.getUpserts().size(),
            result.getDeletedCount());

        for (MongoDbChangeEventContext event : events) {
          context.output(SUCCESSFUL_WRITE_TAG, event, Instant.now(), GlobalWindow.INSTANCE);
          successfulWrites.inc();
        }
      } catch (MongoBulkWriteException e) {
        LOG.warn(
            "Bulk write partially failed for collection {}: {}", collectionName, e.getMessage());

        Set<Integer> failedIndices = new HashSet<>();
        for (BulkWriteError error : e.getWriteErrors()) {
          failedIndices.add(error.getIndex());
          MongoDbChangeEventContext event = events.get(error.getIndex());
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(error.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));

          if (error.getCode() == ProcessChangeEventFn.INVALID_ARGUMENT) {
            context.output(
                SEVERE_FAILED_WRITE_TAG, failedElement, Instant.now(), GlobalWindow.INSTANCE);
            severeFailedWrites.inc();
          } else {
            context.output(FAILED_WRITE_TAG, failedElement, Instant.now(), GlobalWindow.INSTANCE);
            retriableFailedWrites.inc();
          }
        }

        for (int i = 0; i < events.size(); i++) {
          if (!failedIndices.contains(i)) {
            context.output(
                SUCCESSFUL_WRITE_TAG, events.get(i), Instant.now(), GlobalWindow.INSTANCE);
            successfulWrites.inc();
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Error processing backfill batch for collection {}: {}",
            collectionName,
            e.getMessage(),
            e);

        for (MongoDbChangeEventContext event : events) {
          FailsafeElement<MongoDbChangeEventContext, MongoDbChangeEventContext> failedElement =
              FailsafeElement.of(event, event);
          failedElement.setErrorMessage(e.getMessage());
          failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
          context.output(FAILED_WRITE_TAG, failedElement, Instant.now(), GlobalWindow.INSTANCE);
          retriableFailedWrites.inc();
        }
      }

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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static class PrepareUdfInputFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {

    private final Counter skippedUpdates =
        Metrics.counter(PrepareUdfInputFn.class, "skippedUpdatesWithNullData");

    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<String, String> element = c.element();
      try {
        String fullEventJson = element.getPayload();
        Document doc = Document.parse(fullEventJson);
        Document innerDoc = Utils.extractInnerEvent(doc);

        String changeType = innerDoc.getString(DatastreamConstants.EVENT_CHANGE_TYPE_KEY);
        if (changeType == null) {
          changeType = "";
        }

        Object dataVal = innerDoc.get(MongoDbChangeEventContext.DATA_COL);

        if ("DELETE".equalsIgnoreCase(changeType)) {
          c.output(BYPASS_UDF_TAG, element);
          return;
        }

        if ("UPDATE".equalsIgnoreCase(changeType) && dataVal == null) {
          skippedUpdates.inc();
          return;
        }

        String canonicalJson = Utils.getCanonicalJsonOfDataField(innerDoc);
        if (canonicalJson == null) {
          throw new IllegalArgumentException(
              "Missing data field in event or unsupported data field type");
        }

        c.output(FailsafeElement.of(fullEventJson, canonicalJson));
      } catch (Exception e) {
        LOG.error("Error preparing UDF input, exception: {}", e.getMessage(), e);
        FailsafeElement<String, String> failedElement =
            FailsafeElement.of(element.getOriginalPayload(), element.getPayload());
        failedElement.setErrorMessage(e.getMessage());
        failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
        c.output(PREPARE_FAILURE_TAG, failedElement);
      }
    }
  }

  public static class RestoreUdfOutputFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      FailsafeElement<String, String> element = c.element();
      String fullEventJson = element.getOriginalPayload();
      String transformedData = element.getPayload();

      try {
        JsonNode fullEventNode = OBJECT_MAPPER.readTree(fullEventJson);
        Document.parse(transformedData);

        JsonNode targetNode = Utils.extractInnerEvent(fullEventNode);
        ((ObjectNode) targetNode).put(MongoDbChangeEventContext.DATA_COL, transformedData);

        String modifiedEventJson = OBJECT_MAPPER.writeValueAsString(fullEventNode);
        c.output(FailsafeElement.of(element.getOriginalPayload(), modifiedEventJson));
      } catch (Exception e) {
        LOG.error("Error restoring UDF output, exception: {}", e.getMessage(), e);
        FailsafeElement<String, String> failedElement =
            FailsafeElement.of(element.getOriginalPayload(), element.getPayload());
        failedElement.setErrorMessage(e.getMessage());
        failedElement.setStacktrace(Throwables.getStackTraceAsString(e));
        c.output(RESTORE_FAILURE_TAG, failedElement);
      }
    }
  }

  public static class ApplyUdfToDataField
      extends PTransform<
          PCollection<FailsafeElement<String, String>>,
          PCollection<FailsafeElement<String, String>>> {
    private final Options options;
    private final DeadLetterQueueManager dlqManager;

    public ApplyUdfToDataField(Options options, DeadLetterQueueManager dlqManager) {
      this.options = options;
      this.dlqManager = dlqManager;
    }

    @Override
    public PCollection<FailsafeElement<String, String>> expand(
        PCollection<FailsafeElement<String, String>> input) {

      PCollectionTuple preparedResult =
          input.apply(
              "Prepare UDF Input",
              ParDo.of(new PrepareUdfInputFn())
                  .withOutputTags(
                      UDF_SUCCESS_TAG, TupleTagList.of(PREPARE_FAILURE_TAG).and(BYPASS_UDF_TAG)));

      writeFailedJsonToDlq(options, preparedResult, dlqManager, PREPARE_FAILURE_TAG);

      PCollection<FailsafeElement<String, String>> preparedInput =
          preparedResult
              .get(UDF_SUCCESS_TAG)
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

      PCollection<FailsafeElement<String, String>> bypassedElements =
          preparedResult
              .get(BYPASS_UDF_TAG)
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

      PCollectionTuple udfResult =
          preparedInput.apply(
              "Run UDF",
              FailsafeJavascriptUdf.<String>newBuilder()
                  .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                  .setFunctionName(options.getJavascriptTextTransformFunctionName())
                  .setReloadIntervalMinutes(
                      options.getJavascriptTextTransformReloadIntervalMinutes())
                  .setSuccessTag(UDF_SUCCESS_TAG)
                  .setFailureTag(UDF_FAILURE_TAG)
                  .build());

      writeFailedJsonToDlq(options, udfResult, dlqManager, UDF_FAILURE_TAG);

      PCollectionTuple restoreResult =
          udfResult
              .get(UDF_SUCCESS_TAG)
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
              .apply(
                  "Restore UDF Output",
                  ParDo.of(new RestoreUdfOutputFn())
                      .withOutputTags(UDF_SUCCESS_TAG, TupleTagList.of(RESTORE_FAILURE_TAG)));

      writeFailedJsonToDlq(options, restoreResult, dlqManager, RESTORE_FAILURE_TAG);

      PCollection<FailsafeElement<String, String>> restoredOutput =
          restoreResult
              .get(UDF_SUCCESS_TAG)
              .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

      return PCollectionList.of(restoredOutput)
          .and(bypassedElements)
          .apply("Merge Streams", Flatten.pCollections());
    }
  }

  /**
   * Composite PTransform that encapsulates DLQ polling and reconsumption logic under the Read/
   * stage.
   */
  public static class ReconsumeDlqTransform extends PTransform<PBegin, PCollectionTuple> {
    private final Options options;
    private final DeadLetterQueueManager dlqManager;

    public ReconsumeDlqTransform(Options options, DeadLetterQueueManager dlqManager) {
      this.options = options;
      this.dlqManager = dlqManager;
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
      boolean isRegularMode = "regular".equals(options.getRunMode());
      if (isRegularMode && (!Strings.isNullOrEmpty(options.getDlqGcsPubSubSubscription()))) {
        return dlqManager.getReconsumerDataTransformForFiles(
            input.apply(
                "Read retry from PubSub",
                new PubSubNotifiedDlqIO(
                    options.getDlqGcsPubSubSubscription(),
                    new ArrayList<String>(
                        Arrays.asList("/severe/", "/tmp_retry", "/tmp_severe/", ".temp")))));
      } else {
        return dlqManager.getReconsumerDataTransform(
            input.apply(
                "Periodically polling from DLQ",
                dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
      }
    }
  }
}
