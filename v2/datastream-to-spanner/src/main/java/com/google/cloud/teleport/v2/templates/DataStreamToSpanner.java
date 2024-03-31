/*
 * Copyright (C) 2020 Google LLC
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

import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Options.RpcPriority;
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
import com.google.cloud.teleport.v2.datastream.utils.DataStreamClient;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.TransformationContextReader;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner.Options;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.spanner.ProcessInformationSchema;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests DataStream data from GCS as events. The events are written to Cloud
 * Spanner.
 *
 * <p>NOTE: Future versions will support: Pub/Sub, GCS, or Kafka as per DataStream
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Datastream_to_Spanner",
    category = TemplateCategory.STREAMING,
    displayName = "Datastream to Cloud Spanner",
    description = {
      "The Datastream to Cloud Spanner template is a streaming pipeline that reads <a"
          + " href=\"https://cloud.google.com/datastream/docs\">Datastream</a> events from a Cloud"
          + " Storage bucket and writes them to a Cloud Spanner database. It is intended for data"
          + " migration from Datastream sources to Cloud Spanner.\n",
      "All tables required for migration must exist in the destination Cloud Spanner database prior"
          + " to template execution. Hence schema migration from a source database to destination"
          + " Cloud Spanner must be completed prior to data migration. Data can exist in the tables"
          + " prior to migration. This template does not propagate Datastream schema changes to the"
          + " Cloud Spanner database.\n",
      "Data consistency is guaranteed only at the end of migration when all data has been written"
          + " to Cloud Spanner. To store ordering information for each record written to Cloud"
          + " Spanner, this template creates an additional table (called a shadow table) for each"
          + " table in the Cloud Spanner database. This is used to ensure consistency at the end of"
          + " migration. The shadow tables are not deleted after migration and can be used for"
          + " validation purposes at the end of migration.\n",
      "Any errors that occur during operation, such as schema mismatches, malformed JSON files, or"
          + " errors resulting from executing transforms, are recorded in an error queue. The error"
          + " queue is a Cloud Storage folder which stores all the Datastream events that had"
          + " encountered errors along with the error reason in text format. The errors can be"
          + " transient or permanent and are stored in appropriate Cloud Storage folders in the"
          + " error queue. The transient errors are retried automatically while the permanent"
          + " errors are not. In case of permanent errors, you have the option of making"
          + " corrections to the change events and moving them to the retriable bucket while the"
          + " template is running."
    },
    optionsClass = Options.class,
    flexContainerName = "datastream-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-cloud-spanner",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "A Datastream stream in Running or Not started state.",
      "A Cloud Storage bucket where Datastream events are replicated.",
      "A Cloud Spanner database with existing tables. These tables can be empty or contain data.",
    },
    streaming = true)
public class DataStreamToSpanner {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpanner.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @TemplateParameter.Text(
        order = 1,
        description = "File location for Datastream file output in Cloud Storage.",
        helpText =
            "This is the file location for Datastream file output in Cloud Storage. Normally, this"
                + " will be gs://${BUCKET}/${ROOT_PATH}/.")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {@TemplateEnumOption("avro"), @TemplateEnumOption("json")},
        optional = true,
        description = "Datastream output file format (avro/json).",
        helpText =
            "This is the format of the output file produced by Datastream. By default this will be"
                + " avro.")
    @Default.String("avro")
    String getInputFileFormat();

    void setInputFileFormat(String value);

    @TemplateParameter.GcsReadFile(
        order = 3,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Text(
        order = 4,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changes are replicated.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 5,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database where the changes are replicated.")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 6,
        optional = true,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getProjectId();

    void setProjectId(String projectId);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "The Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    String getSpannerHost();

    void setSpannerHost(String value);

    @TemplateParameter.PubsubSubscription(
        order = 8,
        optional = true,
        description = "The Pub/Sub subscription being used in a Cloud Storage notification policy.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy. The name"
                + " should be in the format of"
                + " projects/<project-id>/subscriptions/<subscription-name>.")
    String getGcsPubSubSubscription();

    void setGcsPubSubSubscription(String value);

    @TemplateParameter.Text(
        order = 9,
        description = "Datastream stream name.",
        helpText = "This is the Datastream stream name used to get information.")
    String getStreamName();

    void setStreamName(String value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        description = "Cloud Spanner shadow table prefix.",
        helpText = "The prefix used for the shadow table.")
    @Default.String("shadow_")
    String getShadowTablePrefix();

    void setShadowTablePrefix(String value);

    @TemplateParameter.Boolean(
        order = 11,
        optional = true,
        description = "If true, create shadow tables in Cloud Spanner.",
        helpText =
            "This flag indicates whether shadow tables must be created in Cloud Spanner database.")
    @Default.Boolean(true)
    Boolean getShouldCreateShadowTables();

    void setShouldCreateShadowTables(Boolean value);

    @TemplateParameter.DateTime(
        order = 12,
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

    @TemplateParameter.Integer(
        order = 13,
        optional = true,
        description = "File read concurrency",
        helpText = "The number of concurrent DataStream files to read.")
    @Default.Integer(30)
    Integer getFileReadConcurrency();

    void setFileReadConcurrency(Integer value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "This is the file path to store the deadletter queue output. Default is a directory"
                + " under the Dataflow job's temp location. The default value is enough under most"
                + " conditions.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 15,
        optional = true,
        description = "Dead letter queue retry minutes",
        helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);

    @TemplateParameter.Integer(
        order = 16,
        optional = true,
        description = "Dead letter queue maximum retry count",
        helpText =
            "The max number of times temporary errors can be retried through DLQ. Defaults to 500.")
    @Default.Integer(500)
    Integer getDlqMaxRetryCount();

    void setDlqMaxRetryCount(Integer value);

    // DataStream API Root Url (only used for testing)
    @TemplateParameter.Text(
        order = 17,
        optional = true,
        description = "Datastream API Root URL (only required for testing)",
        helpText = "Datastream API Root URL.")
    @Default.String("https://datastream.googleapis.com/")
    String getDataStreamRootUrl();

    void setDataStreamRootUrl(String value);

    @TemplateParameter.Text(
        order = 18,
        optional = true,
        description = "Datastream source type (only required for testing)",
        helpText =
            "This is the type of source database that Datastream connects to. Example -"
                + " mysql/oracle. Need to be set when testing without an actual running"
                + " Datastream.")
    String getDatastreamSourceType();

    void setDatastreamSourceType(String value);

    @TemplateParameter.Boolean(
        order = 19,
        optional = true,
        description =
            "If true, rounds the decimal values in json columns to a number that can be stored"
                + " without loss of precision.",
        helpText =
            "This flag if set, rounds the decimal values in json columns to a number that can be"
                + " stored without loss of precision.")
    @Default.Boolean(false)
    Boolean getRoundJsonDecimals();

    void setRoundJsonDecimals(Boolean value);

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
        order = 21,
        optional = true,
        helpText =
            "Transformation context file path in cloud storage used to populate data used in"
                + " transformations performed during migrations   Eg: The shard id to db name to"
                + " identify the db from which a row was migrated",
        description = "Transformation context file path in cloud storage")
    String getTransformationContextFilePath();

    void setTransformationContextFilePath(String value);

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

    @TemplateParameter.Enum(
        order = 23,
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [HIGH,MEDIUM,LOW]. Defaults to HIGH")
    @Default.Enum("HIGH")
    RpcPriority getSpannerPriority();

    void setSpannerPriority(RpcPriority value);

    @TemplateParameter.PubsubSubscription(
        order = 24,
        optional = true,
        description =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode. The name should be in the format"
                + " of projects/<project-id>/subscriptions/<subscription-name>. When set, the"
                + " deadLetterQueueDirectory and dlqRetryMinutes are ignored.")
    String getDlqGcsPubSubSubscription();

    void setDlqGcsPubSubSubscription(String value);

    @TemplateParameter.GcsReadFile(
        order = 25,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the advanced transformation logic for processing records"
                + " in forward migration.")
    @Default.String("")
    String getTransformationJarPath();

    void setTransformationJarPath(String value);

    @TemplateParameter.Text(
        order = 26,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the advanced transformation implementation.  It is a"
                + " mandatory field in case transformationJarPath is specified")
    @Default.String("")
    String getTransformationClassName();

    void setTransformationClassName(String value);

    @TemplateParameter.Text(
        order = 27,
        optional = true,
        description = "Custom parameters for transformation",
        helpText =
            "String containing any custom parameters to be passed to the advanced transformation class.")
    @Default.String("")
    String getTransformationCustomParameters();

    void setTransformationCustomParameters(String value);
  }

  private static void validateSourceType(Options options) {
    boolean isRetryMode = "retryDLQ".equals(options.getRunMode());
    if (isRetryMode) {
      // retry mode does not read from Datastream
      return;
    }
    String sourceType = getSourceType(options);
    if (!DatastreamConstants.SUPPORTED_DATASTREAM_SOURCES.contains(sourceType)) {
      throw new IllegalArgumentException(
          "Unsupported source type found: "
              + sourceType
              + ". Specify one of the following source types: "
              + DatastreamConstants.SUPPORTED_DATASTREAM_SOURCES);
    }
    options.setDatastreamSourceType(sourceType);
  }

  private static String getSourceType(Options options) {
    if (options.getDatastreamSourceType() != null) {
      return options.getDatastreamSourceType();
    }
    if (options.getStreamName() == null) {
      throw new IllegalArgumentException("Stream name cannot be empty. ");
    }
    GcpOptions gcpOptions = options.as(GcpOptions.class);
    DataStreamClient datastreamClient;
    SourceConfig sourceConfig;
    try {
      datastreamClient = new DataStreamClient(gcpOptions.getGcpCredential());
      sourceConfig = datastreamClient.getSourceConnectionProfile(options.getStreamName());
    } catch (IOException e) {
      LOG.error("IOException Occurred: DataStreamClient failed initialization.");
      throw new IllegalArgumentException("Unable to initialize DatastreamClient: " + e);
    }
    // TODO: use getPostgresSourceConfig() instead of an else once SourceConfig.java is updated.
    if (sourceConfig.getMysqlSourceConfig() != null) {
      return DatastreamConstants.MYSQL_SOURCE_TYPE;
    } else if (sourceConfig.getOracleSourceConfig() != null) {
      return DatastreamConstants.ORACLE_SOURCE_TYPE;
    } else {
      return DatastreamConstants.POSTGRES_SOURCE_TYPE;
    }
    // LOG.error("Source Connection Profile Type Not Supported");
    // throw new IllegalArgumentException("Unsupported source connection profile type in
    // Datastream");
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    LOG.info("Starting DataStream to Cloud Spanner");
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    validateSourceType(options);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Write JSON Strings to Cloud Spanner
     *   3) Write Failures to GCS Dead Letter Queue
     */
    Pipeline pipeline = Pipeline.create(options);
    DeadLetterQueueManager dlqManager = buildDlqManager(options);
    // Ingest session file into schema object.
    Schema schema = SessionFileReader.read(options.getSessionFilePath());
    /*
     * Stage 1: Ingest/Normalize Data to FailsafeElement with JSON Strings and
     * read Cloud Spanner information schema.
     *   a) Prepare spanner config and process information schema
     *   b) Read DataStream data from GCS into JSON String FailsafeElements
     *   c) Reconsume Dead Letter Queue data from GCS into JSON String FailsafeElements
     *   d) Flatten DataStream and DLQ Streams
     */

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));
    /* Process information schema
     * 1) Read information schema from destination Cloud Spanner database
     * 2) Check if shadow tables are present and create if necessary
     * 3) Return new information schema
     */
    PCollection<Ddl> ddl =
        pipeline.apply(
            "Process Information Schema",
            new ProcessInformationSchema(
                spannerConfig,
                options.getShouldCreateShadowTables(),
                options.getShadowTablePrefix(),
                options.getDatastreamSourceType()));
    PCollectionView<Ddl> ddlView = ddl.apply("Cloud Spanner DDL as view", View.asSingleton());
    PCollection<FailsafeElement<String, String>> jsonRecords = null;
    // Elements sent to the Dead Letter Queue are to be reconsumed.
    // A DLQManager is to be created using PipelineOptions, and it is in charge
    // of building pieces of the DLQ.
    PCollectionTuple reconsumedElements = null;
    boolean isRegularMode = "regular".equals(options.getRunMode());
    if (isRegularMode && (!Strings.isNullOrEmpty(options.getDlqGcsPubSubSubscription()))) {
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
      reconsumedElements =
          dlqManager.getReconsumerDataTransform(
              pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    }
    PCollection<FailsafeElement<String, String>> dlqJsonRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    if (isRegularMode) {
      LOG.info("Regular Datastream flow");
      PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
          pipeline.apply(
              new DataStreamIO(
                      options.getStreamName(),
                      options.getInputFilePattern(),
                      options.getInputFileFormat(),
                      options.getGcsPubSubSubscription(),
                      options.getRfcStartDateTime())
                  .withFileReadConcurrency(options.getFileReadConcurrency())
                  .withDirectoryWatchDuration(
                      Duration.standardMinutes(options.getDirectoryWatchDurationInMinutes())));
      jsonRecords =
          PCollectionList.of(datastreamJsonRecords)
              .and(dlqJsonRecords)
              .apply(Flatten.pCollections())
              .apply("Reshuffle", Reshuffle.viaRandomKey());
    } else {
      LOG.info("DLQ retry flow");
      jsonRecords =
          PCollectionList.of(dlqJsonRecords)
              .apply(Flatten.pCollections())
              .apply("Reshuffle", Reshuffle.viaRandomKey());
    }
    /*
     * Stage 2: Write records to Cloud Spanner
     */
    // Ingest transformation context file into memory.
    TransformationContext transformationContext =
        TransformationContextReader.getTransformationContext(
            options.getTransformationContextFilePath());
    SpannerTransactionWriter.Result spannerWriteResults =
        jsonRecords.apply(
            "Write events to Cloud Spanner",
            new SpannerTransactionWriter(
                spannerConfig,
                ddlView,
                schema,
                transformationContext,
                options.getShadowTablePrefix(),
                options.getDatastreamSourceType(),
                isRegularMode,
                options.getTransformationCustomParameters(),
                options.getTransformationJarPath(),
                options.getTransformationClassName()));
    spannerWriteResults
        .filteredEvents()
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(
            "Write To GCS",
            TextIO.write()
                .to("gs://smt-job-b427-bbce/filter.json")
                .withNumShards(1)
                .withWindowedWrites());
    /*
     * Stage 3: Write failures to GCS Dead Letter Queue
     * a) Retryable errors are written to retry GCS Dead letter queue
     * b) Severe errors are written to severe GCS Dead letter queue
     */
    spannerWriteResults
        .retryableErrors()
        .apply(
            "DLQ: Write retryable Failures to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getRetryDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_retry/")
                .setIncludePaneInfo(true)
                .build());
    PCollection<FailsafeElement<String, String>> dlqErrorRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.PERMANENT_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    PCollection<FailsafeElement<String, String>> permanentErrors =
        PCollectionList.of(dlqErrorRecords)
            .and(spannerWriteResults.permanentErrors())
            .apply(Flatten.pCollections())
            .apply("Reshuffle", Reshuffle.viaRandomKey());
    // increment the metrics
    permanentErrors
        .apply("Update metrics", ParDo.of(new MetricUpdaterDoFn(isRegularMode)))
        .apply(
            "DLQ: Write Severe errors to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory((options).getDeadLetterQueueDirectory() + "/tmp_severe/")
                .setIncludePaneInfo(true)
                .build());
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();
    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    options.setDeadLetterQueueDirectory(dlqDirectory);
    if ("regular".equals(options.getRunMode())) {
      return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetryCount());
    } else {
      String retryDlqUri =
          FileSystems.matchNewResource(dlqDirectory, true)
              .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
              .toString();
      LOG.info("Dead-letter retry directory: {}", retryDlqUri);
      return DeadLetterQueueManager.create(dlqDirectory, retryDlqUri, 0);
    }
  }
}
