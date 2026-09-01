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

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.options.DataStreamToSpannerOptions;
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
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NoopSchemaOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesParser;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.ShardingContext;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.DataflowWorkerMachineTypeUtils;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.TransformationContextReader;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.source.DatastreamToSpannerSourceConnectorRegistry;
import com.google.cloud.teleport.v2.templates.spanner.ProcessInformationSchema;
import com.google.cloud.teleport.v2.templates.transform.ChangeEventTransformerDoFn;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerServiceFactoryImpl;
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
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

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
          + " errors are not. In case of permanent errors, you can run the pipeline in one of"
          + " two retry modes depending on your pipeline state. Use `retryDLQ` mode when the"
          + " regular pipeline is concurrently running and pointing to the same DLQ directory."
          + " This mode consumes only severe errors while the regular mode will consume the retriable errors."
          + " Use `retryAllDLQ` mode when the regular pipeline is not running or is stopped."
          + " The `retryAllDLQ` mode consumes errors from both the retry and severe buckets. Do NOT run `retryAllDLQ` concurrently"
          + " with the regular pipeline as they will conflict."
    },
    optionsClass = DataStreamToSpannerOptions.class,
    flexContainerName = "datastream-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-cloud-spanner",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "A Datastream stream in Running or Not started state.",
      "A Cloud Storage bucket where Datastream events are replicated.",
      "A Cloud Spanner database with existing tables. These tables can be empty or contain data.",
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class DataStreamToSpanner {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpanner.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  static void validateSourceType(DataStreamToSpannerOptions options) {
    boolean isRetryMode = Constants.RUN_MODE_RETRY_DLQ.equals(options.getRunMode());
    if (isRetryMode) {
      // retry mode does not read from Datastream
      return;
    }
    String sourceType = getSourceType(options);
    if (!DatastreamToSpannerSourceConnectorRegistry.getSupportedSourceTypes()
        .contains(sourceType)) {
      throw new IllegalArgumentException(
          "Unsupported source type found: "
              + sourceType
              + ". Specify one of the following source types: "
              + DatastreamToSpannerSourceConnectorRegistry.getSupportedSourceTypes());
    }
    options.setDatastreamSourceType(sourceType);
  }

  static String getSourceType(DataStreamToSpannerOptions options) {
    if (options.getDatastreamSourceType() != null) {
      return options.getDatastreamSourceType();
    }
    if (options.getStreamName() == null) {
      throw new IllegalArgumentException("Stream name cannot be empty.");
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
    return getSourceTypeFromConfig(sourceConfig);
  }

  static String getSourceTypeFromConfig(SourceConfig sourceConfig) {
    try {
      return DatastreamToSpannerSourceConnectorRegistry.getSourceTypeFromConfig(sourceConfig);
    } catch (IllegalArgumentException e) {
      LOG.error("Source Connection Profile Type Not Supported", e);
      throw e;
    }
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    LOG.info("Starting DataStream to Cloud Spanner");
    DataStreamToSpannerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataStreamToSpannerOptions.class);
    boolean isRetryDLQMode = Constants.RUN_MODE_RETRY_DLQ.equals(options.getRunMode());
    options.setStreaming(!isRetryDLQMode);
    validateSourceType(options);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(DataStreamToSpannerOptions options) {
    return buildPipeline(options).run();
  }

  static Pipeline buildPipeline(DataStreamToSpannerOptions options) {
    long startTime = System.currentTimeMillis();
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Write JSON Strings to Cloud Spanner
     *   3) Write Failures to GCS Dead Letter Queue
     */
    Pipeline pipeline = Pipeline.create(options);
    String workerMachineType =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getWorkerMachineType();
    Optional<Integer> resourceHintsMinCpus =
        DataflowWorkerMachineTypeUtils.getMinCpuResourceHint(pipeline.getOptions());
    DataflowWorkerMachineTypeUtils.validateMachineSpecs(workerMachineType, 4, resourceHintsMinCpus);
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
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()))
            .withCommitRetrySettings(
                RetrySettings.newBuilder()
                    .setTotalTimeout(Duration.ofMinutes(4))
                    .setInitialRetryDelay(Duration.ofMinutes(0))
                    .setRetryDelayMultiplier(1)
                    .setMaxRetryDelay(Duration.ofMinutes(0))
                    .setInitialRpcTimeout(Duration.ofMinutes(4))
                    .setRpcTimeoutMultiplier(1)
                    .setMaxRpcTimeout(Duration.ofMinutes(4))
                    .setMaxAttempts(1)
                    .build());
    SpannerConfig shadowTableSpannerConfig = getShadowTableSpannerConfig(options);
    /* Process information schema
     * 1) Read information schema from destination Cloud Spanner database
     * 2) Check if shadow tables are present and create if necessary
     * 3) Return new information schema
     */
    PCollectionTuple ddlTuple =
        pipeline.apply(
            "Process Information Schema",
            new ProcessInformationSchema(
                spannerConfig,
                shadowTableSpannerConfig,
                options.getShouldCreateShadowTables(),
                options.getShadowTablePrefix(),
                options.getDatastreamSourceType()));
    PCollectionView<Ddl> ddlView =
        ddlTuple
            .get(ProcessInformationSchema.MAIN_DDL_TAG)
            .apply("Cloud Spanner Main DDL as view", View.asSingleton());

    PCollectionView<Ddl> shadowTableDdlView =
        ddlTuple
            .get(ProcessInformationSchema.SHADOW_TABLE_DDL_TAG)
            .apply("Cloud Spanner shadow tables DDL as view", View.asSingleton());

    PCollection<FailsafeElement<String, String>> jsonRecords = null;
    // Elements sent to the Dead Letter Queue are to be reconsumed.
    // A DLQManager is to be created using PipelineOptions, and it is in charge
    // of building pieces of the DLQ.
    PCollectionTuple reconsumedElements = null;
    boolean isRegularMode = Constants.RUN_MODE_REGULAR.equals(options.getRunMode());
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
      if (isRegularMode) {
        reconsumedElements =
            dlqManager.getReconsumerDataTransform(
                pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
      } else { // retryDLQ or retryAllDLQ mode
        PCollection<String> oneShotRecords =
            pipeline.apply("Read severe from OneShot", dlqManager.dlqOneShotReconsumer(startTime));

        if (Constants.RUN_MODE_RETRY_DLQ.equals(options.getRunMode())) {
          reconsumedElements = dlqManager.getReconsumerDataTransform(oneShotRecords);
        } else {
          // retryAllDLQ mode: Drain both the severe (one-shot) and retry (continuous) buckets
          PCollection<String> continuousRecords =
              pipeline.apply(
                  "Read retry from Continuous",
                  dlqManager.dlqReconsumer(options.getDlqRetryMinutes()));

          PCollection<String> allRecords =
              PCollectionList.of(continuousRecords)
                  .and(oneShotRecords)
                  .apply("Flatten DLQ Records", Flatten.pCollections());

          reconsumedElements = dlqManager.getReconsumerDataTransform(allRecords);
        }
      }
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
                  .withoutDatastreamRecordsReshuffle()
                  .withDirectoryWatchDuration(
                      org.joda.time.Duration.standardMinutes(
                          options.getDirectoryWatchDurationInMinutes()))
                  .withDatastreamSourceType(options.getDatastreamSourceType()));
      int maxNumWorkers = options.getMaxNumWorkers() != 0 ? options.getMaxNumWorkers() : 1;
      jsonRecords =
          PCollectionList.of(datastreamJsonRecords)
              .and(dlqJsonRecords)
              .apply(Flatten.pCollections())
              .apply(
                  "Reshuffle",
                  Reshuffle.<FailsafeElement<String, String>>viaRandomKey()
                      .withNumBuckets(
                          maxNumWorkers * DatastreamToSpannerConstants.MAX_DOFN_PER_WORKER));
    } else {
      LOG.info("DLQ retry flow");
      jsonRecords =
          PCollectionList.of(dlqJsonRecords)
              .apply(Flatten.pCollections())
              .apply("Reshuffle", Reshuffle.viaRandomKey());
    }
    /*
     * Stage 2: Transform records
     */

    // Ingest transformation context file into memory.
    TransformationContext transformationContext =
        TransformationContextReader.getTransformationContext(
            options.getTransformationContextFilePath());

    // Ingest sharding context file into memory.
    ShardingContext shardingContext = getShardingContext(options);

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();

    // Create the overrides mapping.
    ISchemaOverridesParser schemaOverridesParser = configureSchemaOverrides(options);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema,
            schemaOverridesParser,
            transformationContext,
            shardingContext,
            options.getDatastreamSourceType(),
            customTransformation,
            options.getRoundJsonDecimals(),
            ddlView,
            spannerConfig);

    PCollectionTuple transformedRecords =
        jsonRecords.apply(
            "Apply Transformation to events",
            ParDo.of(changeEventTransformerDoFn)
                .withSideInputs(ddlView)
                .withOutputTags(
                    DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG,
                    TupleTagList.of(
                        Arrays.asList(
                            DatastreamToSpannerConstants.FILTERED_EVENT_TAG,
                            DatastreamToSpannerConstants.PERMANENT_ERROR_TAG))));

    /*
     * Stage 3: Write filtered records to GCS
     */
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String filterEventsDirectory =
        options.getFilteredEventsDirectory().isEmpty()
            ? tempLocation + "filteredEvents/"
            : options.getFilteredEventsDirectory();
    LOG.info("Filtered events directory: {}", filterEventsDirectory);
    TextIO.Write filterEventsWrite;
    if (options.getRunner() != null && options.getRunner().getSimpleName().equals("DirectRunner")) {
      // DirectRunner does not support dynamic sharding for unbounded PCollections
      filterEventsWrite =
          TextIO.write()
              .to(filterEventsDirectory)
              .withSuffix(".json")
              .withWindowedWrites()
              .withNumShards(20);
    } else {
      // Cloud Dataflow natively supports dynamic sharding
      filterEventsWrite =
          TextIO.write().to(filterEventsDirectory).withSuffix(".json").withWindowedWrites();
    }

    transformedRecords
        .get(DatastreamToSpannerConstants.FILTERED_EVENT_TAG)
        .apply(Window.into(FixedWindows.of(org.joda.time.Duration.standardMinutes(1))))
        .apply("Write Filtered Events To GCS", filterEventsWrite);

    spannerConfig =
        SpannerServiceFactoryImpl.createSpannerService(
            spannerConfig, options.getFailureInjectionParameter());
    shadowTableSpannerConfig =
        SpannerServiceFactoryImpl.createSpannerService(
            shadowTableSpannerConfig, options.getFailureInjectionParameter());

    /*
     * Stage 4: Write transformed records to Cloud Spanner
     */
    SpannerTransactionWriter.Result spannerWriteResults =
        transformedRecords
            .get(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG)
            .apply(
                "Write events to Cloud Spanner",
                new SpannerTransactionWriter(
                    spannerConfig,
                    shadowTableSpannerConfig,
                    ddlView,
                    shadowTableDdlView,
                    options.getShadowTablePrefix(),
                    options.getDatastreamSourceType(),
                    isRegularMode));
    /*
     * Stage 5: Write failures to GCS Dead Letter Queue
     * a) Retryable errors are written to retry GCS Dead letter queue
     * b) Severe errors are written to severe GCS Dead letter queue
     */
    // We will write only the original payload from the failsafe event to the DLQ.  We are doing
    // that in
    // StringDeadLetterQueueSanitizer.
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
    // TODO: Write errors from transformer and spanner writer into separate folders
    PCollection<FailsafeElement<String, String>> permanentErrors =
        PCollectionList.of(dlqErrorRecords)
            .and(spannerWriteResults.permanentErrors())
            .and(transformedRecords.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG))
            .apply(Flatten.pCollections());
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
    return pipeline;
  }

  static ShardingContext getShardingContext(DataStreamToSpannerOptions options) {
    // Ingest sharding context file into memory.
    ShardingContext shardingContext = new ShardingContext();
    if (options.getSourceConfigURL() != null && !options.getSourceConfigURL().isEmpty()) {
      try {
        SourceConfigParser parser = new SourceConfigParser(new SecretManagerAccessorImpl());
        SourceConnectionConfig sourceConfig =
            parser.parseConfiguration(
                getSourceType(options), options.getSourceConfigURL(), /* resolveSecrets= */ false);

        if (sourceConfig instanceof JdbcShardConfig jdbcShardConfig) {
          List<Shard> shards = jdbcShardConfig.getShardConfigs();
          Map<String, Map<String, String>> streamToDbAndShardMap = new HashMap<>();
          for (Shard shard : shards) {
            // TO-DO: add checks in SourceConfigParser to ensure not null fields.
            streamToDbAndShardMap
                .computeIfAbsent(shard.getStreamId(), k -> new HashMap<>())
                .put(shard.getDbName(), shard.getLogicalShardId());
          }
          shardingContext = new ShardingContext(streamToDbAndShardMap);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse source config URL", e);
      }
    }
    return shardingContext;
  }

  static SpannerConfig getShadowTableSpannerConfig(DataStreamToSpannerOptions options) {
    // Validate shadow table Spanner config - both instance and database must be specified together
    String shadowTableSpannerInstanceId = options.getShadowTableSpannerInstanceId();
    String shadowTableSpannerDatabaseId = options.getShadowTableSpannerDatabaseId();
    LOG.info(
        "Input Shadow table db -  instance {} and database {}",
        shadowTableSpannerInstanceId,
        shadowTableSpannerDatabaseId);

    if ((Strings.isNullOrEmpty(shadowTableSpannerInstanceId)
            && !Strings.isNullOrEmpty(shadowTableSpannerDatabaseId))
        || (!Strings.isNullOrEmpty(shadowTableSpannerInstanceId)
            && Strings.isNullOrEmpty(shadowTableSpannerDatabaseId))) {
      throw new IllegalArgumentException(
          "Both shadowTableSpannerInstanceId and shadowTableSpannerDatabaseId must be specified together");
    }
    // If not specified, use main instance and main database values. The shadow table database
    // stores the shadow tables and by default, is the same as the main database for backward
    // compatibility.
    if (Strings.isNullOrEmpty(shadowTableSpannerInstanceId)
        && Strings.isNullOrEmpty(shadowTableSpannerDatabaseId)) {
      shadowTableSpannerInstanceId = options.getInstanceId();
      shadowTableSpannerDatabaseId = options.getDatabaseId();
      LOG.info(
          "Overwrote shadow table instance - {} and db- {}",
          shadowTableSpannerInstanceId,
          shadowTableSpannerDatabaseId);
    }
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(shadowTableSpannerInstanceId))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(shadowTableSpannerDatabaseId))
        .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()))
        .withCommitRetrySettings(
            RetrySettings.newBuilder()
                .setTotalTimeout(Duration.ofMinutes(4))
                .setInitialRetryDelay(Duration.ofMinutes(0))
                .setRetryDelayMultiplier(1)
                .setMaxRetryDelay(Duration.ofMinutes(0))
                .setInitialRpcTimeout(Duration.ofMinutes(4))
                .setRpcTimeoutMultiplier(1)
                .setMaxRpcTimeout(Duration.ofMinutes(4))
                .setMaxAttempts(1)
                .build());
  }

  static DeadLetterQueueManager buildDlqManager(DataStreamToSpannerOptions options) {
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
    return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetryCount(), true);
  }

  static ISchemaOverridesParser configureSchemaOverrides(DataStreamToSpannerOptions options) {
    // incorrect configuration
    if (!options.getSchemaOverridesFilePath().isEmpty()
        && (!options.getTableOverrides().isEmpty() || !options.getColumnOverrides().isEmpty())) {
      throw new IllegalArgumentException(
          "Only one of file based or string based overrides must be configured! Please correct the configuration and re-run the job");
    }
    // string based overrides
    if (!options.getTableOverrides().isEmpty() || !options.getColumnOverrides().isEmpty()) {
      Map<String, String> userOptionsOverrides = new HashMap<>();
      if (!options.getTableOverrides().isEmpty()) {
        userOptionsOverrides.put("tableOverrides", options.getTableOverrides());
      }
      if (!options.getColumnOverrides().isEmpty()) {
        userOptionsOverrides.put("columnOverrides", options.getColumnOverrides());
      }
      return new SchemaStringOverridesParser(userOptionsOverrides);
    }
    // file based overrides
    if (!options.getSchemaOverridesFilePath().isEmpty()) {
      return new SchemaFileOverridesParser(options.getSchemaOverridesFilePath());
    }
    // no overrides
    return new NoopSchemaOverridesParser();
  }
}
