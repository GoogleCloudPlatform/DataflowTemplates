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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_REGULAR;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_RETRY_ALL_DLQ;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.RUN_MODE_RETRY_DLQ;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.options.SpannerToSourceDbOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.PubSubNotifiedDlqIO;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.CommonTemplateJvmInitializer;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.DataflowWorkerMachineTypeUtils;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessorFactory;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.ConvertChangeStreamErrorRecordToFailsafeElementFn;
import com.google.cloud.teleport.v2.templates.transforms.ConvertDlqRecordToTrimmedShardedDataChangeRecordFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.SourceWriterTransform;
import com.google.cloud.teleport.v2.templates.transforms.SpannerInformationSchemaProcessorTransform;
import com.google.cloud.teleport.v2.templates.transforms.UpdateDlqMetricsFn;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerServiceFactoryImpl;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This pipeline reads Spanner Change streams data and writes them to a source DB. */
@Template(
    name = "Spanner_to_SourceDb",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Source Database",
    description =
        "Streaming pipeline. Reads data from Spanner Change Streams and"
            + " writes them to a source.",
    optionsClass = SpannerToSourceDbOptions.class,
    flexContainerName = "spanner-to-sourcedb",
    contactInformation = "https://cloud.google.com/support",
    hidden = false,
    streaming = true)
public class SpannerToSourceDb {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb.class);

  // JDBC Drivers
  private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

  // JDBC URL Prefixes
  private static final String MYSQL_JDBC_PREFIX = "jdbc:mysql://";
  private static final String POSTGRESQL_JDBC_PREFIX = "jdbc:postgresql://";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sink");

    SpannerToSourceDbOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToSourceDbOptions.class);

    // Stage SSL certificates to extraFiles if required as per the pipeline options.
    // Ref https://cloud.google.com/dataflow/docs/guides/templates/ssl-certificates
    new CommonTemplateJvmInitializer().beforeProcessing(options);

    boolean isRetryDLQMode = RUN_MODE_RETRY_DLQ.equals(options.getRunMode());
    options.setStreaming(!isRetryDLQMode);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(SpannerToSourceDbOptions options) {
    long startTime = System.currentTimeMillis();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);

    // calculate the max connections per worker
    int maxNumWorkers =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers() > 0
            ? pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers()
            : 1;
    int connectionPoolSizePerWorker =
        calculateConnectionPoolSizePerWorker(options.getMaxShardConnections(), maxNumWorkers);

    String workerMachineType =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getWorkerMachineType();
    Optional<Integer> resourceHintsMinCpus =
        DataflowWorkerMachineTypeUtils.getMinCpuResourceHint(pipeline.getOptions());
    DataflowWorkerMachineTypeUtils.validateMachineSpecs(workerMachineType, 4, resourceHintsMinCpus);

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));

    // Create shadow tables
    // Note that there is a limit on the number of tables that can be created per DB: 5000.
    // If we create shadow tables per shard, there will be an explosion of tables.
    // Anyway the shadow table has Spanner PK so no need to again separate by the shard
    // Lookup by the Spanner PK should be sufficient.

    // Prepare Spanner config
    SpannerConfig spannerMetadataConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getMetadataInstance()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getMetadataDatabase()))
            .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));

    // Fetch DDLs and create shadow tables in a DoFn to avoid launcher-side timeout.
    PCollectionTuple ddlTuple =
        pipeline.apply(
            "Process Information Schema",
            new SpannerInformationSchemaProcessorTransform(
                spannerConfig, spannerMetadataConfig, options.getShadowTablePrefix()));

    final PCollectionView<Ddl> ddlView =
        ddlTuple
            .get(SpannerInformationSchemaProcessorTransform.MAIN_DDL_TAG)
            .apply("View Main DDL", View.asSingleton());

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);

    final PCollectionView<Ddl> shadowTableDdlView =
        ddlTuple
            .get(SpannerInformationSchemaProcessorTransform.SHADOW_TABLE_DDL_TAG)
            .apply("View Shadow DDL", View.asSingleton());

    List<Shard> shards;
    ISpToSrcSourceConnector sourceConnector;
    try {
      sourceConnector = SourceProcessorFactory.getSource(options.getSourceType());
      shards = sourceConnector.parseShardConfig(options.getSourceShardsFilePath());
    } catch (Exception e) {
      throw new RuntimeException("Error parsing shard list", e);
    }

    if (shards == null || shards.isEmpty()) {
      LOG.error("Shard list should have at least 1 element.");
      throw new IllegalArgumentException("Shard list should have at least 1 element.");
    }

    String shardingMode =
        sourceConnector.supportsSharding()
            ? Constants.SHARDING_MODE_MULTI_SHARD
            : Constants.SHARDING_MODE_SINGLE_SHARD;

    try {
      sourceConnector.validate(shards, options);
    } catch (Exception e) {
      throw new RuntimeException("Validation failed", e);
    }

    SourceSchema sourceSchema;
    try {
      sourceSchema = sourceConnector.getInformationSchema(shards);
    } catch (Exception e) {
      throw new RuntimeException("Error fetching source schema", e);
    }
    LOG.info("Source schema: {}", sourceSchema);

    if (shards.size() == 1 && !options.getIsShardedMigration()) {
      shardingMode = Constants.SHARDING_MODE_SINGLE_SHARD;
      Shard shard = shards.get(0);
      if (shard.getLogicalShardId() == null || shard.getLogicalShardId().isEmpty()) {
        shard.setLogicalShardId(Constants.DEFAULT_SHARD_ID);
        LOG.info(
            "Logical shard id was not found, hence setting it to : " + Constants.DEFAULT_SHARD_ID);
      }
    }

    buildPipeline(
        pipeline,
        options,
        sourceSchema,
        shards,
        ddlView,
        shadowTableDdlView,
        spannerConfig,
        spannerMetadataConfig,
        connectionPoolSizePerWorker,
        shardingMode,
        startTime,
        maxNumWorkers);

    return pipeline.run();
  }

  static void buildPipeline(
      Pipeline pipeline,
      SpannerToSourceDbOptions options,
      SourceSchema sourceSchema,
      List<Shard> shards,
      PCollectionView<Ddl> ddlView,
      PCollectionView<Ddl> shadowTableDdlView,
      SpannerConfig spannerConfig,
      SpannerConfig spannerMetadataConfig,
      int connectionPoolSizePerWorker,
      String shardingMode,
      long startTime,
      int maxNumWorkers) {

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    boolean isRegularMode = RUN_MODE_REGULAR.equals(options.getRunMode());
    PCollectionTuple reconsumedElements = null;
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    int reshuffleBucketSize =
        maxNumWorkers
            * (debugOptions.getNumberOfWorkerHarnessThreads() > 0
                ? debugOptions.getNumberOfWorkerHarnessThreads()
                : Constants.DEFAULT_WORKER_HARNESS_THREAD_COUNT);

    if (isRegularMode && (!Strings.isNullOrEmpty(options.getDlqGcsPubSubSubscription()))) {
      reconsumedElements =
          dlqManager.getReconsumerDataTransformForFiles(
              pipeline.apply(
                  "Read retry from PubSub",
                  new PubSubNotifiedDlqIO(
                      options.getDlqGcsPubSubSubscription(),
                      // file paths to ignore when re-consuming for retry
                      new ArrayList<String>(
                          Arrays.asList(
                              "/severe/",
                              "/tmp_retry",
                              "/tmp_severe/",
                              ".temp",
                              "/tmp_skip/",
                              "/" + options.getSkipDirectoryName())))));
    } else {
      if (isRegularMode) {
        reconsumedElements =
            dlqManager.getReconsumerDataTransform(
                pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
      } else { // retryDLQ or retryAllDLQ mode
        PCollection<String> oneShotRecords =
            pipeline.apply("Read severe from OneShot", dlqManager.dlqOneShotReconsumer(startTime));

        if (RUN_MODE_RETRY_DLQ.equals(options.getRunMode())) {
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

    PCollection<FailsafeElement<String, String>> dlqJsonStrRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.RETRYABLE_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<TrimmedShardedDataChangeRecord> dlqRecords =
        dlqJsonStrRecords.apply(
            "Convert DLQ records to TrimmedShardedDataChangeRecord",
            ParDo.of(new ConvertDlqRecordToTrimmedShardedDataChangeRecordFn()));
    PCollection<TrimmedShardedDataChangeRecord> mergedRecords = null;

    if (options.getFailureInjectionParameter() != null
        && !options.getFailureInjectionParameter().isBlank()) {
      spannerConfig =
          SpannerServiceFactoryImpl.createSpannerService(
              spannerConfig, options.getFailureInjectionParameter());
    }

    if (isRegularMode) {
      PCollection<TrimmedShardedDataChangeRecord> changeRecordsFromDB =
          pipeline
              .apply(
                  getReadChangeStreamDoFn(
                      options,
                      spannerConfig)) // This emits PCollection<DataChangeRecord> which is Spanner
              // change
              // stream data
              .apply("Reshuffle", Reshuffle.viaRandomKey())
              .apply("Filteration", ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
              .apply("Preprocess", ParDo.of(new PreprocessRecordsFn()));
      mergedRecords =
          PCollectionList.of(changeRecordsFromDB)
              .and(dlqRecords)
              .apply("Flatten", Flatten.pCollections());
    } else {
      mergedRecords = dlqRecords;
    }
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();

    if (options.getFailureInjectionParameter() != null
        && !options.getFailureInjectionParameter().isBlank()) {
      spannerMetadataConfig =
          SpannerServiceFactoryImpl.createSpannerService(
              spannerMetadataConfig, options.getFailureInjectionParameter());
    }

    SourceWriterTransform.Result sourceWriterOutput =
        mergedRecords
            .apply(
                "AssignShardId", // This emits PCollection<KV<Long,
                // TrimmedShardedDataChangeRecord>> which is Spanner change stream data with key as
                // PK
                // mod
                // number of parallelism
                ParDo.of(
                        new AssignShardIdFn(
                            spannerConfig,
                            ddlView,
                            sourceSchema,
                            shardingMode,
                            shards.get(0).getLogicalShardId(),
                            options.getSkipDirectoryName(),
                            options.getShardingCustomJarPath(),
                            options.getShardingCustomClassName(),
                            options.getShardingCustomParameters(),
                            options.getMaxShardConnections() * shards.size(),
                            options.getSourceType(),
                            options.getSessionFilePath(),
                            options.getSchemaOverridesFilePath(),
                            options.getTableOverrides(),
                            options
                                .getColumnOverrides())) // currently assume that all shards accept
                    // the
                    // same source type
                    .withSideInputs(ddlView))
            .setCoder(
                KvCoder.of(VarLongCoder.of(), AvroCoder.of(TrimmedShardedDataChangeRecord.class)))
            .apply("Reshuffle2", Reshuffle.of())
            .apply(
                "Write to source",
                new SourceWriterTransform(
                    shards,
                    spannerMetadataConfig,
                    options.getSourceDbTimezoneOffset(),
                    ddlView,
                    shadowTableDdlView,
                    sourceSchema,
                    options.getShadowTablePrefix(),
                    options.getSkipDirectoryName(),
                    connectionPoolSizePerWorker,
                    options.getSourceType(),
                    customTransformation,
                    options.getSessionFilePath(),
                    options.getSchemaOverridesFilePath(),
                    options.getTableOverrides(),
                    options.getColumnOverrides()));

    PCollection<FailsafeElement<String, String>> dlqPermErrorRecords =
        reconsumedElements
            .get(DeadLetterQueueManager.PERMANENT_ERRORS)
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<FailsafeElement<String, String>> permErrorsFromSourceWriter =
        sourceWriterOutput
            .permanentErrors()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle3", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert permanent errors from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<FailsafeElement<String, String>> permanentErrors =
        PCollectionList.of(dlqPermErrorRecords)
            .and(permErrorsFromSourceWriter)
            .apply(Flatten.pCollections())
            .apply("Reshuffle", Reshuffle.viaRandomKey());

    permanentErrors
        .apply("Update DLQ metrics", ParDo.of(new UpdateDlqMetricsFn(isRegularMode)))
        .apply(
            "DLQ: Write Severe errors to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ for severe errors",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory((options).getDeadLetterQueueDirectory() + "/tmp_severe/")
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> retryErrors =
        sourceWriterOutput
            .retryableErrors()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle4", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert retryable errors from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    retryErrors
        .apply(
            "DLQ: Write retryable Failures to GCS",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Write To DLQ for retryable errors",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getRetryDlqDirectoryWithDateTime())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_retry/")
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> skippedRecords =
        sourceWriterOutput
            .skippedSourceWrites()
            .setCoder(StringUtf8Coder.of())
            .apply(
                "Reshuffle5", Reshuffle.<String>viaRandomKey().withNumBuckets(reshuffleBucketSize))
            .apply(
                "Convert skipped records from source writer to DLQ format",
                ParDo.of(new ConvertChangeStreamErrorRecordToFailsafeElementFn()))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    skippedRecords
        .apply(
            "Write skipped records to GCS", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            "Writing skipped records to GCS",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(
                    options.getDeadLetterQueueDirectory() + "/" + options.getSkipDirectoryName())
                .withTmpDirectory(options.getDeadLetterQueueDirectory() + "/tmp_skip/")
                .setIncludePaneInfo(true)
                .build());
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      SpannerToSourceDbOptions options, SpannerConfig spannerConfig) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    String changeStreamMetadataDb = options.getChangeStreamMetadataDatabase();
    if (Strings.isNullOrEmpty(changeStreamMetadataDb)) {
      changeStreamMetadataDb = options.getMetadataDatabase();
    }
    LOG.info("Using database {} for change stream metadata.", changeStreamMetadataDb);

    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(options.getChangeStreamName())
            .withMetadataInstance(options.getMetadataInstance())
            .withMetadataDatabase(changeStreamMetadataDb)
            .withInclusiveStartAt(startTime)
            .withRpcPriority(options.getSpannerPriority());

    if (options.getSpannerMetadataTableName() != null
        && !options.getSpannerMetadataTableName().isEmpty()) {
      readChangeStreamDoFn =
          readChangeStreamDoFn.withMetadataTable(options.getSpannerMetadataTableName());
    }
    if (!options.getEndTimestamp().equals("")) {
      return readChangeStreamDoFn.withInclusiveEndAt(
          Timestamp.parseTimestamp(options.getEndTimestamp()));
    }
    return readChangeStreamDoFn;
  }

  static DeadLetterQueueManager buildDlqManager(SpannerToSourceDbOptions options) {
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

  static int calculateConnectionPoolSizePerWorker(Long maxShardConnections, int maxNumWorkers) {
    int connectionPoolSizePerWorker = (int) (maxShardConnections / maxNumWorkers);
    if (connectionPoolSizePerWorker < 1) {
      throw new IllegalArgumentException(
          "Max Dataflow workers "
              + maxNumWorkers
              + " is more than max per shard connections: "
              + maxShardConnections
              + " this can lead to more"
              + " database connections than desired. Either reduce the max allowed workers or"
              + " incease the max shard connections");
    }
    return connectionPoolSizePerWorker;
  }
}
