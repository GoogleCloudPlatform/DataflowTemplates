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

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.SourceWriterFn;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
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
    optionsClass = Options.class,
    flexContainerName = "spanner-to-sourcedb",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class SpannerToSourceDb {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Name of the change stream to read from",
        helpText =
            "This is the name of the Spanner change stream that the pipeline will read from.")
    String getChangeStreamName();

    void setChangeStreamName(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changestream is present.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database that the changestream is monitoring")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 4,
        optional = false,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getSpannerProjectId();

    void setSpannerProjectId(String projectId);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        description = "Cloud Spanner Instance to store metadata when reading from changestreams",
        helpText =
            "This is the instance to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataInstance();

    void setMetadataInstance(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "Cloud Spanner Database to store metadata when reading from changestreams",
        helpText =
            "This is the database to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataDatabase();

    void setMetadataDatabase(String value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Changes are read from the given timestamp",
        helpText = "Read changes from the given timestamp.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Changes are read until the given timestamp",
        helpText =
            "Read changes until the given timestamp. If no timestamp provided, reads indefinitely.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Cloud Spanner shadow table prefix.",
        helpText = "The prefix used to name shadow tables. Default: `shadow_`.")
    @Default.String("shadow_")
    String getShadowTablePrefix();

    void setShadowTablePrefix(String value);

    @TemplateParameter.GcsReadFile(
        order = 10,
        optional = false,
        description = "Path to GCS file containing the the Source shard details",
        helpText = "Path to GCS file containing connection profile info for source shards.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 11,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 12,
        optional = true,
        enumOptions = {@TemplateEnumOption("none"), @TemplateEnumOption("forward_migration")},
        description = "Filtration mode",
        helpText =
            "Mode of Filtration, decides how to drop certain records based on a criteria. Currently"
                + " supported modes are: none (filter nothing), forward_migration (filter records"
                + " written via the forward migration pipeline). Defaults to forward_migration.")
    @Default.String("forward_migration")
    String getFiltrationMode();

    void setFiltrationMode(String value);

    @TemplateParameter.GcsReadFile(
        order = 13,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the customization logic"
                + " for fetching shard id.")
    @Default.String("")
    String getShardingCustomJarPath();

    void setShardingCustomJarPath(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom shard id implementation.  It is a"
                + " mandatory field in case shardingCustomJarPath is specified")
    @Default.String("")
    String getShardingCustomClassName();

    void setShardingCustomClassName(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Custom sharding logic parameters",
        helpText =
            "String containing any custom parameters to be passed to the custom sharding class.")
    @Default.String("")
    String getShardingCustomParameters();

    void setShardingCustomParameters(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.PubsubSubscription(
        order = 17,
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

    @TemplateParameter.Text(
        order = 18,
        optional = true,
        description = "Directory name for holding skipped records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("skip")
    String getSkipDirectoryName();

    void setSkipDirectoryName(String value);

    @TemplateParameter.Long(
        order = 19,
        optional = true,
        description = "Maximum connections per shard.",
        helpText = "This will come from shard file eventually.")
    @Default.Long(10000)
    Long getMaxShardConnections();

    void setMaxShardConnections(Long value);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The file path used when storing the error queue output. "
                + "The default file path is a directory under the Dataflow job's temp location.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sink");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);

    // Read the session file
    Schema schema = SessionFileReader.read(options.getSessionFilePath());

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

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
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getMetadataDatabase()));

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            spannerConfig,
            spannerMetadataConfig,
            SpannerAccessor.getOrCreate(spannerMetadataConfig)
                .getDatabaseAdminClient()
                .getDatabase(
                    spannerMetadataConfig.getInstanceId().get(),
                    spannerMetadataConfig.getDatabaseId().get())
                .getDialect(),
            options.getShadowTablePrefix());

    shadowTableCreator.createShadowTablesInSpanner();
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails(options.getSourceShardsFilePath());
    String shardingMode = Constants.SHARDING_MODE_SINGLE_SHARD;
    if (shards.size() > 1) {
      shardingMode = Constants.SHARDING_MODE_MULTI_SHARD;
    }
    pipeline
        .apply(
            getReadChangeStreamDoFn(
                options,
                spannerConfig)) // This emits PCollection<DataChangeRecord> which is Spanner change
        // stream data
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        .apply(ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
        .apply(ParDo.of(new PreprocessRecordsFn())) // This emits
        // PCollection<TrimmedShardedDataChangeRecord> which is
        // Spanner change stream data with only needed fields
        .setCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class))
        .apply(
            ParDo.of(
                new AssignShardIdFn(
                    spannerConfig,
                    schema,
                    ddl,
                    shardingMode,
                    shards.get(0).getLogicalShardId(),
                    options.getSkipDirectoryName(),
                    options.getShardingCustomJarPath(),
                    options.getShardingCustomClassName(),
                    options.getShardingCustomParameters(),
                    options.getMaxShardConnections()))) // This emits PCollection<KV<Long,
        // TrimmedShardedDataChangeRecord>> which is Spanner change stream data with key as PK mod
        // number of parallelism
        .apply("Reshuffle2", Reshuffle.of())
        .apply(
            ParDo.of(
                new SourceWriterFn(
                    shards,
                    schema,
                    spannerMetadataConfig,
                    options.getSourceDbTimezoneOffset(),
                    ddl,
                    options
                        .getShadowTablePrefix()))); // This writes to source, will emit DLQ records
    // in future
    // TODO: write a DLQ writer and a DLQ reader
    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      Options options, SpannerConfig spannerConfig) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(options.getChangeStreamName())
            .withMetadataInstance(options.getMetadataInstance())
            .withMetadataDatabase(options.getMetadataDatabase())
            .withInclusiveStartAt(startTime);
    if (!options.getEndTimestamp().equals("")) {
      return readChangeStreamDoFn.withInclusiveEndAt(
          Timestamp.parseTimestamp(options.getEndTimestamp()));
    }
    return readChangeStreamDoFn;
  }
}
