/*
 * Copyright (C) 2023 Google LLC
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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.metadata.SpannerToGcsJobMetadata;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.SpannerChangeStreamsToShardedFileSink.Options;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.ChangeDataProgressTrackerFn;
import com.google.cloud.teleport.v2.templates.transforms.FileProgressTrackerFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.WriterGCS;
import com.google.cloud.teleport.v2.templates.utils.FileCreationTracker;
import com.google.cloud.teleport.v2.templates.utils.InformationSchemaReader;
import com.google.cloud.teleport.v2.templates.utils.JobMetadataUpdater;
import com.google.cloud.teleport.v2.templates.utils.SpannerDao;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import java.util.List;
import java.util.regex.Pattern;
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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests Spanner Change streams data and writes them to a GCS file sink sharded by
 * the logical shards.
 */
@Template(
    name = "Spanner_Change_Streams_to_Sharded_File_Sink",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Sharded File Sink",
    description =
        "Streaming pipeline. Ingests data from Spanner Change Streams, splits them into shards and"
            + " intervals , and writes them to a file sink.",
    optionsClass = Options.class,
    flexContainerName = "spanner-change-streams-to-sharded-file-sink",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-change-streams-to-sharded-file-sink",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class SpannerChangeStreamsToShardedFileSink {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamsToShardedFileSink.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        optional = false,
        description = "Name of the change stream to read from",
        helpText =
            "This is the name of the Spanner change stream that the pipeline will read from.")
    String getChangeStreamName();

    void setChangeStreamName(String value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        optional = false,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changestream is present.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        optional = false,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database that the changestream is monitoring")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 4,
        groupName = "Source",
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

    @TemplateParameter.GcsReadFile(
        order = 9,
        optional = true,
        description = "Session File Path in Cloud Storage, needed for sharded reverse replication",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge. Needed when doing sharded reverse replication.")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Duration(
        order = 10,
        optional = true,
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed"
                + " formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh"
                + " (for hours, example: 2h).",
        example = "5m")
    @Default.String("10s")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.GcsWriteFolder(
        order = 11,
        groupName = "Target",
        optional = false,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path/")
    String getGcsOutputDirectory();

    void setGcsOutputDirectory(String gcsOutputDirectory);

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
        optional = false,
        description = "Source shard details file path in Cloud Storage",
        helpText =
            "Source shard details file path in Cloud Storage that contains connection profile of"
                + " source shards. Atleast one shard information is expected.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Metadata table suffix",
        helpText =
            "Suffix appended to the spanner_to_gcs_metadata and shard_file_create_progress metadata"
                + " tables.Useful when doing multiple runs.Only alpha numeric and underscores"
                + " are allowed.")
    @Default.String("")
    String getMetadataTableSuffix();

    void setMetadataTableSuffix(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Directory name for holding skipped records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("skip")
    String getSkipDirectoryName();

    void setSkipDirectoryName(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = false,
        description = "Reverse replication run identifier",
        helpText =
            "The identifier to distinguish between different runs of reverse replication flows.")
    String getRunIdentifier();

    void setRunIdentifier(String value);

    @TemplateParameter.Enum(
        order = 17,
        optional = true,
        enumOptions = {@TemplateEnumOption("regular"), @TemplateEnumOption("resume")},
        description =
            "This is the type of run mode. Supported values are regular and resume. Default is"
                + " regular.",
        helpText = "Regular starts from input start time, resume start from last processed time.")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.GcsReadFile(
        order = 18,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the customization logic"
                + " for fetching shard id.")
    @Default.String("")
    String getShardingCustomJarPath();

    void setShardingCustomJarPath(String value);

    @TemplateParameter.Text(
        order = 19,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom shard id implementation.  It is a"
                + " mandatory field in case shardingCustomJarPath is specified")
    @Default.String("")
    String getShardingCustomClassName();

    void setShardingCustomClassName(String value);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        description = "Custom sharding logic parameters",
        helpText =
            "String containing any custom parameters to be passed to the custom sharding class.")
    @Default.String("")
    String getShardingCustomParameters();

    void setShardingCustomParameters(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sharded file sink");

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
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails(options.getSourceShardsFilePath());
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("The source shards file cannot be empty");
    }

    String shardingMode = Constants.SHARDING_MODE_SINGLE_SHARD;
    if (shards.size() > 1) {
      shardingMode = Constants.SHARDING_MODE_MULTI_SHARD;
    }

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

    SpannerConfig spannerMetadataConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getMetadataInstance()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getMetadataDatabase()));
    boolean isMetadataDbPostgres =
        Dialect.POSTGRESQL
            == SpannerAccessor.getOrCreate(spannerMetadataConfig)
                .getDatabaseAdminClient()
                .getDatabase(
                    spannerMetadataConfig.getInstanceId().get(),
                    spannerMetadataConfig.getDatabaseId().get())
                .getDialect();

    Schema schema = null;
    Ddl ddl = null;
    if (shardingMode.equals(Constants.SHARDING_MODE_MULTI_SHARD)) {
      schema = SessionFileReader.read(options.getSessionFilePath());
      ddl = InformationSchemaReader.getInformationSchemaAsDdl(spannerConfig);
    }

    String tableSuffix = "";
    if (options.getMetadataTableSuffix() != null && !options.getMetadataTableSuffix().isEmpty()) {

      tableSuffix = options.getMetadataTableSuffix();
      if (!Pattern.compile("[a-zA-Z0-9_]+").matcher(tableSuffix).matches()) {
        throw new RuntimeException(
            "Only alpha numeric and underscores allowed in metadataTableSuffix, however found : "
                + tableSuffix);
      }
    }

    // Have a common start time stamp when updating the metadata tables
    // And when reading from change streams
    SpannerDao spannerDao =
        new SpannerDao(
            options.getSpannerProjectId(),
            options.getMetadataInstance(),
            options.getMetadataDatabase(),
            tableSuffix,
            isMetadataDbPostgres);
    SpannerToGcsJobMetadata jobMetadata = getStartTimeAndDuration(options, spannerDao);

    // Capture the window start time and duration config.
    // This is read by the GCSToSource template to ensure the same config is used in both templates.
    if (options.getRunMode().equals(Constants.RUN_MODE_REGULAR)) {
      JobMetadataUpdater.writeStartAndDuration(spannerDao, options.getRunIdentifier(), jobMetadata);
    }

    // Initialize the per shard progress with historical value
    // This makes it easier to fire blind UPDATES later on when
    // updating per shard file creation progress
    FileCreationTracker fileCreationTracker =
        new FileCreationTracker(spannerDao, options.getRunIdentifier());
    fileCreationTracker.init(shards);

    spannerDao.close();

    pipeline
        .apply(
            getReadChangeStreamDoFn(
                options, spannerConfig, Timestamp.parseTimestamp(jobMetadata.getStartTimestamp())))
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        .apply(ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
        .apply(ParDo.of(new PreprocessRecordsFn()))
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
                    options.getShardingCustomParameters())))
        .apply(
            "Creating " + options.getWindowDuration() + " Window",
            Window.into(
                FixedWindows.of(DurationUtils.parseDuration(jobMetadata.getWindowDuration()))))
        .apply(
            "Tracking change data seen",
            ParDo.of(
                new ChangeDataProgressTrackerFn(
                    spannerMetadataConfig,
                    tableSuffix,
                    options.getRunIdentifier(),
                    isMetadataDbPostgres)))
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        .apply(
            "Write To GCS",
            WriterGCS.newBuilder()
                .withGcsOutputDirectory(options.getGcsOutputDirectory())
                .withTempLocation(options.getTempLocation())
                .build())
        .apply(
            "Creating file tracking window",
            Window.into(
                FixedWindows.of(DurationUtils.parseDuration(jobMetadata.getWindowDuration()))))
        .apply(
            "Tracking file progress ",
            ParDo.of(
                new FileProgressTrackerFn(
                    spannerMetadataConfig,
                    tableSuffix,
                    options.getRunIdentifier(),
                    isMetadataDbPostgres)));
    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      Options options, SpannerConfig spannerConfig, Timestamp startTime) {

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

  /**
   * This method gets the start time of reading change streams and the window duration.
   *
   * <p>In the regular mode, they are taken from the pipeline input. If start time is not there in
   * input, it takes the current time.
   *
   * <p>In the resume mode, the duration is taken from the original run via spanner_to_gcs_metadata
   * table. The start time is taken based on the what is the minimum duration processed by the
   * writer job. This is the most deterministic way to know the starting point. We cannot rely on
   * the file creation metadata tables since files can be created in parallel for various windows.
   *
   * <p>In the scenario where pause was triggered before the writer job could start,the start time
   * cannot be determined by looking at shard_file_process_progress table. In this case, the start
   * time is taken from the original run via spanner_to_gcs_metadata.
   */
  private static SpannerToGcsJobMetadata getStartTimeAndDuration(
      Options options, SpannerDao spannerDao) {
    Timestamp startTime = Timestamp.now();
    String duration = options.getWindowDuration();
    if (options.getRunMode().equals(Constants.RUN_MODE_REGULAR)) {
      if (!options.getStartTimestamp().equals("")) {
        startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
      }
    } else {
      // resume run mode
      // Get the original job start time and duration
      SpannerToGcsJobMetadata jobMetadata =
          spannerDao.getSpannerToGcsJobMetadata(options.getRunIdentifier());
      if (jobMetadata == null) {
        throw new RuntimeException(
            "Could not read from spanner_to_gcs_metadata table. This table should exist when"
                + " running in resume mode.");
      }
      duration = jobMetadata.getWindowDuration();
      startTime = spannerDao.getMinimumStartTimeAcrossAllShards(options.getRunIdentifier());
      if (startTime == null) {
        // This happens when the pause was called before writer job could start
        // In this case, fallback to the start time of the original run
        startTime = Timestamp.parseTimestamp(jobMetadata.getStartTimestamp());
      }
    }
    LOG.info(
        "Run mode {} and startTime {} and duration {} ", options.getRunMode(), startTime, duration);
    SpannerToGcsJobMetadata response = new SpannerToGcsJobMetadata(startTime.toString(), duration);
    return response;
  }
}
