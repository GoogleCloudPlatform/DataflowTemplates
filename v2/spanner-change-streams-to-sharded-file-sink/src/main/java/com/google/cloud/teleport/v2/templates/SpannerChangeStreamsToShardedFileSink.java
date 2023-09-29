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
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.SpannerChangeStreamsToShardedFileSink.Options;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.FileProgressTracker;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.WriterGCS;
import com.google.cloud.teleport.v2.templates.utils.FileCreationTracker;
import com.google.cloud.teleport.v2.templates.utils.InformationSchemaReader;
import com.google.cloud.teleport.v2.templates.utils.JobMetadataUpdater;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
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
    hidden = true)
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

    @TemplateParameter.GcsReadFile(
        order = 9,
        optional = false,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
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
                + " source shards")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);
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

    Schema schema = SessionFileReader.read(options.getSessionFilePath());

    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

    Ddl ddl = InformationSchemaReader.getInformationSchemaAsDdl(spannerConfig);

    // Capture the window start time and duration config.
    // This is read by the GCSToSource template to ensure the same config is used in both templates.
    JobMetadataUpdater.writeStartAndDuration(
        options.getSpannerProjectId(),
        options.getMetadataInstance(),
        options.getMetadataDatabase(),
        options.getStartTimestamp(),
        options.getWindowDuration());

    // Initialize the per shard progress with historical value
    // This makes it easier to fire blind UPDATES later on when
    // updating per shard file creation progress
    FileCreationTracker fileCreationTracker =
        new FileCreationTracker(
            options.getSpannerProjectId(),
            options.getMetadataInstance(),
            options.getMetadataDatabase());
    fileCreationTracker.init(options.getSourceShardsFilePath());

    pipeline
        .apply(getReadChangeStreamDoFn(options, spannerConfig))
        .apply(ParDo.of(new FilterRecordsFn(options.getFiltrationMode())))
        .apply(ParDo.of(new PreprocessRecordsFn()))
        .setCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class))
        .apply(ParDo.of(new AssignShardIdFn(spannerConfig, schema, ddl)))
        .apply(
            "Creating " + options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply(
            "Write To GCS",
            WriterGCS.newBuilder()
                .withGcsOutputDirectory(options.getGcsOutputDirectory())
                .withTempLocation(options.getTempLocation())
                .build())
        .apply(
            "Creating file tracking window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply(
            "Tracking file progress ",
            ParDo.of(
                new FileProgressTracker(
                    options.getSpannerProjectId(),
                    options.getMetadataInstance(),
                    options.getMetadataDatabase())));

    // TODO: add metrics
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
