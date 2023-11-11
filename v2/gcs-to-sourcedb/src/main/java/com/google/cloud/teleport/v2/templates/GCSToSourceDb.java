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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.GCSToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.transforms.GcsToSourceStreamer;
import com.google.cloud.teleport.v2.templates.utils.ProcessingContextGenerator;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests GCS data containing change stream records to source database - currently
 * MySQL.
 *
 * <p>NOTE: Future versions will support: Postgres,Oracle
 */
@Template(
    name = "GCS_to_Sourcedb",
    category = TemplateCategory.STREAMING,
    displayName = "GCS to Source DB",
    description =
        "Streaming pipeline. Reads Spanner change stream messages from GCS, orders them,"
            + " transforms them, and writes them to a Source Database like MySQL.",
    optionsClass = Options.class,
    flexContainerName = "gcs-to-sourcedb",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-to-sourcedb",
    contactInformation = "https://cloud.google.com/support",
    hidden = true)
public class GCSToSourceDb {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDb.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = false,
        description = "Source shard details file path in Cloud Storage",
        helpText =
            "Source shard details file path in Cloud Storage that contains connection profile of"
                + " source shards")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 2,
        optional = false,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 3,
        optional = true,
        description = "Destination source type",
        enumOptions = {@TemplateEnumOption("mysql")},
        helpText = "This is the type of source databse.Currently only" + " mysql is supported.")
    @Default.String("mysql")
    String getSourceType();

    void setSourceType(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.Integer(
        order = 5,
        optional = true,
        description = "Duration in seconds between calls to stateful timer processing. ",
        helpText =
            "Controls the time between successive polls to buffer and processing of the resultant"
                + " records.")
    @Default.Integer(1)
    Integer getTimerInterval();

    void setTimerInterval(Integer value);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description =
            "File start timestamp, takes precedence if provided, else value from"
                + " spanner_to_gcs_metadata is considered, for regular mode.",
        helpText =
            "Start time of file for all shards. If not provided, the value is taken from"
                + " spanner_to_gcs_metadata. If provided, this takes precedence. To be given when"
                + " running in regular run mode.")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Duration(
        order = 7,
        optional = true,
        description =
            "File increment window duration,takes precedence if provided, else value from"
                + " spanner_to_gcs_metadata is considered, for regular mode.",
        helpText =
            "The window duration/size in which data is written to Cloud Storage. Allowed"
                + " formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh"
                + " (for hours, example: 2h). If not provided, the value is taken from"
                + " spanner_to_gcs_metadata. If provided, this takes precedence. To be given when"
                + " running in regular run mode.",
        example = "5m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.Text(
        order = 8,
        optional = false,
        description = "GCS input directory path",
        helpText = "Path from where to read the change stream files.")
    String getGCSInputDirectoryPath();

    void setGCSInputDirectoryPath(String value);

    @TemplateParameter.ProjectId(
        order = 9,
        optional = false,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getSpannerProjectId();

    void setSpannerProjectId(String projectId);

    @TemplateParameter.Text(
        order = 10,
        optional = false,
        description = "Cloud Spanner Instance to store the shard progress when reading from gcs",
        helpText = "This is the instance to store the shard progress of the files processed.")
    String getMetadataInstance();

    void setMetadataInstance(String value);

    @TemplateParameter.Text(
        order = 11,
        optional = false,
        description = "Cloud Spanner Database to store the shard progress when reading from gcs",
        helpText = "This is the database to store  the shard progress of the files processed..")
    String getMetadataDatabase();

    void setMetadataDatabase(String value);

    @TemplateParameter.Enum(
        order = 12,
        optional = true,
        enumOptions = {@TemplateEnumOption("regular"), @TemplateEnumOption("reprocess")},
        description = "This type of run mode. Supported values - regular/reprocess.",
        helpText = "Regular writes to source db, reprocess erred shards")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Metadata table suffix",
        helpText =
            "Suffix appended to the spanner_to_gcs_metadata and shard_file_create_progress metadata"
                + " tables.Useful when doing multiple runs.Only alpha numeric and underscores are"
                + " allowed.")
    @Default.String("")
    String getMetadataTableSuffix();

    void setMetadataTableSuffix(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = false,
        description = "Reverse replication run identifier",
        helpText =
            "The identifier to distinguish between different runs of reverse replication flows.")
    String getRunIdentifier();

    void setRunIdentifier(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Ordered Changestream Buffer to SourceDb");

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
    String tableSuffix = "";
    if (options.getMetadataTableSuffix() != null && !options.getMetadataTableSuffix().isEmpty()) {
      tableSuffix = options.getMetadataTableSuffix();
      if (!Pattern.compile("[a-zA-Z0-9_]+").matcher(tableSuffix).matches()) {
        throw new RuntimeException(
            "Only alpha numeric and underscores allowed in metadataTableSuffix, however found : "
                + tableSuffix);
      }
    }

    Map<String, ProcessingContext> processingContextMap = null;
    processingContextMap =
        ProcessingContextGenerator.getProcessingContextForGCS(
            options.getSourceShardsFilePath(),
            options.getSourceType(),
            options.getSessionFilePath(),
            options.getSourceDbTimezoneOffset(),
            options.getStartTimestamp(),
            options.getWindowDuration(),
            options.getGCSInputDirectoryPath(),
            options.getSpannerProjectId(),
            options.getMetadataInstance(),
            options.getMetadataDatabase(),
            options.getRunMode(),
            tableSuffix,
            options.getRunIdentifier());

    LOG.info("The size of  processing context is : " + processingContextMap.size());
    // TODO: add deletegcs mode handling
    pipeline
        .apply(
            "Create Context",
            Create.of(processingContextMap)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(), SerializableCoder.of(ProcessingContext.class))))
        .apply(
            "Write to source",
            ParDo.of(
                new GcsToSourceStreamer(
                    options.getTimerInterval(),
                    options.getSpannerProjectId(),
                    options.getMetadataInstance(),
                    options.getMetadataDatabase(),
                    tableSuffix)));

    return pipeline.run();
  }
}
