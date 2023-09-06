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
    contactInformation = "https://cloud.google.com/support")
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
        optional = false,
        description = "File start timestamp",
        helpText = "Start time of file for all shards. To be given at first launch of the job.")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Duration(
        order = 7,
        optional = false,
        description = "File increment window duration",
        helpText =
            "The window duration/size in which data is written to Cloud Storage. Allowed"
                + " formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh"
                + " (for hours, example: 2h).",
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

    @TemplateParameter.Integer(
        order = 12,
        optional = true,
        description = "Number of times file lookup needs to be retried. ",
        helpText = "Number of times file lookup needs to be retried.")
    @Default.Integer(3)
    Integer getGCSLookupRetryCount();

    void setGCSLookupRetryCount(Integer value);

    @TemplateParameter.Integer(
        order = 13,
        optional = true,
        description = "Duration in seconds between file lookup retries. ",
        helpText = "Controls the time between succssive polls GCS file loop retries.")
    @Default.Integer(3)
    Integer getGCSLookupRetryInterval();

    void setGCSLookupRetryInterval(Integer value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        // TODO: enumOptions = {"regular", "reprocess", "deletegcs"},
        description = "This type of run mode. Supported values - regular/reprocess/deletegcs.",
        helpText = "Regular writes to source db, reprocess erred shards or delete the gcs files")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);
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
            options.getGCSLookupRetryCount(),
            options.getGCSLookupRetryInterval(),
            options.getRunMode());

    LOG.info("The size of  processing context is : " + processingContextMap.size());
    // TODO: get start time and interval from metadata database, add deletegcs mode handling
    pipeline
        .apply(
            Create.of(processingContextMap)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(), SerializableCoder.of(ProcessingContext.class))))
        .apply(ParDo.of(new GcsToSourceStreamer(options.getTimerInterval())));

    return pipeline.run();
  }
}
