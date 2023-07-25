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
import com.google.cloud.teleport.v2.templates.OrderedChangestreamBufferToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.transforms.BufferToSourceStreamer;
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
 * This pipeline ingests PubSub/Kafka data containing change stream records to source database -
 * currently MySQL.
 *
 * <p>NOTE: Future versions will support: Postgres,Oracle
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/kafka-to-source/README_Kafka_to_Source.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Ordered_Changestream_Buffer_to_Sourcedb",
    category = TemplateCategory.STREAMING,
    displayName = "Ordered change stream buffer to Source DB",
    description =
        "Streaming pipeline. Reads ordered Spanner change stream message from Pub/SubKafka,"
            + " transforms them, and writes them to a Source Database like MySQL.",
    optionsClass = Options.class,
    flexContainerName = "ordered-changestream-buffer-to-sourcedb",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/ordered-changestream-buffer-to-sourcedb",
    contactInformation = "https://cloud.google.com/support")
public class OrderedChangestreamBufferToSourceDb {

  private static final Logger LOG =
      LoggerFactory.getLogger(OrderedChangestreamBufferToSourceDb.class);

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

    @TemplateParameter.Enum(
        order = 4,
        optional = true,
        enumOptions = {@TemplateEnumOption("pubsub"), @TemplateEnumOption("kafka")},
        description = "Input buffer type",
        helpText = "This is the type of input buffer read from. Supported values - PubSub/Kafka.")
    @Default.String("pubsub")
    String getBufferType();

    void setBufferType(String value);

    @TemplateParameter.ProjectId(
        order = 5,
        optional = true,
        description = "Project id for the PubSub subscriber",
        helpText =
            "This is the project containing the pubsub subscribers. Required when the buffer is"
                + " PubSub.")
    String getPubSubProjectId();

    void setPubSubProjectId(String value);

    @TemplateParameter.Integer(
        order = 6,
        optional = true,
        description = "Max messages to read from PubSub subscriber",
        helpText = "Tuning parameter, to control the throughput.")
    @Default.Integer(2000)
    Integer getPubSubMaxReadCount();

    void setPubSubMaxReadCount(Integer value);

    @TemplateParameter.GcsReadFile(
        order = 7,
        optional = true,
        description = "File location for Kafka cluster details file in Cloud Storage.",
        helpText =
            "This is the file location for Kafka cluster details file in Cloud Storage.Required"
                + " when the buffer is Kafka.")
    String getKafkaClusterFilePath();

    void setKafkaClusterFilePath(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.Integer(
        order = 9,
        optional = true,
        description = "Duration in seconds between calls to stateful timer processing. ",
        helpText =
            "Controls the time between succssive polls to buffer and processing of the resultant"
                + " records.")
    @Default.Integer(1)
    Integer getTimerInterval();

    void setTimerInterval(Integer value);
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
    // List<ProcessingContext> processingContext = null;
    Map<String, ProcessingContext> processingContextMap = null;
    if ("pubsub".equals(options.getBufferType())) {
      processingContextMap =
          ProcessingContextGenerator.getProcessingContextForPubSub(
              options.getSourceShardsFilePath(),
              options.getSourceType(),
              options.getPubSubProjectId(),
              options.getSessionFilePath(),
              options.getPubSubMaxReadCount(),
              options.getSourceDbTimezoneOffset());

    } else if ("kafka".equals(options.getBufferType())) {
      processingContextMap =
          ProcessingContextGenerator.getProcessingContextForKafka(
              options.getSourceShardsFilePath(),
              options.getSourceType(),
              options.getKafkaClusterFilePath(),
              options.getSessionFilePath(),
              options.getSourceDbTimezoneOffset());

    } else {
      throw new RuntimeException("Unsupported buffer type: " + options.getBufferType());
    }

    LOG.info("The size of  processing context is : " + processingContextMap.size());
    pipeline
        .apply(
            Create.of(processingContextMap)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(), SerializableCoder.of(ProcessingContext.class))))
        .apply(ParDo.of(new BufferToSourceStreamer(options.getTimerInterval())));

    return pipeline.run();
  }
}
