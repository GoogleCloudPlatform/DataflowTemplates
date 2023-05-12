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
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.KafkaToSource.Options;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.transforms.KafkaToSourceStreamer;
import com.google.cloud.teleport.v2.templates.utils.ProcessingContextGenerator;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests Kafka data containing change stream records to source database - currently
 * MySQL.
 *
 * <p>NOTE: Future versions will support: Postgres,Oracle
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/kafka-to-source/README_Kafka_to_Source.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Kafka_to_Source",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Source",
    description =
        "Streaming pipeline. Ingests messages from a stream in Kafka, transforms them, and"
            + " writes them to a Source Database like MySQL.",
    optionsClass = Options.class,
    flexContainerName = "kafka-to-source",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-source",
    contactInformation = "https://cloud.google.com/support")
public class KafkaToSource {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToSource.class);

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
        description = "File location for Kafka cluster details file in Cloud Storage.",
        helpText = "This is the file location for Kafka cluster details file in Cloud Storage.")
    String getKafkaClusterFilePath();

    void setKafkaClusterFilePath(String value);

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
        optional = true,
        description = "Destination source type",
        helpText =
            "This is the type of source databse. Example - mysql/oracle/postgres. Currently only"
                + " mysql is supported.")
    @Default.String("mysql")
    String getSourceType();

    void setSourceType(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Kafka to Source");

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

    List<ProcessingContext> processingContext =
        ProcessingContextGenerator.getProcessingContext(
            options.getSourceShardsFilePath(),
            options.getSourceType(),
            options.getKafkaClusterFilePath());

    pipeline.apply(Create.of(processingContext)).apply(ParDo.of(new KafkaToSourceStreamer()));
    return pipeline.run();
  }
}
