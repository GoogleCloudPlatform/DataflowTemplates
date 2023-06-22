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
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.SpannerChangeStreamsToSink.Options;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.sinks.DataSink;
import com.google.cloud.teleport.v2.templates.sinks.KafkaSink;
import com.google.cloud.teleport.v2.templates.sinks.PubSubSink;
import com.google.cloud.teleport.v2.templates.transforms.AssignShardIdFn;
import com.google.cloud.teleport.v2.templates.transforms.FilterRecordsFn;
import com.google.cloud.teleport.v2.templates.transforms.OrderRecordsAndWriteToSinkFn;
import com.google.cloud.teleport.v2.templates.transforms.PreprocessRecordsFn;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests Spanner Change streams data and writes them to a sink. Currently supported
 * sinks are Pubsub and Kafka.
 */
@Template(
    name = "Spanner_Change_Streams_to_Sink",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Sink",
    description =
        "Streaming pipeline. Ingests data from Spanner Change Streams, orders them, and"
            + " writes them to a sink.",
    optionsClass = Options.class,
    flexContainerName = "spanner-change-streams-to-sink",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-change-streams-to-sink",
    contactInformation = "https://cloud.google.com/support")
public class SpannerChangeStreamsToSink {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToSink.class);

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

    @TemplateParameter.Long(
        order = 9,
        optional = true,
        description = "Time interval to increment the Stateful timer.",
        helpText =
            "The timer gets incremented by the specified time interval in seconds. By default, the"
                + " next timer is set to the current real time.")
    @Default.Long(0)
    Long getIncrementInterval();

    void setIncrementInterval(Long value);

    @TemplateParameter.Enum(
        order = 10,
        optional = false,
        enumOptions = {"pubsub", "kafka"},
        description = "Type of sink to write the data to",
        helpText = "The type of sink where the data will get written to.")
    String getSinkType();

    void setSinkType(String value);

    @TemplateParameter.PubsubTopic(
        order = 11,
        optional = true,
        description =
            "PubSub topic where records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'",
        helpText =
            "PubSub topic where records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is"
                + " pubsub.")
    String getPubSubDataTopicId();

    void setPubSubDataTopicId(String value);

    @TemplateParameter.PubsubTopic(
        order = 12,
        optional = true,
        description =
            "PubSub topic where error records will get written to , in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'",
        helpText =
            "PubSub topic where error records will get written to, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'. Must be provided if sink is"
                + " pubsub.")
    String getPubSubErrorTopicId();

    void setPubSubErrorTopicId(String value);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Endpoint for pubsub",
        helpText = "Endpoint for pubsub. Must be provided if sink is pubsub.")
    @Default.String("")
    String getPubSubEndpoint();

    void setPubSubEndpoint(String value);

    @TemplateParameter.GcsReadFile(
        order = 14,
        optional = true,
        description = "Path to GCS file containing Kafka cluster details",
        helpText =
            "This is the path to GCS file containing Kafka cluster details. Must be provided if"
                + " sink is kafka.")
    String getKafkaClusterFilePath();

    void setKafkaClusterFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 15,
        optional = true,
        description = "Path to GCS file containing the the Source shard details",
        helpText =
            "Path to GCS file containing connection profile info for source shards. Must be"
                + " provided if sink is kafka.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 16,
        optional = false,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);
  }

  private static void validateSinkParams(Options options) {
    if (options.getSinkType().equals(Constants.PUBSUB_SINK)) {
      if (options.getPubSubDataTopicId().equals("")
          || options.getPubSubErrorTopicId().equals("")
          || options.getPubSubEndpoint().equals("")) {
        throw new IllegalArgumentException(
            "need to provide data topic, error topic and endpoint for pubsub sink.");
      }
    } else {
      if (options.getKafkaClusterFilePath() == null) {
        throw new IllegalArgumentException(
            "need to provide a valid GCS file path containing kafka cluster config for kafka"
                + " sink.");
      }
      if (options.getSourceShardsFilePath() == null) {
        throw new IllegalArgumentException(
            "need to provide a valid GCS file path containing source shard details for kafka"
                + " sink.");
      }
    }
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

    validateSinkParams(options);

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
    // We disable auto-scaling as scaling operations accumulate records in the buffer which can
    // cause the pipeline to crash.
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.NONE);

    DataSink dataSink = null;
    if (options.getSinkType().equals(Constants.PUBSUB_SINK)) {
      dataSink =
          new PubSubSink(
              options.getPubSubDataTopicId(),
              options.getPubSubErrorTopicId(),
              options.getPubSubEndpoint());
    } else {
      dataSink =
          new KafkaSink(options.getKafkaClusterFilePath(), options.getSourceShardsFilePath());
    }

    Schema schema = SessionFileReader.read(options.getSessionFilePath());
    LOG.info("Found schema in main: " + schema);

    pipeline
        .apply(getReadChangeStreamDoFn(options))
        .apply(ParDo.of(new FilterRecordsFn()))
        .apply(ParDo.of(new PreprocessRecordsFn()))
        .setCoder(SerializableCoder.of(TrimmedDataChangeRecord.class))
        .apply(ParDo.of(new AssignShardIdFn(schema)))
        .apply(
            ParDo.of(new OrderRecordsAndWriteToSinkFn(options.getIncrementInterval(), dataSink)));

    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(Options options) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withProjectId(options.getSpannerProjectId())
                    .withInstanceId(options.getInstanceId())
                    .withDatabaseId(options.getDatabaseId()))
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
