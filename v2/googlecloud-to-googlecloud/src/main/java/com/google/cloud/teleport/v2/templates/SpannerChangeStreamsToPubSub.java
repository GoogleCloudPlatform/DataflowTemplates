/*
 * Copyright (C) 2022 Google LLC
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
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.transforms.FileFormatFactorySpannerChangeStreamsToPubSub;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToPubSub} pipeline streams change stream record(s) and stores to
 * pubsub topic in user specified format. The sink data can be stored in a JSON Text or Avro data
 * format.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Spanner_Change_Streams_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Spanner_Change_Streams_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Spanner change streams to Pub/Sub",
    description = {
      "The Cloud Spanner change streams to the Pub/Sub template is a streaming pipeline that streams Cloud Spanner data change records and writes them into Pub/Sub topics using Dataflow Runner V2.\n",
      "To output your data to a new Pub/Sub topic, you need to first create the topic. After creation, Pub/Sub automatically generates and attaches a subscription to the new topic. "
          + "If you try to output data to a Pub/Sub topic that doesn't exist, the dataflow pipeline throws an exception, and the pipeline gets stuck as it continuously tries to make a connection.\n",
      "If the necessary Pub/Sub topic already exists, you can output data to that topic.",
      "Learn more about <a href=\"https://cloud.google.com/spanner/docs/change-streams\">change streams</a>, <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow\">how to build change streams Dataflow pipelines</a>, and <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices\">best practices</a>."
    },
    optionsClass = SpannerChangeStreamsToPubSubOptions.class,
    flexContainerName = "spanner-changestreams-to-pubsub",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner instance must exist before running the pipeline.",
      "The Cloud Spanner database must exist prior to running the pipeline.",
      "The Cloud Spanner metadata instance must exist prior to running the pipeline.",
      "The Cloud Spanner metadata database must exist prior to running the pipeline.",
      "The Cloud Spanner change stream must exist prior to running the pipeline.",
      "The Pub/Sub topic must exist prior to running the pipeline."
    })
public class SpannerChangeStreamsToPubSub {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToPubSub.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Input Messages to Pub/Sub");

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToPubSubOptions.class);

    run(options);
  }

  private static String getSpannerProjectId(SpannerChangeStreamsToPubSubOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  private static String getPubsubProjectId(SpannerChangeStreamsToPubSubOptions options) {
    return options.getPubsubProjectId().isEmpty()
        ? options.getProject()
        : options.getPubsubProjectId();
  }

  public static PipelineResult run(SpannerChangeStreamsToPubSubOptions options) {
    LOG.info("Requested Message Format is " + options.getOutputDataFormat());
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    final Pipeline pipeline = Pipeline.create(options);
    // Get the Spanner project, instance, database, metadata instance, metadata database
    // change stream, pubsub topic, and pubsub api parameters.
    String spannerProjectId = getSpannerProjectId(options);
    String instanceId = options.getSpannerInstanceId();
    String databaseId = options.getSpannerDatabase();
    String metadataInstanceId = options.getSpannerMetadataInstanceId();
    String metadataDatabaseId = options.getSpannerMetadataDatabase();
    String changeStreamName = options.getSpannerChangeStreamName();
    String pubsubProjectId = getPubsubProjectId(options);
    String pubsubTopicName = options.getPubsubTopic();
    String pubsubAPI = options.getPubsubAPI();

    // Retrieve and parse the start / end timestamps.
    Timestamp startTimestamp =
        options.getStartTimestamp().isEmpty()
            ? Timestamp.now()
            : Timestamp.parseTimestamp(options.getStartTimestamp());
    Timestamp endTimestamp =
        options.getEndTimestamp().isEmpty()
            ? Timestamp.MAX_VALUE
            : Timestamp.parseTimestamp(options.getEndTimestamp());

    // Add use_runner_v2 to the experiments option, since Change Streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    if (!experiments.contains(USE_RUNNER_V2_EXPERIMENT)) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);

    String metadataTableName =
        options.getSpannerMetadataTableName() == null
            ? null
            : options.getSpannerMetadataTableName();

    final RpcPriority rpcPriority = options.getRpcPriority();
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
            .withProjectId(spannerProjectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    // Propagate database role for fine-grained access control on change stream.
    if (options.getSpannerDatabaseRole() != null) {
      spannerConfig =
          spannerConfig.withDatabaseRole(
              ValueProvider.StaticValueProvider.of(options.getSpannerDatabaseRole()));
    }
    pipeline
        .apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(spannerConfig)
                .withMetadataInstance(metadataInstanceId)
                .withMetadataDatabase(metadataDatabaseId)
                .withChangeStreamName(changeStreamName)
                .withInclusiveStartAt(startTimestamp)
                .withInclusiveEndAt(endTimestamp)
                .withRpcPriority(rpcPriority)
                .withMetadataTable(metadataTableName))
        .apply(
            "Convert each record to a PubsubMessage",
            FileFormatFactorySpannerChangeStreamsToPubSub.newBuilder()
                .setOutputDataFormat(options.getOutputDataFormat())
                .setProjectId(pubsubProjectId)
                .setPubsubAPI(pubsubAPI)
                .setPubsubTopicName(pubsubTopicName)
                .build());
    return pipeline.run();
  }
}
