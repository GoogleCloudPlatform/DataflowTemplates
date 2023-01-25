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
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.transforms.FileFormatFactorySpannerChangeStreams;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToGcs} pipeline streams change stream record(s) and stores to
 * Google Cloud Storage bucket in user specified format. The sink data can be stored in a Text or
 * Avro file format.
 */
@Template(
    name = "Spanner_Change_Streams_to_Google_Cloud_Storage",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Spanner change streams to Cloud Storage",
    description =
        "Streaming pipeline. Streams Spanner change stream data records and writes them into a"
            + " Cloud Storage bucket using Dataflow Runner V2.",
    optionsClass = SpannerChangeStreamsToGcsOptions.class,
    flexContainerName = "spanner-changestreams-to-gcs",
    contactInformation = "https://cloud.google.com/support")
public class SpannerChangeStreamsToGcs {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToGcs.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Input Files to GCS");

    SpannerChangeStreamsToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToGcsOptions.class);

    run(options);
  }

  private static String getProjectId(SpannerChangeStreamsToGcsOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  public static PipelineResult run(SpannerChangeStreamsToGcsOptions options) {
    LOG.info("Requested File Format is " + options.getOutputFileFormat());
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    final Pipeline pipeline = Pipeline.create(options);

    // Get the Spanner project, instance, database, and change stream parameters.
    String projectId = getProjectId(options);
    String instanceId = options.getSpannerInstanceId();
    String databaseId = options.getSpannerDatabase();
    String metadataInstanceId = options.getSpannerMetadataInstanceId();
    String metadataDatabaseId = options.getSpannerMetadataDatabase();
    String changeStreamName = options.getSpannerChangeStreamName();

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
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    // Propagate database role for fine-grained access control on change stream.
    if (options.getSpannerDatabaseRole() != null) {
      LOG.info("Setting database role on SpannerConfig: " + options.getSpannerDatabaseRole());
      spannerConfig =
          spannerConfig.withDatabaseRole(
              ValueProvider.StaticValueProvider.of(options.getSpannerDatabaseRole()));
    }
    LOG.info("Created SpannerConfig: " + spannerConfig);
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
            "Creating " + options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply(
            "Write To GCS",
            FileFormatFactorySpannerChangeStreams.newBuilder().setOptions(options).build());

    return pipeline.run();
  }
}
