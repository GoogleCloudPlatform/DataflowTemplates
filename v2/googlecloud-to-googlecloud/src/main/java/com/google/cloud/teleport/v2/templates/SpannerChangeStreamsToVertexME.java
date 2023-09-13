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
import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.aiplatform.v1.IndexName;
import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.aiplatform.v1.UpsertDatapointsRequest;
import com.google.cloud.aiplatform.v1.UpsertDatapointsResponse;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToVertexMEOptions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToVertexME} pipeline streams change stream record(s) and stores to
 * pubsub topic in user specified format. The sink data can be stored in a JSON Text or Avro data
 * format.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Spanner_Change_Streams_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Spanner_Change_Streams_to_VertexME",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Spanner change streams to Vertex ME",
    description = {
      "The Cloud Spanner change streams to the Vertex ME template is a streaming pipeline that streams Cloud Spanner data change records and writes them into Pub/Sub topics using Dataflow Runner V2.\n",
      "To output your data to a new Pub/Sub topic, you need to first create the topic. After creation, Pub/Sub automatically generates and attaches a subscription to the new topic. "
          + "If you try to output data to a Pub/Sub topic that doesn't exist, the dataflow pipeline throws an exception, and the pipeline gets stuck as it continuously tries to make a connection.\n",
      "If the necessary Pub/Sub topic already exists, you can output data to that topic.",
      "Learn more about <a href=\"https://cloud.google.com/spanner/docs/change-streams\">change streams</a>, <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow\">how to build change streams Dataflow pipelines</a>, and <a href=\"https://cloud.google.com/spanner/docs/change-streams/use-dataflow#best_practices\">best practices</a>."
    },
    optionsClass = SpannerChangeStreamsToVertexMEOptions.class,
    flexContainerName = "spanner-changestreams-to-pubsub",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner instance must exist before running the pipeline.",
      "The Cloud Spanner database must exist prior to running the pipeline.",
      "The Cloud Spanner metadata instance must exist prior to running the pipeline.",
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-change-streams-to-vertexme",
    })
public class SpannerChangeStreamsToVertexME {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToVertexME.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";
  private static final String ENDPOINT = "us-central1-aiplatform.googleapis.com";
  private static final String PROJECT_ID = "span-cloud-testing";
  private static final String REGION = "us-central1";
  private static final String INDEX_ID = "1012034482670141440"; // nsujir_prototype


  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Input Messages to Pub/Sub");

    SpannerChangeStreamsToVertexMEOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToVertexMEOptions.class);

    run(options);
  }

  private static String getSpannerProjectId(SpannerChangeStreamsToVertexMEOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  static void upsertDatapoints(DataChangeRecord record) {
    try {
      IndexServiceSettings indexServiceSettings =
          IndexServiceSettings.newBuilder().setEndpoint(ENDPOINT + ":443").build();
      IndexServiceClient indexServiceClient = IndexServiceClient.create(
          indexServiceSettings);
      for (var mods : record.getMods()) {
        JSONObject keys = new JSONObject(mods.getKeysJson());
        JSONObject values = new JSONObject(mods.getNewValuesJson());
        LOG.info("key id: ", keys.get("id"));
        JSONArray embeddings = values.getJSONArray("embeddings");
        LOG.info("embeddings: ", embeddings.toString());
        Object id = keys.get("id");
        List<Float> floats = new Gson().fromJson(embeddings.toString(), new TypeToken<List<Float>>() {}.getType());
        LOG.warn("floats: ", floats);
        IndexDatapoint dp = IndexDatapoint.newBuilder().setDatapointId(id.toString())
            .addAllFeatureVector(floats)
            .build();
        ArrayList<IndexDatapoint> dps = new ArrayList<IndexDatapoint>();
        dps.add(dp);

        UpsertDatapointsRequest request =
            UpsertDatapointsRequest.newBuilder()
                .setIndex(IndexName.of(PROJECT_ID, REGION, INDEX_ID).toString())
                .addAllDatapoints(dps)
                .build();
        UpsertDatapointsResponse response = indexServiceClient.upsertDatapoints(request);
      }
    } catch (IOException e) {
      LOG.error("Error: ", e);
    }
  }

  public static PipelineResult run(SpannerChangeStreamsToVertexMEOptions options) {
    LOG.info("Requested Message Format is " + options.getOutputDataFormat());
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    final Pipeline pipeline = Pipeline.create(options);
    // Get the Spanner project, instance, database, metadata instance, metadata database
    // change stream .
    String spannerProjectId = getSpannerProjectId(options);
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
                .withMetadataTable(metadataTableName)
        )
        .apply(
            "Log record",
    ParDo.of(new DoFn<DataChangeRecord, String>() {
      @ProcessElement
          public void processElement(@Element DataChangeRecord record, OutputReceiver<String> out) {
        LOG.info(record.getMods().toString());
        upsertDatapoints(record);
        out.output("");
      }
    }));
    return pipeline.run();
  }
}
