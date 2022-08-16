/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.elasticsearch.options.SpannerChangeStreamsToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToElasticsearch} pipeline forwards Spanner Change stream data to
 * Elasticsearch.
 */
public class SpannerChangeStreamsToElasticsearch {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamsToElasticsearch.class);
  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  public static void main(String[] args) {
    LOG.info("Starting Input Files to GCS");

    SpannerChangeStreamsToElasticsearchOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToElasticsearchOptions.class);

    run(options);
  }

  private static String getProjectId(SpannerChangeStreamsToElasticsearchOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  public static PipelineResult run(SpannerChangeStreamsToElasticsearchOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);
    options.setAutoscalingAlgorithm(
        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE);
    // Spanner change stream updates would require partial updates when writing to Elasticsearch
    // unless the full record is read
    options.setUsePartialUpdate(true);

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
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.toLowerCase().equals(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);

    String metadataTableName =
        options.getSpannerMetadataTableName() == null
            ? null
            : options.getSpannerMetadataTableName();

    final RpcPriority rpcPriority = options.getRpcPriority();
    pipeline
        .apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(
                    SpannerConfig.create()
                        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
                        .withProjectId(projectId)
                        .withInstanceId(instanceId)
                        .withDatabaseId(databaseId))
                .withMetadataInstance(metadataInstanceId)
                .withMetadataDatabase(metadataDatabaseId)
                .withChangeStreamName(changeStreamName)
                .withInclusiveStartAt(startTimestamp)
                .withInclusiveEndAt(endTimestamp)
                .withRpcPriority(rpcPriority)
                .withMetadataTable(metadataTableName))
        .apply("DataChangeRecord To JSON", ParDo.of(new DataChangeRecordToJsonFn()))
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(SpannerChangeStreamsToElasticsearchOptions.class))
                .build());

    return pipeline.run();
  }

  static class DataChangeRecordToJsonFn extends DoFn<DataChangeRecord, String> {

    @ProcessElement
    public void process(@Element DataChangeRecord element, OutputReceiver<String> output) {
      List<ColumnType> cols = element.getRowType();
      ModType modType = element.getModType();
      String tableName = element.getTableName();
      element
          .getMods()
          .forEach(
              mod -> {
                JSONObject keysJsonExtended = new JSONObject(mod.getKeysJson());
                keysJsonExtended.put("TableName", tableName);
                if (modType == ModType.INSERT || modType == ModType.UPDATE) {
                  JSONObject newValuesJson = new JSONObject(mod.getNewValuesJson());
                  // add any properties that are present in newValuesJson to the set of keys to make
                  // a partial (or complete) row reflecting
                  // the update (or insert)
                  cols.forEach(
                      col -> {
                        if (newValuesJson.has(col.getName())) {
                          keysJsonExtended.put(col.getName(), newValuesJson.get(col.getName()));
                        }
                      });
                } else {
                  // For DELETE mod, need the keys and a property indicating it is a delete
                  keysJsonExtended.put("IsDelete", true);
                }

                output.output(keysJsonExtended.toString());
              });
    }
  }
}
