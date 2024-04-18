/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import com.google.cloud.Timestamp;
import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadOptions;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToVectorSearchOptions;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "Bigtable_Change_Streams_to_Vector_Search",
    category = TemplateCategory.STREAMING,
    displayName = "Bigtable Change Streams to Vector Search",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into Vertex AI Vector Search using Dataflow Runner V2.",
    optionsClass = BigtableChangeStreamsToVectorSearchOptions.class,
    optionsOrder = {
      BigtableChangeStreamsToVectorSearchOptions.class,
      ReadChangeStreamOptions.class,
      ReadOptions.class
    },
    skipOptions = {
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-change-streams-to-vector-search",
    flexContainerName = "bigtable-changestreams-to-vector-search",
    contactInformation = "https://cloud.google.com/support",
    streaming = true)
public final class BigtableChangeStreamsToVectorSearch {
  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToVectorSearch.class);

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Starting replication from Cloud Bigtable Change Streams to Vector Search");

    BigtableChangeStreamsToVectorSearchOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToVectorSearchOptions.class);

    run(options);
  }

  public static PipelineResult run(BigtableChangeStreamsToVectorSearchOptions options)
      throws IOException {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);

    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    String bigtableProjectId = getBigtableProjectId(options);

    LOG.info("  - startTimestamp {}", startTimestamp);
    LOG.info("  - bigtableReadInstanceId {}", options.getBigtableReadInstanceId());
    LOG.info("  - bigtableReadTableId {}", options.getBigtableReadTableId());
    LOG.info("  - bigtableChangeStreamAppProfile {}", options.getBigtableChangeStreamAppProfile());
    LOG.info("  - embeddingColumn {}", options.getEmbeddingColumn());
    LOG.info("  - crowdingTagColumn {}", options.getCrowdingTagColumn());
    LOG.info("  - project {}", options.getProject());
    LOG.info("  - indexName {}", options.getVectorSearchIndex());

    String indexName = options.getVectorSearchIndex();

    String vertexRegion = Utils.extractRegionFromIndexName(indexName);
    String vertexEndpoint = vertexRegion + "-aiplatform.googleapis.com:443";

    final Pipeline pipeline = Pipeline.create(options);

    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions(
                options.getBigtableChangeStreamResume()
                    ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
                    : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS)
            .withProjectId(bigtableProjectId)
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withMetadataTableTableId(options.getBigtableMetadataTableTableId())
            .withStartTime(startTimestamp);

    PCollectionTuple results =
        pipeline
            .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
            .apply("Create Values", Values.create())
            .apply(
                "Converting to Vector Search Datapoints",
                ParDo.of(
                        new ChangeStreamMutationToDatapointOperationFn(
                            options.getEmbeddingColumn(),
                            options.getEmbeddingByteSize(),
                            options.getCrowdingTagColumn(),
                            Utils.parseColumnMapping(options.getAllowRestrictsMappings()),
                            Utils.parseColumnMapping(options.getDenyRestrictsMappings()),
                            Utils.parseColumnMapping(options.getIntNumericRestrictsMappings()),
                            Utils.parseColumnMapping(options.getFloatNumericRestrictsMappings()),
                            Utils.parseColumnMapping(options.getDoubleNumericRestrictsMappings())))
                    .withOutputTags(
                        ChangeStreamMutationToDatapointOperationFn.UPSERT_DATAPOINT_TAG,
                        TupleTagList.of(
                            ChangeStreamMutationToDatapointOperationFn.REMOVE_DATAPOINT_TAG)));
    results
        .get(ChangeStreamMutationToDatapointOperationFn.UPSERT_DATAPOINT_TAG)
        .apply("Add placeholer keys", WithKeys.of("placeholder"))
        .apply(
            "Batch Contents",
            GroupIntoBatches.<String, IndexDatapoint>ofSize(
                    bufferSizeOption(options.getUpsertMaxBatchSize()))
                .withMaxBufferingDuration(
                    bufferDurationOption(options.getUpsertMaxBufferDuration())))
        .apply("Map to Values", Values.create())
        .apply(
            "Upsert Datapoints to VectorSearch",
            ParDo.of(new UpsertDatapointsFn(vertexEndpoint, indexName)))
        .apply(
            "Write errors to DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectory() + "YYYY/MM/dd/HH/mm/")
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    results
        .get(ChangeStreamMutationToDatapointOperationFn.REMOVE_DATAPOINT_TAG)
        .apply("Add placeholder keys", WithKeys.of("placeholer"))
        .apply(
            "Batch Contents",
            GroupIntoBatches.<String, String>ofSize(
                    bufferSizeOption(options.getDeleteMaxBatchSize()))
                .withMaxBufferingDuration(
                    bufferDurationOption(options.getDeleteMaxBufferDuration())))
        .apply("Map to Values", Values.create())
        .apply(
            "Remove Datapoints From VectorSearch",
            ParDo.of(new RemoveDatapointsFn(vertexEndpoint, indexName)))
        .apply(
            "Write errors to DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectory() + "YYYY/MM/dd/HH/mm/")
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  private static String getBigtableProjectId(BigtableChangeStreamsToVectorSearchOptions options) {
    return StringUtils.isEmpty(options.getBigtableReadProjectId())
        ? options.getProject()
        : options.getBigtableReadProjectId();
  }

  private static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }
  }

  private static int bufferSizeOption(int size) {
    if (size < 1) {
      size = 1;
    }

    return size;
  }

  private static Duration bufferDurationOption(String duration) {
    if (duration.isEmpty()) {
      return Duration.standardSeconds(1);
    }

    return DurationUtils.parseDuration(duration);
  }

  private static DeadLetterQueueManager buildDlqManager(
      BigtableChangeStreamsToVectorSearchOptions options) {
    String dlqDirectory = options.getDlqDirectory();
    if (dlqDirectory.isEmpty()) {
      LOG.info("Falling back to temp dir for DLQ");

      String tempLocation = options.as(DataflowPipelineOptions.class).getTempLocation();

      LOG.info("Have temp location {}", tempLocation);
      if (tempLocation == null || tempLocation.isEmpty()) {
        tempLocation = "/";
      } else if (!tempLocation.endsWith("/")) {
        tempLocation += "/";
      }

      dlqDirectory = tempLocation + "dlq";
    }

    LOG.info("Writing dead letter queue to: {}", dlqDirectory);

    return DeadLetterQueueManager.create(dlqDirectory, 1);
  }
}
