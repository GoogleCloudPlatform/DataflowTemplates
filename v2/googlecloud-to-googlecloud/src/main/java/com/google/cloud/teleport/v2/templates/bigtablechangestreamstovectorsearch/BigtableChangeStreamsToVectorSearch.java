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
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToVectorSearchOptions;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import java.io.IOException;
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
    displayName = "Cloud Bigtable Change Streams to Vector Search",
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
    // TODO(meagar): Documentation link
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-vector-search",
    flexContainerName = "bigtable-changestreams-to-vector-search",
    contactInformation = "https://cloud.google.com/support",
    streaming = true)
public final class BigtableChangeStreamsToVectorSearch {
  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToVectorSearch.class);

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info(
        "Starting to replicate change records from Cloud Bigtable change streams to Vector Search");

    BigtableChangeStreamsToVectorSearchOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToVectorSearchOptions.class);

    // TODO(meagar): Copied from Ron's template, when is this required?
    // Wait for pipeline to finish only if it is not constructing a template.
    // if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
    //   result.waitUntilFinish();
    // }
    try {
      run(options);
    } catch (IOException e) {
      LOG.error("{}", e);
    }

    LOG.info("Completed pipeline setup");
  }

  public static PipelineResult run(BigtableChangeStreamsToVectorSearchOptions options)
      throws IOException {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // TODO(meagar): Copied from Ron's template, is this important?
    // Do not validate input fields if it is running as a template.
    // if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
    //   read = read.withoutValidation();
    // }

    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    String bigtableProjectId = getBigtableProjectId(options);

    LOG.debug("  - startTimestamp {}", startTimestamp);
    LOG.debug("  - bigtableReadInstanceId {}", options.getBigtableReadInstanceId());
    LOG.debug("  - bigtableReadTableId {}", options.getBigtableReadTableId());
    LOG.debug("  - bigtableChangeStreamAppProfile {}", options.getBigtableChangeStreamAppProfile());
    LOG.debug("  - embeddingColumn {}", options.getEmbeddingColumn());
    LOG.debug("  - crowdingTagColumn {}", options.getCrowdingTagColumn());
    LOG.debug("  - project {}", options.getProject());

    String indexName = options.getVectorSearchIndex();
    // TODO(meagar): Raises, do we handle this, or allow exceptions to escape?
    String vertexRegion = Utils.extractRegionFromIndexName(indexName);
    String vertexEndpoint = vertexRegion + "-aiplatform.googleapis.com:443";

    final Pipeline pipeline = Pipeline.create(options);

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
            // TODO(meagar): Remove this
            .apply("Log Changes", ParDo.of(new LogChangeStreamFn()))
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
                        ChangeStreamMutationToDatapointOperationFn.UpsertDatapointTag,
                        TupleTagList.of(
                            ChangeStreamMutationToDatapointOperationFn.RemoveDatapointTag)));
    results
        .get(ChangeStreamMutationToDatapointOperationFn.UpsertDatapointTag)
        .apply(WithKeys.of("foo")) // TOOD(meagar): Better name for place-holders?
        .apply(
            "Batch Contents",
            GroupIntoBatches.<String, IndexDatapoint>ofSize(
                    bufferSizeOption(options.getUpsertMaxBatchSize()))
                .withMaxBufferingDuration(
                    bufferDurationOption(options.getUpsertMaxBufferDuration()))
            // .withShardedKey() // TODO(meagar): What does this do?
            )
        .apply("Values", Values.create())
        .apply(
            "Upsert Datapoints to VectorSearch",
            ParDo.of(new UpsertDatapointsFn(vertexEndpoint, indexName)));

    results
        .get(ChangeStreamMutationToDatapointOperationFn.RemoveDatapointTag)
        .apply(WithKeys.of("foo")) // TOOD(meagar): Better name for place-holders?
        .apply(
            "Batch Contents",
            GroupIntoBatches.<String, String>ofSize(
                    bufferSizeOption(options.getDeleteMaxBatchSize()))
                .withMaxBufferingDuration(
                    bufferDurationOption(options.getDeleteMaxBufferDuration()))
            // .withByteSize() // TODO(meagar): We may also want to buffer by size
            // .withShardedKey() // TODO(meagar): What does this do?
            )
        .apply("Values", Values.create())
        .apply(
            "Remove Datapoints From VectorSearch",
            ParDo.of(new RemoveDatapointsFn(vertexEndpoint, indexName)));

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
      // TODO(meagar): Maybe throw instead?
      size = 1;
    }

    return size;
  }

  private static Duration bufferDurationOption(String duration) {
    if (duration == "" || duration == null) {
      return Duration.standardSeconds(1);
    }

    return DurationUtils.parseDuration(duration);
  }
}
