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
import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadOptions;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToVectorSearchOptions;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
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
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-pubsub",
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
    // PipelineResult result = run(options);
    // Wait for pipeline to finish only if it is not constructing a template.
    // if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
    //   result.waitUntilFinish();
    // }
    run(options);

    LOG.info("Completed pipeline setup");
  }

  public static PipelineResult run(BigtableChangeStreamsToVectorSearchOptions options) {
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
    Duration windowingDuration = DurationUtils.parseDuration(options.getWindowDuration());

    LOG.info("  - startTimestamp {}", startTimestamp);
    LOG.info("  - bigtableReadInstanceId {}", options.getBigtableReadInstanceId());
    LOG.info("  - bigtableReadTableId {}", options.getBigtableReadTableId());
    LOG.info("  - bigtableChangeStreamAppProfile {}", options.getBigtableChangeStreamAppProfile());
    LOG.info("  - windowingDuration {}", windowingDuration);
    LOG.info("  - embeddingColumn {}", options.getEmbeddingColumn());
    LOG.info("  - crowdingTagColumn {}", options.getCrowdingTagColumn());
    LOG.info("  - project {}", options.getProject());

    LOG.info("  - embeddingColumn{}", options.getEmbeddingColumn());

    final Pipeline pipeline = Pipeline.create(options);

    String endpoint = "us-east1" + "-aiplatform.googleapis.com:443";

    IndexServiceClient vertexClient;

    try {
      vertexClient =
          IndexServiceClient.create(
              IndexServiceSettings.newBuilder().setEndpoint(endpoint).build());
    } catch (IOException e) {
      LOG.error("Error: ", e);
      return null; // TODO
    }

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions( // TODO(meagar): What is this?
                options.getBigtableChangeStreamResume()
                    ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
                    : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS)
            .withProjectId(bigtableProjectId)
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            // TODO(meagar): What is this?
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withMetadataTableTableId(options.getBigtableMetadataTableTableId())
            .withStartTime(startTimestamp);

    pipeline
        .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
        .apply(introduceTimestamps()) // TODO(meagar): Not sure what this does
        .apply(
            "Creating " + windowingDuration + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        // TODO(meagar): I understand nearly none of this
        // Window.<KV<ByteString, ChangeStreamMutation>>into(FixedWindows.of(windowingDuration))
        //     .triggering(
        //         Repeatedly.forever(
        //             AfterWatermark.pastEndOfWindow()
        //                 .withEarlyFirings(
        //                     AfterFirst.of(
        //                         AfterProcessingTime.pastFirstElementInPane()
        //                             .plusDelayOf(windowingDuration),
        //                         AfterPane.elementCountAtLeast(5)
        //                         ))
        //                 .withLateFirings(
        //                     AfterFirst.of(
        //                         AfterProcessingTime.pastFirstElementInPane()
        //                             .plusDelayOf(windowingDuration),
        //                         AfterPane.elementCountAtLeast(5)
        //                         ))))
        //
        //     .withAllowedLateness(Duration.millis(0))
        //     .discardingFiredPanes())
        .apply(Values.create()) // TODO(meagar): What does this do?
        .apply(
            "Log Changes",
            ParDo.of(
                new DoFn<ChangeStreamMutation, ChangeStreamMutation>() {
                  @ProcessElement
                  public void process(
                      @Element ChangeStreamMutation input,
                      OutputReceiver<ChangeStreamMutation> recv) {

                    LOG.info("Received Change:");
                    LOG.info("  - rowkey: {}", input.getRowKey().toStringUtf8());
                    LOG.info("  - type: {}", input.getType());
                    LOG.info("  - Mods:");
                    for (Entry entry : input.getEntries()) {
                      LOG.info("    - mod: {}", entry);
                      LOG.info("    - class: {}", entry.getClass());
                      if (entry instanceof SetCell) {
                        LOG.info("    - type: SetCell");
                        SetCell m = (SetCell) entry;
                        LOG.info("    - familyName: {}", m.getFamilyName());
                        LOG.info("    - qualifier: {}", m.getQualifier().toStringUtf8());
                        LOG.info("    - timestamp: {}", m.getTimestamp());
                        LOG.info("    - value: {}", m.getValue().toStringUtf8());
                      } else if (entry instanceof DeleteCells) {
                        LOG.info("    - type: DeleteCell");
                        DeleteCells m = (DeleteCells) entry;
                        LOG.info("    - familyName: {}", m.getFamilyName());
                        LOG.info("    - qualifier: {}", m.getQualifier().toStringUtf8());
                        LOG.info("    - timestamp: {}", m.getTimestampRange());
                      } else if (entry instanceof DeleteFamily) {
                        LOG.info("    - type: DeleteFamily");
                        DeleteFamily m = (DeleteFamily) entry;
                        LOG.info("    - familyName: {}", m.getFamilyName());

                        // TODO(meagar): Not sure this is a special case, or just treated as a
                        // generic mutation
                      } else {
                        LOG.warn("Unexpected mod type");
                      }
                    }

                    // TODO
                    recv.output(input);
                  }
                }))
        .apply(
            "Writing to Vector Search",
            ParDo.of(
                WriteChangeStreamMutationToVectorSearchFn.newBuilder()
                    .withIndexId("7774602739538984960")
                    .withProjectId("818418350420")
                    .withRegion("us-east1")
                    .withEmbeddingsColumn(options.getEmbeddingColumn())
                    .withEmbeddingsByteSize(options.getEmbeddingByteSize())
                    .withCrowdingTagColumn(options.getCrowdingTagColumn())
                    .withAllowRestrictsMappings(parseMapping(options.getAllowRestrictsMappings()))
                    .withDenyRestrictsMappings(parseMapping(options.getDenyRestrictsMappings()))
                    .withIntNumericRestrictsMappings(
                        parseMapping(options.getIntNumericRestrictsMappings()))
                    .withFloatNumericRestrictsMappings(
                        parseMapping(options.getFloatNumericRestrictsMappings()))
                    .withDoubleNumberRestrictsMappings(
                        parseMapping(options.getDoubleNumericRestrictsMappings()))
                    .build()));

    return pipeline.run();
  }

  // Split "cf1:foo1->bar1,cf1:foo2->bar2" into a map of { "cf1:foo1": "bar1", "cf1:foo2": "bar2" }
  private static Map<String, String> parseMapping(String mapstr) {
    Map<String, String> columnsWithAliases = new HashMap<>();
    if (StringUtils.isBlank(mapstr)) {
      return columnsWithAliases;
    }
    String[] columnsList = mapstr.split(",");

    for (String columnsWithAlias : columnsList) {
      String[] columnWithAlias = columnsWithAlias.split("->");
      if (columnWithAlias.length == 2) {
        columnsWithAliases.put(columnWithAlias[0], columnWithAlias[1]);
      }
    }
    return columnsWithAliases;
  }

  private static WithTimestamps<KV<ByteString, ChangeStreamMutation>> introduceTimestamps() {
    return WithTimestamps.of(
        (SerializableFunction<KV<ByteString, ChangeStreamMutation>, Instant>)
            input -> {
              if (input == null || input.getValue() == null) {
                return null;
              } else {
                return Instant.ofEpochMilli(input.getValue().getCommitTimestamp().toEpochMilli());
              }
            });
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
}
