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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.Timestamp;
import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.aiplatform.v1.IndexName;
import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.aiplatform.v1.UpsertDatapointsRequest;
import com.google.cloud.aiplatform.v1.UpsertDatapointsResponse;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadChangeStreamOptions;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions.ReadOptions;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToLogsOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ModType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import com.google.cloud.aiplatform.v1.Restrict;

@Template(
    name = "Bigtable_Change_Streams_to_Logs",
    category = TemplateCategory.STREAMING,
    displayName = "Bigtable Change Streams to Logs",
    description = "a test pipeline",
    optionsClass = BigtableChangeStreamsToLogsOptions.class,
    contactInformation = "https://cloud.google.com/support",
    optionsOrder = {
      BigtableChangeStreamsToLogsOptions.class,
      ReadChangeStreamOptions.class,
      ReadOptions.class
    },
    skipOptions = {
      // TODO - What does it mean to skip these, and why these ones specifically?
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs"
    },
    flexContainerName = "bigtable-changestreams-to-logs",
    streaming = true)
public class BigtableChangeStreamsToLogs {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToLogs.class);

  // Index         projects/818418350420/locations/us-east1/indexes/7774602739538984960
  // Index endpoint projects/818418350420/locations/us-east1/indexEndpoints/6641762715301838848

  private static final String ENDPOINT = "us-east1-aiplatform.googleapis.com";
  private static final String PROJECT_ID = "818418350420";
  private static final String REGION = "us-east1";
  private static final String INDEX_ID = "7774602739538984960"; // index ID
  // #private static final String INDEX_ID = "6641762715301838848"; // endpoint ID
  private static final int DIMENSIONS = 768;

  public static void main(String[] args) {
    BigtableChangeStreamsToLogsOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToLogsOptions.class);

    run(options);
  }

  public static PipelineResult run(BigtableChangeStreamsToLogsOptions options) {
    // TODO(meagar): What do these do?
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    String bigtableProjectId = getBigtableProjectId(options);

    LOG.debug("Config:");
    LOG.debug("  - bigtableProjectId {}", bigtableProjectId);

    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));
    LOG.debug("  - startTimestamp {}", startTimestamp);
    LOG.debug("  - bigtableReadInstanceId {}", options.getBigtableReadInstanceId());
    LOG.debug("  - bigtableReadTableId {}", options.getBigtableReadTableId());
    LOG.debug("  - bigtableChangeStreamAppProfile {}", options.getBigtableChangeStreamAppProfile());

    final Pipeline pipeline = Pipeline.create(options);

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            // TODO(meagar): What is this?
            // .withExistingPipelineOptions(
            //     options.getBigtableChangeStreamResume()
            //         ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
            //         : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS)
            // )
            .withProjectId(bigtableProjectId)
            // TODO(meagar): What is this?
            // .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withStartTime(startTimestamp);

    pipeline
        .apply("Read Bigtable Change Stream", readChangeStream)
        .apply(Values.create()) // TODO(meagar): Why is this here?
        .apply(
            "Log Changes",
            ParDo.of(
                new DoFn<ChangeStreamMutation, String>() {
                  @ProcessElement
                  public void process(
                      @Element ChangeStreamMutation input, OutputReceiver<String> recv) {

                    try {
                      LOG.info("Doing a dataset upload");
                      IndexServiceSettings indexServiceSettings =
                          IndexServiceSettings.newBuilder().setEndpoint(ENDPOINT + ":443").build();
                      IndexServiceClient indexServiceClient =
                          IndexServiceClient.create(indexServiceSettings);

                      // JSONArray embeddings = {};
                      String id = "12345"; //

                      LOG.info("WRiting with id length {}", id.length());
                      ArrayList<Float> floats = new ArrayList<Float>();

                      for (int i = 0; i < DIMENSIONS; i++) {
                        floats.add(0.1f);
                      }

                      // ArrayList<Restrict>
                      // IndexDatapoint.Types.CrowdingTag t = new
                      // IndexDatapoint.Types.CrowdingData();
                      var tag =
                          IndexDatapoint.CrowdingTag.newBuilder()
                              .setCrowdingAttribute("foo")
                              .build();

                      // String[] restrictsStrings = {"foo", "bar", "baz"};
                      var restricts =
                          IndexDatapoint.Restriction.newBuilder()
                              .addAllAllowList(List.of("foo", "bar", "baz"))
                              .setNamespace("wat")
                              .build();

                      IndexDatapoint dp1 =
                          IndexDatapoint.newBuilder()
                              .setDatapointId(id.toString())
                              .addAllFeatureVector(floats)
                              .addRestricts(restricts)
                              .setCrowdingTag(tag)
                              .build();

                      IndexDatapoint dp2 =
                          IndexDatapoint.newBuilder()
                              .setDatapointId(id.toString())
                              .addAllFeatureVector(floats)
                              .addRestricts(restricts)
                              .build();

                      List<IndexDatapoint> dps = Arrays.asList(dp1, dp2);

                      UpsertDatapointsRequest request =
                          UpsertDatapointsRequest.newBuilder()
                              .setIndex(IndexName.of(PROJECT_ID, REGION, INDEX_ID).toString())
                              .addAllDatapoints(dps)
                              .build();

                      LOG.info("Sending request {}", request);
                      UpsertDatapointsResponse response =
                          indexServiceClient.upsertDatapoints(request);

                      LOG.info("Response: {}", response.toString());
                    } catch (IOException e) {
                      LOG.error("Error: ", e);
                    }

                    LOG.info("Received Change:");
                    LOG.info("  - rowkey: {}", input.getRowKey().toStringUtf8());
                    LOG.info("  - type: {}", input.getType());
                    LOG.info("  - Mods:");
                    for (Entry entry : input.getEntries()) {
                      LOG.debug("    - mod: {}", entry);
                      LOG.debug("    - class {}", entry.getClass());
                      if (entry instanceof SetCell) {
                        LOG.info("    - type: SetCell");
                        SetCell m = (SetCell) entry;
                        LOG.info("    - familyName, {}", m.getFamilyName());
                        LOG.info("    - qualifier, {}", m.getQualifier().toStringUtf8());
                        LOG.info("    - timestamp, {}", m.getTimestamp());
                        LOG.info("    - value, {}", m.getValue().toStringUtf8());
                      } else if (entry instanceof DeleteCells) {
                        LOG.info("    - type: DeleteCell");
                        DeleteCells m = (DeleteCells) entry;
                        LOG.info("    - familyName {}", m.getFamilyName());
                        LOG.info("    - qualifier, {}", m.getQualifier().toStringUtf8());
                        LOG.info("    - timestamp, {}", m.getTimestampRange());
                      } else if (entry instanceof DeleteFamily) {
                        LOG.info("    - type: DeleteFamily");
                        DeleteFamily m = (DeleteFamily) entry;
                        LOG.info("    - familyName {}", m.getFamilyName());

                        // TODO(meagar): Not sure this is a special case, or just treated as a
                        // generic mutation
                      } else {
                        LOG.warn("Unexpected mod type");
                      }
                    }

                    // TODO
                    recv.output("foo");
                  }
                }));

    return pipeline.run();
  }

  static void upsertDatapoints(ChangeStreamMutation input) {
    ArrayList<IndexDatapoint> datapoints = new ArrayList<IndexDatapoint>();

    try {
      IndexServiceSettings settings = IndexServiceSettings.newBuilder().setEndpoint("foo").build();
      IndexServiceClient client = IndexServiceClient.create(settings);

      // for (Entry entry : input.getEntries()) {
      //     IndexDatapoint dp = IndexDatapoint.newBuilder()
      //         .setDatapointId(id.toString())
      //         .addAllFeatureVector(floats)
      //         .build();
      // }

    } catch (IOException e) {
      LOG.error("Error: {}", e);
    }
  }

  private static String getBigtableProjectId(BigtableChangeStreamsToLogsOptions options) {
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

  private static ModType getModType(Entry entry) {
    if (entry instanceof SetCell) {
      return ModType.SET_CELL;
    } else if (entry instanceof DeleteCells) {
      return ModType.DELETE_CELLS;
    } else if (entry instanceof DeleteFamily) {
      return ModType.DELETE_FAMILY;
    }
    return ModType.UNKNOWN;
  }
}
