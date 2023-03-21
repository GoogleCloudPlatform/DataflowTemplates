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
package com.google.cloud.syndeo.perf;

import static com.google.cloud.syndeo.perf.AnyToAnySyndeoTestLT.buildPipelinePayload;

import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.v1.SyndeoV1;
import com.google.cloud.teleport.it.PipelineUtils;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class SyndeoTestingPairsIT {
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String ZONE_SUFFIX = "-c";
  private static final String TEST_ID = "syndeo-any-to-any-" + UUID.randomUUID();

  private static final Integer NUM_ROWS_FOR_TEST = 300;

  private static final Logger LOG = LoggerFactory.getLogger(SyndeoTestingPairsIT.class);

  private static final List<ResourceManager> RESOURCE_MANAGERS = new ArrayList<>();

  @Rule public TestPipeline syndeoPipeline = TestPipeline.create();

  @Rule public TestPipeline dataGenerator = TestPipeline.create();

  @Parameterized.Parameters(name = "{index}: {0}")
  public static List<SourceSinkUrns> data() {
    return new ArrayList<>(
        new HashSet<>(
            List.of(
                // We test all sinks against a Pubsub source
//                SourceSinkUrns.create(
//                    "beam:schematransform:org.apache.beam:kafka_read:v1",
//                    "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
//                    true)
                //                SourceSinkUrns.create(
                //                        "beam:schematransform:org.apache.beam:kafka_read:v1",
                //                    "beam:schematransform:org.apache.beam:spanner_write:v1",
                //                    true),
                //                SourceSinkUrns.create(
                //                        "beam:schematransform:org.apache.beam:kafka_read:v1",
                //                    "beam:schematransform:org.apache.beam:kafka_write:v1",
                //                    true),
                //                SourceSinkUrns.create(
                //                    "syndeo:schematransform:com.google.cloud:pubsub_read:v1",
                //                    "beam:schematransform:org.apache.beam:file_write:v1",
                //                    true),
                //                SourceSinkUrns.create(
                //                        "beam:schematransform:org.apache.beam:kafka_read:v1",
                //                    "beam:schematransform:org.apache.beam:pubsublite_write:v1",
                //                    true),
                //                SourceSinkUrns.create(
                //                        "beam:schematransform:org.apache.beam:kafka_read:v1",
                //                    "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
                //                    true),
                //
                //                // We test all sources against a BigQuery sink
                                SourceSinkUrns.create(

                 "beam:schematransform:org.apache.beam:bigquery_storage_read:v1",

                 "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                                    false),
                //                SourceSinkUrns.create(
                //                    "beam:schematransform:org.apache.beam:kafka_read:v1",
                //
                // "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                //                    true),
                                SourceSinkUrns.create(
                                    "beam:schematransform:org.apache.beam:pubsublite_read:v1",

                 "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                                        true),
                                SourceSinkUrns.create(
                                    "syndeo:schematransform:com.google.cloud:pubsub_read:v1",

                 "beam:schematransform:org.apache.beam:bigquery_storage_write:v1",
                                        true)
                )));
  }

  private final SourceSinkUrns pairToTest;

  public SyndeoTestingPairsIT(SourceSinkUrns pairToTest) {
    this.pairToTest = pairToTest;
  }

  @Test
  public void testSyndeoTemplateBasicLocal() throws IOException, InterruptedException {
    LOG.info("Running test for {}", pairToTest);
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(
                "--blockOnRun=false",
                "--runner=DirectRunner",
                // TODO(pabloem): Avoid passing this value. Instead use a proper auto-defined value.
                "--numStorageWriteApiStreams=10")
            .create();
    run(options, options, Duration.ofMinutes(3));
  }

  @Test
  public void testSyndeoTemplateDataflow() throws IOException, InterruptedException {
    PipelineOptions syndeoOptions =
        PipelineOptionsFactory.fromArgs(
                "--blockOnRun=false",
                "--runner=DataflowRunner",
                "--region=" + REGION,
                "--project=" + PROJECT,
                "--experiments=enable_streaming_engine",
                "--experiments=enable_streaming_auto_sharding=true",
                "--experiments=streaming_auto_sharding_algorithm=FIXED_THROUGHPUT",
                "--numStorageWriteApiStreams=10")
            .create();
    PipelineOptions dataGenOptions =
        PipelineOptionsFactory.fromArgs(
                "--blockOnRun=false",
                "--runner=DirectRunner",
                // TODO(pabloem): Avoid passing this value. Instead use a proper auto-defined value.
                "--numStorageWriteApiStreams=10")
            .create();
    run(syndeoOptions, dataGenOptions, Duration.ofMinutes(1));
  }

  public void run(
      PipelineOptions syndeoOptions, PipelineOptions dataGenOptions, Duration timeoutMultiplier)
      throws IOException, InterruptedException {

    LOG.info("Getting the source provider for {}", pairToTest.getSourceUrn());
    AnyToAnySyndeoTestLT.TransformProvider sourceProvider =
        AnyToAnySyndeoTestLT.SOURCE_PROVIDERS.get(pairToTest.getSourceUrn());
    ResourceManager sourceRm = sourceProvider.getResourceManagerBuilder().get();
    RESOURCE_MANAGERS.add(sourceRm);
    LOG.info("Building configuration parameters for source.");
    AnyToAnySyndeoTestLT.SinkAndSourceConfigs sourceConfigs =
        sourceProvider.getConfigurationParametersBuilder().apply(sourceRm);
    LOG.info("Source configuration parameters: {}", sourceConfigs);

    LOG.info("Getting the sink provider for {}", pairToTest.getSinkUrn());
    AnyToAnySyndeoTestLT.TransformProvider sinkProvider =
        AnyToAnySyndeoTestLT.SINK_PROVIDERS.get(pairToTest.getSinkUrn());
    ResourceManager sinkRm = sinkProvider.getResourceManagerBuilder().get();
    RESOURCE_MANAGERS.add(sinkRm);
    LOG.info("Building configuration parameters for sink.");
    AnyToAnySyndeoTestLT.SinkAndSourceConfigs sinkConfigs =
        sinkProvider.getConfigurationParametersBuilder().apply(sinkRm);
    LOG.info("Sink configuration parameters: {}", sinkConfigs);

    String generatorPipelinePayload =
        buildPipelinePayload(dataGeneratorConfiguration(), sourceConfigs.getSinkConfig());
    SyndeoV1.PipelineDescription generatorDescription =
        SyndeoTemplate.buildFromJsonPayload(generatorPipelinePayload);
    LOG.info("Building data generator pipeline from configuration: {}", generatorPipelinePayload);
    SyndeoTemplate.buildPipeline(dataGenerator, generatorDescription);

    LOG.info("Starting data generation pipeline");
    PipelineResult generatorResult = dataGenerator.run(dataGenOptions);

    if (!pairToTest.getIsStreaming()) {
      LOG.info(
          "Waiting for data generation pipeline to finish before starting Syndeo pipeline"
              + " because the source is a batch source.");
      generatorResult.waitUntilFinish();
    }

    String syndeoPipelinePayload =
        buildPipelinePayload(sourceConfigs.getSourceConfig(), sinkConfigs.getSinkConfig());
    SyndeoV1.PipelineDescription syndeoPipeDescription =
        SyndeoTemplate.buildFromJsonPayload(syndeoPipelinePayload);
    LOG.info("Building syndeo test pipeline from configuration: {}", syndeoPipelinePayload);
    SyndeoTemplate.buildPipeline(syndeoPipeline, syndeoPipeDescription);

    LOG.info("Starting syndeo test pipeline");
    PipelineResult syndeoResult = syndeoPipeline.run(options);

    if (pairToTest.getIsStreaming()) {
      // Data generator will finish at some point because it creates a limited amount of data (300
      // rows).
      LOG.info("Waiting for data generation pipeline to finish.");
      generatorResult.waitUntilFinish();
    }

    LOG.info(
        "Waiting up to 4 minutes for syndeo test pipeline to finish processing all the input."
            + " Input size: {} elements",
        getCounterValue("elementsProcessed", generatorResult));
        boolean completed = PipelineUtils.waitUntil(
            syndeoResult,
            () -> getCounterValue("elementsProcessed", syndeoResult).equals(NUM_ROWS_FOR_TEST),
            4 * timeoutMultiplier.toMillis());

    LOG.info(
        "Pipeline {} in processing all elements. Cancelling and waiting "
            + "for cancellation to succeed.",
        completed ? "succeeded" : "failed");
    generatorResult.cancel();
    syndeoResult.cancel();

    PipelineUtils.waitUntil(
        syndeoResult,
        () ->
            Set.of(
                    PipelineResult.State.CANCELLED,
                    PipelineResult.State.DONE,
                    PipelineResult.State.STOPPED)
                .contains(syndeoResult.getState()),
        3 * timeoutMultiplier.toMillis());

    boolean success = getCounterValue("elementsProcessed", syndeoResult) > NUM_ROWS_FOR_TEST * 0.95;
    if (!success) {
      throw new AssertionError(
          "Processed a total of "
              + getCounterValue("elementsProcessed", syndeoResult)
              + " elements. Expectded "
              + NUM_ROWS_FOR_TEST);
    }
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(RESOURCE_MANAGERS.toArray(new ResourceManager[0]));
    RESOURCE_MANAGERS.clear();
  }

  static Long getCounterValue(String counterName, PipelineResult pipelineResult) {
    return StreamSupport.stream(
            pipelineResult.metrics().allMetrics().getCounters().spliterator(), false)
        .filter(counterResult -> counterResult.getName().getName().equals(counterName))
        .findFirst()
        .orElse(
            MetricResult.create(MetricKey.create("any", MetricName.named("any", "any")), -1L, -1L))
        .getAttempted();
  }

  static Map<String, Object> dataGeneratorConfiguration() {
    return Map.of(
        "urn",
        "syndeo_test:schematransform:com.google.cloud:generate_data:v1",
        "configurationParameters",
        Map.of("numRows", NUM_ROWS_FOR_TEST, "runtimeSeconds", 60, "useNestedSchema", true));
  }

  @AutoValue
  abstract static class SourceSinkUrns {
    public abstract String getSourceUrn();

    public abstract String getSinkUrn();

    public abstract Boolean getIsStreaming();

    public static SourceSinkUrns create(String sourceUrn, String sinkUrn, Boolean isStreaming) {
      return new AutoValue_SyndeoTestingPairsIT_SourceSinkUrns(sourceUrn, sinkUrn, isStreaming);
    }
  }
}
