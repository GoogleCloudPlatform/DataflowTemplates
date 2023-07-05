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

import static com.google.cloud.syndeo.transforms.SyndeoStatsSchemaTransformProvider.getElementsProcessed;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.gcp.pubsublite.PubsubliteResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.joda.time.Instant;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Category(TemplateLoadTest.class)
@RunWith(JUnit4.class)
public class SyndeoPubsubLiteToBigTableLT {
  private static final Logger LOG = LoggerFactory.getLogger(SyndeoPubsubLiteToBigTableLT.class);
  @Rule public TestPipeline dataGenerator = TestPipeline.create();

  @Rule public TestPipeline syndeoPipeline = TestPipeline.create();

  private static final String PROJECT = TestProperties.project();
  private static final String LOCATION = TestProperties.region();
  private static final String ZONE_SUFFIX = "-c";
  private static final String TEST_CONFIG =
      TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
  // 30 characters is the maximum length for some literals, so we cap it here.
  private final String testUuid = ("syndeo-" + UUID.randomUUID()).substring(0, 30);

  private static final PubsubliteResourceManager pubsubLite = new PubsubliteResourceManager();
  private BigtableResourceManager bigtable = null;
  private Instant testStart = null;

  @Before
  public void beforeTest() throws IOException {
    bigtable = BigtableResourceManager.builder(testUuid, PROJECT).build();
    testStart = Instant.now();
  }

  @After
  public void afterTest() {
    ResourceManagerUtils.cleanResources(pubsubLite, bigtable);
  }

  @AutoValue
  abstract static class PubsubLiteToBigTableConfiguration {
    public abstract Long getNumRows();

    public abstract Integer getDurationMinutes();

    public abstract String getRunner();

    public abstract Long pubsubLiteCapacity();

    public static PubsubLiteToBigTableConfiguration create(
        Long throughput, Integer durationMinutes, String runner, Long pubsubLiteCapacity) {
      return new AutoValue_SyndeoPubsubLiteToBigTableLT_PubsubLiteToBigTableConfiguration(
          throughput, durationMinutes, runner, pubsubLiteCapacity);
    }
  }

  private static final PubsubLiteToBigTableConfiguration LOCAL_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(1_000L, 3, "DirectRunner", 100L);
  private static final PubsubLiteToBigTableConfiguration DATAFLOW_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(50_000_000L, 20, "DataflowRunner", 512L);
  private static final PubsubLiteToBigTableConfiguration LARGE_DATAFLOW_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(500_000_000L, 80, "DataflowRunner", 512L);

  private static final Map<String, PubsubLiteToBigTableConfiguration> TEST_CONFIGS =
      Map.of(
          "local",
          LOCAL_TEST_CONFIG,
          "medium",
          DATAFLOW_TEST_CONFIG,
          "large",
          LARGE_DATAFLOW_TEST_CONFIG);

  @Test
  public void testPubsubLiteToBigTableSyndeoFlow()
      throws NoSuchSchemaException, IOException, InterruptedException {
    PubsubLiteToBigTableConfiguration testConfig = TEST_CONFIGS.get(TEST_CONFIG);
    if (testConfig == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              TEST_CONFIG, TEST_CONFIGS.keySet()));
    }
    String reservationName = "syndeo-reservation-test" + testUuid;
    String topicName = "syndeo-topic-test-" + testUuid;
    String subscriptionName = "syndeo-subscription-test-" + testUuid;

    // Create PS Lite resources
    ReservationPath reservationPath =
        pubsubLite.createReservation(
            reservationName, LOCATION, PROJECT, testConfig.pubsubLiteCapacity());
    TopicName topicPath = pubsubLite.createTopic(topicName, reservationPath);
    SubscriptionName subscriptionPath =
        pubsubLite.createSubscription(reservationPath, topicPath, subscriptionName);

    // Create BigTable resources
    String bigTableCluster = "cluster-" + testUuid.substring(0, 10);
    String bigTableName = testUuid;
    bigtable.createInstance(
        Collections.singletonList(
            BigtableResourceManagerCluster.create(
                bigTableCluster, LOCATION + ZONE_SUFFIX, 10, StorageType.SSD)));
    bigtable.createTable(bigTableName, SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA.getFieldNames());

    PCollectionRowTuple.of(
            "input",
            SyndeoLoadTestUtils.inputData(
                dataGenerator,
                testConfig.getNumRows(),
                testConfig.getDurationMinutes() * 60L,
                SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA))
        .apply(
            "beam:schematransform:org.apache.beam:pubsublite_write:v1",
            new PubsubLiteWriteSchemaTransformProvider()
                .from(
                    // TODO(pabloem): Avoid relying on SchemaRegistry and make from(ConfigClass)
                    // public.
                    Objects.requireNonNull(
                        SchemaRegistry.createDefault()
                            .getToRowFunction(
                                PubsubLiteWriteSchemaTransformProvider
                                    .PubsubLiteWriteSchemaTransformConfiguration.class)
                            .apply(
                                PubsubLiteWriteSchemaTransformProvider
                                    .PubsubLiteWriteSchemaTransformConfiguration.builder()
                                    .setFormat("AVRO")
                                    .setTopicName(topicName)
                                    .setLocation(LOCATION)
                                    .setProject(PROJECT)
                                    .build()))));

    // Build JSON configuration for the template:
    String jsonPayload =
        new ObjectMapper()
            .writeValueAsString(
                Map.of(
                    "source",
                    Map.of(
                        "urn",
                        "beam:schematransform:org.apache.beam:pubsublite_read:v1",
                        "configurationParameters",
                        Map.of(
                            "subscriptionName",
                            subscriptionName,
                            "location",
                            LOCATION,
                            "project",
                            PROJECT,
                            "format",
                            "AVRO",
                            "schema",
                            AvroUtils.toAvroSchema(SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA)
                                .toString())),
                    "sink",
                    Map.of(
                        "urn",
                        "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
                        "configurationParameters",
                        Map.of(
                            "projectId",
                            PROJECT,
                            "tableId",
                            bigTableName,
                            "instanceId",
                            bigtable.getInstanceId(),
                            "keyColumns",
                            List.of("sha1")))));

    LOG.info("Building syndeo pipeline");
    SyndeoTemplate.buildPipeline(syndeoPipeline, SyndeoTemplate.buildFromJsonPayload(jsonPayload));

    LOG.info("Launching data generation pipeline");
    PipelineResult generatorResult =
        dataGenerator.runWithAdditionalOptionArgs(
            List.of(
                "--runner=" + testConfig.getRunner(),
                "--experiments=enable_streaming_engine",
                "--blockOnRun=false"));
    LOG.info("Launching Syndeo pipeline");
    PipelineResult syndeoResult =
        syndeoPipeline.runWithAdditionalOptionArgs(
            List.of(
                "--runner=" + testConfig.getRunner(),
                "--experiments=enable_streaming_engine",
                "--blockOnRun=false"));
    LOG.info("Waiting for data generation pipeline...");
    generatorResult.waitUntilFinish();
    LOG.info("Data generation pipeline concluded.");

    while (true) {
      Long inputElements = getElementsProcessed(generatorResult.metrics());
      Long syndeoProcessed = getElementsProcessed(syndeoResult.metrics());
      int testRuntime = new Period(testStart, Instant.now()).toStandardMinutes().getMinutes();
      LOG.info(
          "Test has ran for {} minutes. Input elements: {}. Elements processed: {}",
          testRuntime,
          inputElements,
          syndeoProcessed);
      if (0 < syndeoProcessed && 0 < inputElements && syndeoProcessed >= inputElements) {
        // HAPPY WE SUCCEEDED!
        syndeoResult.cancel();
        break;
      }
      if (testRuntime > testConfig.getDurationMinutes() * 3
          || syndeoResult.getState().isTerminal()) {
        syndeoResult.cancel();
        fail(
            String.format(
                "Test ran for %s minutes. Pipeline only processed %s elements "
                    + "out of %s. Ending the test with failure.",
                testRuntime, syndeoProcessed, inputElements));
      }
      // Sleep 10 seconds before trying again.
      Thread.sleep(10_000L);
    }
  }
}
