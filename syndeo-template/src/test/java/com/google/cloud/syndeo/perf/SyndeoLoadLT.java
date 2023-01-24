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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.pubsublite.PubsubLiteResourceManager;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Category(TemplateLoadTest.class)
@RunWith(JUnit4.class)
public class SyndeoLoadLT {
  @Rule public TestPipeline dataGenerator = TestPipeline.create();

  @Rule public TestPipeline syndeoPipeline = TestPipeline.create();

  @Rule
  public Timeout timeoutRule =
      Timeout.seconds(
          60L
              * TEST_CONFIGS.get(TEST_CONFIG).getDurationMinutes()
              * 3); // 3x the duration of the injection pipeline

  private static final String PROJECT = TestProperties.project();
  private static final String LOCATION = TestProperties.region();
  private static final String ZONE_SUFFIX = "-c";
  private static final String TEST_CONFIG =
      TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
  // 30 characters is the maximum length for some literals, so we cap it here.
  private final String testUuid = ("syndeo-" + UUID.randomUUID()).substring(0, 30);
  private static final Long PUBSUB_LITE_CAPACITY = 512L;

  private static final PubsubLiteResourceManager pubsubLite =
      new PubsubLiteResourceManager.DefaultPubsubliteResourceManager();
  private BigtableResourceManager bigtable = null;

  @Before
  public void beforeTest() throws IOException {
    bigtable = DefaultBigtableResourceManager.builder(testUuid, PROJECT).build();
  }

  @After
  public void afterTest() {
    pubsubLite.cleanupAll();
    bigtable.cleanupAll();
  }

  @AutoValue
  abstract static class PubsubLiteToBigTableConfiguration {
    public abstract Long getNumRows();

    public abstract Integer getDurationMinutes();

    public abstract String getRunner();

    public static PubsubLiteToBigTableConfiguration create(
        Long throughput, Integer durationMinutes, String runner) {
      return new AutoValue_SyndeoLoadLT_PubsubLiteToBigTableConfiguration(
          throughput, durationMinutes, runner);
    }
  }

  private static final PubsubLiteToBigTableConfiguration LOCAL_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(1_000L, 1, "DirectRunner");
  private static final PubsubLiteToBigTableConfiguration DATAFLOW_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(50_000_000L, 20, "DataflowRunner");
  private static final PubsubLiteToBigTableConfiguration LARGE_DATAFLOW_TEST_CONFIG =
      PubsubLiteToBigTableConfiguration.create(500_000_000L, 60, "DataflowRunner");

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
        pubsubLite.createReservation(reservationName, LOCATION, PROJECT, PUBSUB_LITE_CAPACITY);
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
                dataGenerator, testConfig.getNumRows(), testConfig.getDurationMinutes()))
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
                                    .build())))
                .buildTransform());
    PipelineResult generatorResult =
        dataGenerator.runWithAdditionalOptionArgs(
            List.of("--runner=" + testConfig.getRunner(), "--experiments=enable_streaming_engine"));

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
                            "dataFormat",
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

    SyndeoTemplate.buildPipeline(syndeoPipeline, SyndeoTemplate.buildFromJsonPayload(jsonPayload));
    PipelineResult syndeoResult =
        syndeoPipeline.runWithAdditionalOptionArgs(
            List.of("--runner=" + testConfig.getRunner(), "--experiments=enable_streaming_engine"));
    generatorResult.waitUntilFinish();

    // TODO(pabloem): Handle cancelling the Syndeo Pipeline based on throughput/processing metrics.
    Thread.sleep(60 * 5_000); // 5 minutes
    syndeoResult.cancel();
  }
}
