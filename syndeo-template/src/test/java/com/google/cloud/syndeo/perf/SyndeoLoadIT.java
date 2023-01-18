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

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformProvider;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.pubsublite.PubsubLiteResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class SyndeoLoadIT {
  @Rule public TestPipeline dataGenerator = TestPipeline.create();

  @Rule public TestPipeline syndeoPipeline = TestPipeline.create();

  private static final String PROJECT = TestProperties.project();
  private static final String LOCATION = TestProperties.region();
  private static final String ZONE_SUFFIX = "-c";
  // 30 characters is the maximum length for some literals, so we cap it here.
  private final String testUuid = ("syndeo-" + UUID.randomUUID()).substring(0, 30);

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

  @Test
  public void testPubsubLiteToBigTableSyndeoFlow() throws NoSuchSchemaException {
    Long pubsubLiteThroughput = 512L;
    String reservationName = "syndeo-reservation-test" + testUuid;
    String topicName = "syndeo-topic-test-" + testUuid;
    String subscriptionName = "syndeo-subscription-test-" + testUuid;

    // Create PS Lite resources
    ReservationPath reservationPath =
        pubsubLite.createReservation(reservationName, LOCATION, PROJECT, pubsubLiteThroughput);
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

    // 700GB in the dataset
    long numRows = 1_000_000_000;

    PCollectionRowTuple.of(
            "input",
            SyndeoLoadTestUtils.inputData(dataGenerator, numRows, Duration.standardMinutes(25)))
        .apply(
            new PubsubLiteWriteSchemaTransformProvider()
                .from(
                    // TODO(pabloem): Avoid relying on SchemaRegistry and make from(ConfigClass)
                    // public.
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
                                .build()))
                .buildTransform());
    PipelineResult generatorResult = dataGenerator.run();

    PCollection<Row> inputPcoll =
        PCollectionRowTuple.empty(syndeoPipeline)
            .apply(
                "psLiteRead",
                new PubsubLiteReadSchemaTransformProvider()
                    .from(
                        PubsubLiteReadSchemaTransformProvider
                            .PubsubLiteReadSchemaTransformConfiguration.builder()
                            .setLocation(LOCATION)
                            .setProject(PROJECT)
                            .setDataFormat("AVRO")
                            .setSchema(
                                AvroUtils.toAvroSchema(SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA)
                                    .toString())
                            .setSubscriptionName(subscriptionName)
                            .build())
                    .buildTransform())
            .get("output");
    PCollectionRowTuple.of("input", inputPcoll)
        .apply(
            "btWrite",
            new BigTableWriteSchemaTransformProvider()
                .from(
                    BigTableWriteSchemaTransformConfiguration.builder()
                        .setProjectId(PROJECT)
                        .setTableId(bigTableName)
                        .setInstanceId(bigtable.getInstanceId())
                        .setKeyColumns(
                            Collections.singletonList(
                                "sha1")) // The sha1 of a commit is a byte array.
                        .build())
                .buildTransform());
    PipelineResult syndeoResult = syndeoPipeline.run();
    syndeoResult.waitUntilFinish();
    generatorResult.waitUntilFinish();
  }
}
