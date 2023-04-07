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
package com.google.cloud.syndeo.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.storage.Notification;
import com.google.cloud.syndeo.perf.SyndeoLoadTestUtils;
import com.google.cloud.syndeo.transforms.files.SyndeoFilesReadSchemaTransformProvider;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.storage.GcsResourceManager;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.AvroTypeException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
@RunWith(JUnit4.class)
public class ReadFilesTransformIT {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFilesTransformIT.class);

  private static final List<ResourceManager> RESOURCE_MANAGERS = new ArrayList<>();

  private static final String BUCKET = TestProperties.artifactBucket();
  private static final String PROJECT = TestProperties.project();
  private static final String TEST_ID = "readfiles-transform-it-" + UUID.randomUUID();
  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);

  @Rule public final TestName testName = new TestName();

  @Rule public final TestPipeline mainPipeline = TestPipeline.create();

  private SubscriptionName pubsubSubscription = null;
  private TopicName pubsubTopic = null;
  private GcsResourceManager gcsResourceManager = null;
  private PubsubResourceManager pubsubResourceManager = null;
  private String gcsPrefix = null;

  @Before
  public void setUpPubSubNotifications() throws IOException {
    gcsResourceManager = GcsResourceManager.builder().setBucket(BUCKET).setProject(PROJECT).build();
    RESOURCE_MANAGERS.add(gcsResourceManager);

    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(TEST_ID, PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    RESOURCE_MANAGERS.add(pubsubResourceManager);
    pubsubTopic = pubsubResourceManager.createTopic(TEST_ID + testName.getMethodName());
    LOG.info("Successfully created topic {}", pubsubTopic);
    pubsubSubscription =
        pubsubResourceManager.createSubscription(pubsubTopic, "sub-" + pubsubTopic.getTopic());
    LOG.info("Successfully created subscription {}", pubsubSubscription);

    gcsPrefix = TEST_ID + testName.getMethodName();
    Notification notification =
        gcsResourceManager.createNotification(pubsubTopic.toString(), gcsPrefix);
    LOG.info("Successfully created notification {}", notification);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(RESOURCE_MANAGERS.toArray(new ResourceManager[0]));
    pubsubSubscription = null;
    pubsubTopic = null;
    pubsubResourceManager = null;
    gcsResourceManager = null;
  }

  @Test
  public void testFilesAreConsumed() throws IOException, InterruptedException {
    Thread dataCopy = new Thread(() -> copyFilesToGcs());
    dataCopy.start();

    Counter elementCounter = Metrics.counter("test", "elementCounter");
    PCollectionRowTuple.empty(mainPipeline)
        .apply(
            new SyndeoFilesReadSchemaTransformProvider()
                .from(
                    SyndeoFilesReadSchemaTransformProvider
                        .SyndeoFilesReadSchemaTransformConfiguration.builder()
                        .setFormat("AVRO")
                        .setSchema(
                            AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                .toString())
                        .setPubsubSubscription(pubsubSubscription.toString())
                        .build())
                .buildTransform())
        .get("output")
        .apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    row -> {
                      elementCounter.inc();
                      return null;
                    }));

    PipelineResult result =
        mainPipeline.run(PipelineOptionsFactory.fromArgs("--blockOnRun=false").create());
    result.waitUntilFinish(Duration.standardSeconds(20));
    dataCopy.join();
    result.cancel();
    result
        .metrics()
        .allMetrics()
        .getCounters()
        .forEach(
            counterResult -> {
              LOG.info("Counter result: {}", counterResult);
              if (counterResult.getName().getName().equals("elementCounter")) {
                assertEquals(300L, counterResult.getCommitted().longValue());
              }
            });
  }

  @Test
  public void testBadErrorHandling() throws IOException {
    // Copying a proper avro file WITHOUT the right schema
    File badAvroFile2 = new File("src/test/resources/bad_avro_file_reads/bad_avro_file_2.avro");
    gcsResourceManager.copyFileToGcs(
        badAvroFile2.toPath(), Paths.get(gcsPrefix, badAvroFile2.getName()).toString());

    Counter errorCounter = Metrics.counter("test", "errorCounter");
    Counter elementCounter = Metrics.counter("test", "elementCounter");
    PCollectionRowTuple result =
        PCollectionRowTuple.empty(mainPipeline)
            .apply(
                new SyndeoFilesReadSchemaTransformProvider()
                    .from(
                        SyndeoFilesReadSchemaTransformProvider
                            .SyndeoFilesReadSchemaTransformConfiguration.builder()
                            .setFormat("AVRO")
                            .setSchema(
                                AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                    .toString())
                            .setPubsubSubscription(pubsubSubscription.toString())
                            .build())
                    .buildTransform());

    Pipeline.PipelineExecutionException e =
        assertThrows(
            Pipeline.PipelineExecutionException.class,
            () -> {
              PipelineResult pipelineResult =
                  mainPipeline.run(PipelineOptionsFactory.fromArgs("--blockOnRun=false").create());
              pipelineResult.waitUntilFinish(Duration.standardSeconds(20));
            });
    assertEquals(e.getCause().getClass(), AvroTypeException.class);
  }

  @Test
  public void testBadErrorHandling2() throws IOException {
    // Copying a proper avro file WITHOUT the right schema
    File badAvroFile2 = new File("src/test/resources/bad_avro_file_reads/bad_avro_file.avro");
    gcsResourceManager.copyFileToGcs(
        badAvroFile2.toPath(), Paths.get(gcsPrefix, badAvroFile2.getName()).toString());

    Counter errorCounter = Metrics.counter("test", "errorCounter");
    Counter elementCounter = Metrics.counter("test", "elementCounter");
    PCollectionRowTuple result =
        PCollectionRowTuple.empty(mainPipeline)
            .apply(
                new SyndeoFilesReadSchemaTransformProvider()
                    .from(
                        SyndeoFilesReadSchemaTransformProvider
                            .SyndeoFilesReadSchemaTransformConfiguration.builder()
                            .setFormat("AVRO")
                            .setSchema(
                                AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                    .toString())
                            .setPubsubSubscription(pubsubSubscription.toString())
                            .build())
                    .buildTransform());

    Pipeline.PipelineExecutionException e =
        assertThrows(
            Pipeline.PipelineExecutionException.class,
            () -> {
              PipelineResult pipelineResult =
                  mainPipeline.run(PipelineOptionsFactory.fromArgs("--blockOnRun=false").create());
              pipelineResult.waitUntilFinish(Duration.standardSeconds(20));
            });
    assertEquals(e.getCause().getClass(), IOException.class);
  }

  @Test
  public void testGoodErrorHandling() throws IOException, InterruptedException {
    Thread dataCopy = new Thread(() -> copyFilesToGcs());
    dataCopy.start();

    // Pass a proper JSON string without the right fields
    pubsubResourceManager.publish(pubsubTopic, Map.of(), ByteString.copyFromUtf8("{}"));
    // Pass an improper JSON string without the right fields
    pubsubResourceManager.publish(pubsubTopic, Map.of(), ByteString.copyFromUtf8("{{"));

    // Copying an EMPTY file
    File badAvroFile2 = new File("src/test/resources/bad_avro_file_reads/empty_avro_file.avro");
    gcsResourceManager.copyFileToGcs(
        badAvroFile2.toPath(), Paths.get(gcsPrefix, badAvroFile2.getName()).toString());

    Counter errorCounter = Metrics.counter("test", "errorCounter");
    Counter elementCounter = Metrics.counter("test", "elementCounter");
    PCollectionRowTuple result =
        PCollectionRowTuple.empty(mainPipeline)
            .apply(
                new SyndeoFilesReadSchemaTransformProvider()
                    .from(
                        SyndeoFilesReadSchemaTransformProvider
                            .SyndeoFilesReadSchemaTransformConfiguration.builder()
                            .setFormat("AVRO")
                            .setSchema(
                                AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA)
                                    .toString())
                            .setPubsubSubscription(pubsubSubscription.toString())
                            .build())
                    .buildTransform());

    result
        .get("errors")
        .apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    row -> {
                      errorCounter.inc();
                      return null;
                    }));

    result
        .get("output")
        .apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    row -> {
                      elementCounter.inc();
                      return null;
                    }));

    PipelineResult pipelineResult =
        mainPipeline.run(PipelineOptionsFactory.fromArgs("--blockOnRun=false").create());
    pipelineResult.waitUntilFinish(Duration.standardSeconds(20));
    dataCopy.join();
    pipelineResult.cancel();
    pipelineResult
        .metrics()
        .allMetrics()
        .getCounters()
        .forEach(
            counterResult -> {
              LOG.info("Counter result: {}", counterResult);
              if (counterResult.getName().getName().equals("errorCounter")) {
                assertEquals(2L, counterResult.getCommitted().longValue());
              }
              if (counterResult.getName().getName().equals("elementCounter")) {
                assertEquals(300L, counterResult.getCommitted().longValue());
              }
            });
  }

  void copyFilesToGcs() {
    File avroDir = new File("src/test/resources/avro_file_reads/");
    for (File avroFile : avroDir.listFiles()) {
      try {
        gcsResourceManager.copyFileToGcs(
            avroFile.toPath(), Paths.get(gcsPrefix, avroFile.getName()).toString());
        Thread.sleep(1L);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
