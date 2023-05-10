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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.OrderWith;
import org.junit.runner.RunWith;
import org.junit.runner.manipulation.Alphanumeric;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performance tests for {@link TextToPubsubStreamLT GCS Text to PubSub} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(TextToPubsubStream.class)
@RunWith(JUnit4.class)
@OrderWith(Alphanumeric.class)
public class TextToPubsubStreamLT extends TemplateLoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TextToPubsubStreamLT.class);
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/Stream_GCS_Text_to_Cloud_PubSub");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      TextToPubsubStreamLT.class.getSimpleName().toLowerCase();
  private static final String INPUT_PCOLLECTION =
      "Read Text Data/Via ReadFiles/Read all via FileBasedSource/Read ranges.out0";
  private static final String OUTPUT_PCOLLECTION = "Write to PubSub/MapElements/Map.out0";

  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final long NUM_MESSAGES_FOR_10GB = 35000000L;
  private static final long TIMEOUT_FOR_10_GB_TEST_MINUTES = 30;
  private static final long TIMEOUT_FOR_1_HOUR_TEST_MINUTES = 60;

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient gcsClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();

    Storage storageClient = createStorageClient(CREDENTIALS);

    gcsClient = GcsArtifactClient.builder(storageClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, gcsClient);
  }

  @Test
  public void test1Backlog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(Function.identity());
  }

  @Test
  public void test2SteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(Function.identity());
  }

  private void testBacklog(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Creates a topic on Pub/Sub.
    TopicName outputTopic = pubsubResourceManager.createTopic("topic");
    // Creates a subscription on Pub/Sub.
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES_FOR_10GB))
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setNumShards("20")
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    // Executes the data generator
    dataGenerator.execute(Duration.ofMinutes(TIMEOUT_FOR_10_GB_TEST_MINUTES));

    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("outputTopic", outputTopic.toString())
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*"))
            .build();
    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    // Waits until a number of messages condition is met
    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(TIMEOUT_FOR_10_GB_TEST_MINUTES)),
            () -> {
              Long currentMessages =
                  monitoringClient.getNumMessagesInSubscription(
                      project, outputSubscription.getSubscription());
              LOG.info(
                  "Found {} messages in output subscription, expected {} messages.",
                  currentMessages,
                  NUM_MESSAGES_FOR_10GB);
              return currentMessages != null && currentMessages >= NUM_MESSAGES_FOR_10GB;
            });

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Creates a topic on Pub/Sub.
    TopicName outputTopic = pubsubResourceManager.createTopic("topic");
    // Creates a subscription on Pub/Sub.
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("100000")
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setNumShards("20")
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();

    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("outputTopic", outputTopic.toString())
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*"))
            .build();
    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    // Executes the data generator and return approximate number of messages
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    long expectedMessages = (long) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);

    // Waits until a number of messages condition is met
    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(TIMEOUT_FOR_1_HOUR_TEST_MINUTES)),
            () -> {
              Long currentMessages =
                  monitoringClient.getNumMessagesInSubscription(
                      project, outputSubscription.getSubscription());
              LOG.info(
                  "Found {} messages in output subscription, expected {} messages.",
                  currentMessages,
                  expectedMessages);
              return currentMessages != null && currentMessages >= expectedMessages;
            });

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsClient.runId(), testName);
  }
}
