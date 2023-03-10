/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.pubsubtotext;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link PubsubToText PubSub to GCS Text} Flex template. */
@TemplateLoadTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextLT extends TemplateLoadTestBase {

  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Cloud_PubSub_to_GCS_Text_Flex");
  private static final String TEST_ROOT_DIR = PubsubToTextLT.class.getSimpleName().toLowerCase();
  private static final String INPUT_SUBSCRIPTION = "inputSubscription";
  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String NUM_WORKERS_KEY = "numWorkers";
  private static final String MAX_WORKERS_KEY = "maxWorkers";
  private static final String OUTPUT_FILENAME_PREFIX = "outputFilenamePrefix";
  private static final String DEFAULT_WINDOW_DURATION = "10s";
  private static final Pattern EXPECTED_PATTERN = Pattern.compile(".*subscription-output-.*");
  private static final String INPUT_PCOLLECTION = "Read PubSub Events/PubsubUnboundedSource.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write File(s)/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey.out0";
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final long NUM_MESSAGES = 35000000L;

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient gcsClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    Storage storageClient = createStorageClient(CREDENTIALS);
    gcsClient = GcsArtifactClient.builder(storageClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(gcsClient, pubsubResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, InterruptedException, ParseException {
    testBacklog10gb(Function.identity());
  }

  @Test
  public void testBacklog10gbUsingStreamingEngine()
      throws IOException, InterruptedException, ParseException {
    testBacklog10gb(config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "use_runner_v2"));
  }

  @Test
  public void testBacklog10gbUsingPrime() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "enable_prime"));
  }

  @Test
  public void testSteadyState1hr() throws IOException, InterruptedException, ParseException {
    testSteadyState1hr(Function.identity());
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws IOException, InterruptedException, ParseException {
    testSteadyState1hr(config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testSteadyState1hrUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(config -> config.addParameter("experiments", "use_runner_v2"));
  }

  @Test
  public void testSteadyState1hrUsingPrime()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(config -> config.addParameter("experiments", "enable_prime"));
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName;
    // Create topic and subscription
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    // Generate fake data to topic
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter(INPUT_SUBSCRIPTION, backlogSubscription.toString())
                    .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
                    .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath())
                    .addParameter(NUM_SHARDS_KEY, "20")
                    .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
                    .addParameter(NUM_WORKERS_KEY, "20"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(30)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, NUM_MESSAGES));

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(gcsClient.listArtifacts(name, EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  public void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName;
    TopicName topic = pubsubResourceManager.createTopic(testName + "input");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("100000")
            .setTopic(topic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
                    .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
                    .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath())
                    .addParameter(NUM_SHARDS_KEY, "20")
                    .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
                    .addParameter(NUM_WORKERS_KEY, "10"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    long expectedMessages = (long) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(20)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, expectedMessages));
    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(gcsClient.listArtifacts(name, EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsClient.runId(), testName);
  }
}
