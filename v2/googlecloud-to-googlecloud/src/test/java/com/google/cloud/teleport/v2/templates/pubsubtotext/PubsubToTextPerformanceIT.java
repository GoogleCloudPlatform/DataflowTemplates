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

import static com.google.cloud.teleport.it.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Performance test for testing performance of {@link PubsubToText} (Cloud_PubSub_to_GCS_Text_Flex).
 */
@TemplateIntegrationTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextPerformanceIT extends PerformanceBenchmarkingBase {

  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Cloud_PubSub_to_GCS_Text_Flex");
  private static final String TEST_ROOT_DIR =
      PubsubToTextPerformanceIT.class.getSimpleName().toLowerCase();
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
  private static final String OUTPUT_PTRANSFORM =
      "Write File(s)/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey";
  private static final long numMessages = 35000000L;

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient artifactClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void tearDown() {
    artifactClient.cleanupRun();
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testBacklog10gb() throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    // Create topic and subscription
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    // Generate fake data to topic
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(numMessages))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("150")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, backlogSubscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath())
            .addParameter(NUM_SHARDS_KEY, "20")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "20")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(30)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, numMessages));
    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(artifactClient.listArtifacts(name, EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  @Test
  public void testBacklog10gbUsingStreamingEngine()
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    // Create topic and subscriptions
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-streaming-engine-input");
    SubscriptionName streamingEngineSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "streaming-engine-subscription");
    // Generate fake data to topic
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(numMessages))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("150")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, streamingEngineSubscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath())
            .addParameter(NUM_SHARDS_KEY, "20")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "20")
            .addParameter("enableStreamingEngine", "true")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(30)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, numMessages));
    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(artifactClient.listArtifacts(name, EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  @Test
  public void testSteadyState1hr() throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName topic = pubsubResourceManager.createTopic(testName.getMethodName() + "input");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("100000")
            .setTopic(topic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath())
            .addParameter(NUM_SHARDS_KEY, "20")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "10")
            .addParameter("enableStreamingEngine", "true")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    dataGenerator.execute(Duration.ofMinutes(60));
    Result result = pipelineOperator.drainJobAndFinish(createConfig(info, Duration.ofMinutes(20)));

    // Assert
    assertThat(result).isEqualTo(Result.LAUNCH_FINISHED);
    assertThat(artifactClient.listArtifacts(name, EXPECTED_PATTERN)).isNotEmpty();
    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  private Map<String, Double> computeMetrics(LaunchInfo info)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION);
    metrics.putAll(getCpuUtilizationMetrics(info));
    metrics.putAll(getDataFreshnessMetrics(info));
    metrics.putAll(getSystemLatencyMetrics(info));
    Map<String, Double> outputThroughput =
        getThroughputMetricsOfPtransform(info, OUTPUT_PTRANSFORM);
    Map<String, Double> inputThroughput =
        getThroughputMetricsOfPcollection(info, INPUT_PCOLLECTION);
    metrics.put("AvgInputThroughput", inputThroughput.get("AvgThroughput"));
    metrics.put("MaxInputThroughput", inputThroughput.get("MaxThroughput"));
    metrics.put("AvgOutputThroughput", outputThroughput.get("AvgThroughput"));
    metrics.put("MaxOutputThroughput", outputThroughput.get("MaxThroughput"));
    return metrics;
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(
        ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName.getMethodName());
  }
}
