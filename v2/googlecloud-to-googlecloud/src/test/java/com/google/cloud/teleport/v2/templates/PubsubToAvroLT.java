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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils;
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
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link PubsubToAvro} PubSub to GCS Avro template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(PubsubToAvro.class)
@RunWith(JUnit4.class)
public class PubsubToAvroLT extends TemplateLoadTestBase {

  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Cloud_PubSub_to_Avro_Flex");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = PubsubToAvroLT.class.getSimpleName();
  private static final String AVRO_TEMP_DIR = "avro_tmp";
  private static final String INPUT_PCOLLECTION = "Read PubSub Events/MapElements/Map.out0";
  private static final String OUTPUT_PCOLLECTION = "5m Window/Window.Assign.out0";
  private static final String AVRO_OUTPUT_FILENAME_PREFIX = "topic-output-";
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final long NUM_MESSAGES_FOR_10_GB_TEST = 35000000L;
  private static final long TIMEOUT_FOR_10_GB_TEST_IN_MINUTES = 60;
  private static final long TIMEOUT_FOR_1_HOUR_TEST_MINUTES = 60;
  private static final Pattern EXPECTED_PATTERN =
      Pattern.compile(".*" + AVRO_OUTPUT_FILENAME_PREFIX + ".*");

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
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(
        Function.identity(), NUM_MESSAGES_FOR_10_GB_TEST, TIMEOUT_FOR_10_GB_TEST_IN_MINUTES);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(Function.identity());
  }

  private void testBacklog(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder,
      long numMessages,
      long timeoutMin)
      throws IOException, InterruptedException, ParseException {
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(numMessages))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();

    String outputDirectoryPath =
        ArtifactUtils.getFullGcsPath(
            ARTIFACT_BUCKET, getClass().getSimpleName(), gcsClient.runId(), testName);
    String tempDirectoryPath =
        ArtifactUtils.getFullGcsPath(
            ARTIFACT_BUCKET, getClass().getSimpleName(), gcsClient.runId(), AVRO_TEMP_DIR);

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputDirectory", outputDirectoryPath)
                    .addParameter("avroTempDirectory", tempDirectoryPath)
                    .addParameter("outputFilenamePrefix", AVRO_OUTPUT_FILENAME_PREFIX))
            .build();

    // Act
    dataGenerator.execute(Duration.ofMinutes(timeoutMin));
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(timeoutMin)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, numMessages));

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(gcsClient.listArtifacts(testName + "/", EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private void testSteadyState1hr(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Creates a topic on Pub/Sub.
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("100000")
            .setTopic(backlogTopic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();

    String outputDirectoryPath =
        ArtifactUtils.getFullGcsPath(
            ARTIFACT_BUCKET, getClass().getSimpleName(), gcsClient.runId(), testName);
    String tempDirectoryPath =
        ArtifactUtils.getFullGcsPath(
            ARTIFACT_BUCKET, getClass().getSimpleName(), gcsClient.runId(), AVRO_TEMP_DIR);

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("numShards", "20")
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputDirectory", outputDirectoryPath)
                    .addParameter("avroTempDirectory", tempDirectoryPath)
                    .addParameter("outputFilenamePrefix", AVRO_OUTPUT_FILENAME_PREFIX))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    // Executes the data generator and return approximate number of messages
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    long expectedMessages =
        (long) (dataGenerator.execute(Duration.ofMinutes(TIMEOUT_FOR_1_HOUR_TEST_MINUTES)) * 0.99);

    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(TIMEOUT_FOR_1_HOUR_TEST_MINUTES)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, expectedMessages));

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(gcsClient.listArtifacts(testName + "/", EXPECTED_PATTERN)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }
}
