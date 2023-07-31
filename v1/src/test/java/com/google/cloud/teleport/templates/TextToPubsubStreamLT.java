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

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.ArtifactClient;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performance tests for {@link TextToPubsubStreamLT GCS Text to PubSub} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(TextToPubsubStream.class)
@RunWith(JUnit4.class)
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
  private static final long NUM_MESSAGES_FOR_10GB = 35_000_000L;
  private static final long TIMEOUT_FOR_10_GB_TEST_MINUTES = 30;

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient gcsClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();

    gcsClient =
        GcsResourceManager.builder(ARTIFACT_BUCKET, TEST_ROOT_DIR)
            .setCredentials(CREDENTIALS)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, gcsClient);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::disableRunnerV2);
  }

  @Ignore("RunnerV2 is disabled on streaming templates.")
  @Test
  public void testSteadyState1hrUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableStreamingEngine);
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
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 7)
                    .addParameter("outputTopic", outputTopic.toString())
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*"))
            .build();
    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    // Waits until a number of messages condition is met
    PipelineOperator.Result result =
        // The method waitForConditionAndCancel was used because the streaming pipeline template
        // includes a call to Splittable DoFn. Invoking a splittable DoFn causes the job to remain
        // in the Draining state indefinitely.
        // @see <a
        // href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#important_information_about_draining_a_job">
        // Important information about draining a job</a>
        pipelineOperator.waitForConditionAndCancel(
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
            .setMaxNumWorkers("15")
            .build();

    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 5)
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
        // The method waitForConditionAndCancel was used because the streaming pipeline template
        // includes a call to Splittable DoFn. Invoking a splittable DoFn causes the job to remain
        // in the Draining state indefinitely.
        // @see <a
        // href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#important_information_about_draining_a_job">
        // Important information about draining a job</a>
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(20)),
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
