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
package com.google.cloud.teleport.templates;

import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performance tests for {@link PubsubToPubsub PubSub to PubSub} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(PubsubToPubsub.class)
@RunWith(JUnit4.class)
public class PubsubToPubsubLT extends TemplateLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToPubsubLT.class);
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Cloud_PubSub_to_Cloud_PubSub");
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final long NUM_MESSAGES = 35000000L;
  private static final String INPUT_PCOLLECTION = "Read PubSub Events/PubsubUnboundedSource.out0";
  private static final String OUTPUT_PCOLLECTION = "Write PubSub Events/MapElements/Map.out0";
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project, CREDENTIALS_PROVIDER).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableStreamingEngine);
  }

  @Ignore("RunnerV2 is disabled on streaming templates.")
  @Test
  public void testSteadyState1hrUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingAtLeastOnceMode()
      throws ParseException, IOException, InterruptedException {
    ArrayList<String> experiments = new ArrayList<>();
    experiments.add("streaming_mode_at_least_once");
    testSteadyState1hr(
        b ->
            b.addEnvironment("additionalExperiments", experiments)
                .addEnvironment("enableStreamingEngine", true));
  }

  public void testBacklog(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    TopicName outputTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");
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
                    .addEnvironment("maxWorkers", 5)
                    .addEnvironment("numWorkers", 4)
                    .addEnvironment("machineType", "e2-standard-2")
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputTopic", outputTopic.toString()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(60)),
            () -> {
              Long currentMessages =
                  monitoringClient.getNumMessagesInSubscription(
                      project, outputSubscription.getSubscription());
              LOG.info(
                  "Found {} messages in output subscription, expected {} messages.",
                  currentMessages,
                  NUM_MESSAGES);
              return currentMessages != null && currentMessages >= NUM_MESSAGES;
            });

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  public void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    String qps = getProperty("qps", "100000", TestProperties.Type.PROPERTY);
    TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
    TopicName outputTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "output-subscription");
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS(qps)
            .setTopic(inputTopic.toString())
            .setNumWorkers("8")
            .setMaxNumWorkers("10")
            .build();
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 6)
                    .addEnvironment("machineType", "e2-standard-2")
                    .addEnvironment("additionalUserLabels", Collections.singletonMap("qps", qps))
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputTopic", outputTopic.toString()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    Long expectedMessages = (long) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    Result result =
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
}
