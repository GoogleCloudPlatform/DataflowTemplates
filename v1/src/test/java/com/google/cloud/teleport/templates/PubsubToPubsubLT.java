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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
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
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
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
        DefaultPubsubResourceManager.builder(testName, project)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(Function.identity());
  }

  @Ignore
  @Test
  public void testBacklog10gbUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(Function.identity());
  }

  @Ignore
  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(config -> config.addEnvironment("enableStreamingEngine", true));
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
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
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputTopic", outputTopic.toString()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(60)),
            () -> {
              Long currentMessages =
                  monitoringClient.getNumMessagesInSubscription(
                      project, outputSubscription.getSubscription());
              LOG.info(
                  "Found {} messages in output subscription, expected {} messages.",
                  currentMessages,
                  NUM_MESSAGES);
              return currentMessages >= NUM_MESSAGES;
            });

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  public void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
    TopicName outputTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "output-subscription");
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("100000")
            .setTopic(inputTopic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 100)
                    .addParameter("inputSubscription", backlogSubscription.toString())
                    .addParameter("outputTopic", outputTopic.toString()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    Integer expectedMessages = (int) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(60)),
            () -> {
              Long currentMessages =
                  monitoringClient.getNumMessagesInSubscription(
                      project, outputSubscription.getSubscription());
              LOG.info(
                  "Found {} messages in output subscription, expected {} messages.",
                  currentMessages,
                  expectedMessages);
              return currentMessages >= expectedMessages;
            });

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }
}
