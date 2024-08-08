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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.awaitility.Awaitility.await;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextToPubsub} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextToPubsub.class)
@RunWith(JUnit4.class)
public class TextToPubsubIT extends TemplateTestBase {

  private static final String TEST_ROOT_DIR = TextToPubsubIT.class.getSimpleName();
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testTextToTopic() throws IOException {
    testTextToTopicBase(Function.identity());
  }

  @Test
  public void testTextToTopicStreamingEngine() throws IOException {
    testTextToTopicBase(this::enableStreamingEngine);
  }

  private void testTextToTopicBase(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    Map<String, String> artifacts =
        Map.of(
            "message1-" + testName,
            RandomStringUtils.randomAlphabetic(1, 2),
            "message2-" + testName,
            RandomStringUtils.randomAlphabetic(1, 2),
            "message3-" + testName,
            RandomStringUtils.randomAlphabetic(200, 400),
            "message4-" + testName + "-long-" + RandomStringUtils.randomAlphabetic(100, 200),
            RandomStringUtils.randomAlphabetic(100, 200));
    createArtifacts(artifacts);

    TopicName outputTopic = pubsubResourceManager.createTopic("topic");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");
    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputFilePattern", getInputFilePattern())
                .addParameter("outputTopic", outputTopic.toString()));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(artifacts.size())
            .build();
    Result result = pipelineOperator().waitForCondition(createConfig(info), pubsubCheck);

    // Make sure that the check finds the expected of messages.
    assertThatResult(result).isAnyOf(Result.CONDITION_MET, Result.LAUNCH_FINISHED);

    // Poll checker, to avoid timing issues on DirectRunner
    await("Check if messages got to Pub/Sub on time")
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofSeconds(1))
        .until(pubsubCheck::get);

    assertThat(
            pubsubCheck.getReceivedMessageList().stream()
                .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
                .collect(Collectors.toList()))
        .containsAtLeastElementsIn(artifacts.values());
  }

  private void createArtifacts(Map<String, String> artifacts) {
    for (Map.Entry<String, String> artifact : artifacts.entrySet()) {
      gcsClient.createArtifact(artifact.getKey(), artifact.getValue());
    }
  }

  private String getInputFilePattern() {
    return getFullGcsPath(artifactBucketName, TEST_ROOT_DIR, gcsClient.runId(), "*");
  }
}
