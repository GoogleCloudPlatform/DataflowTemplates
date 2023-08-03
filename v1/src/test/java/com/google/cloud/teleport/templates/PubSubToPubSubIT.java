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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToPubsub} (Cloud_PubSub_to_Cloud_PubSub). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubToPubsub.class)
@RunWith(JUnit4.class)
public class PubSubToPubSubIT extends TemplateTestBase {
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
  public void testSubscriptionToTopic() throws IOException {
    // Arrange
    TopicName inputTopic = pubsubResourceManager.createTopic("input-topic");
    TopicName outputTopic = pubsubResourceManager.createTopic("output-topic");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-subscription");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");

    List<String> expectedMessages =
        List.of("message1-" + testName, "message2-" + testName, "message3-" + testName);
    publishMessages(inputTopic, expectedMessages);
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", inputSubscription.toString())
            .addParameter("outputTopic", outputTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(expectedMessages.size())
            .build();

    Result result = pipelineOperator().waitForConditionAndFinish(createConfig(info), pubsubCheck);

    // Assert
    List<String> actualMessages =
        pubsubCheck.getReceivedMessageList().stream()
            .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
            .collect(Collectors.toList());
    assertThatResult(result).meetsConditions();
    assertThat(actualMessages).containsAtLeastElementsIn(expectedMessages);
  }

  private void publishMessages(TopicName topicName, List<String> messages) {
    for (String message : messages) {
      ByteString messageData = ByteString.copyFromUtf8(message);
      pubsubResourceManager.publish(topicName, ImmutableMap.of(), messageData);
    }
  }
}
