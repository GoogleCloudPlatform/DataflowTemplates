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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testSubscriptionToTopic() throws IOException {
    // Arrange
    String jobName = createJobName(testName.getMethodName());
    TopicName inputTopic = pubsubResourceManager.createTopic("input-topic");
    TopicName outputTopic = pubsubResourceManager.createTopic("output-topic");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-subscription");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");

    List<String> expectedMessages =
        List.of("message1-" + jobName, "message2-" + jobName, "message3-" + jobName);
    publishMessages(inputTopic, expectedMessages);
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("inputSubscription", inputSubscription.toString())
            .addParameter("outputTopic", outputTopic.toString());

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<PullResponse> records = new AtomicReference<>();
    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  PullResponse response =
                      pubsubResourceManager.pull(outputSubscription, expectedMessages.size());
                  records.set(response);
                  return response.getReceivedMessagesList().size() >= expectedMessages.size();
                });

    // Assert
    List<String> actualMessages =
        records.get().getReceivedMessagesList().stream()
            .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
            .collect(Collectors.toList());
    assertThat(result).isEqualTo(Result.CONDITION_MET);
    assertThat(actualMessages).containsAtLeastElementsIn(expectedMessages);
  }

  private void publishMessages(TopicName topicName, List<String> messages) {
    for (String message : messages) {
      ByteString messageData = ByteString.copyFromUtf8(message);
      pubsubResourceManager.publish(topicName, ImmutableMap.of(), messageData);
    }
  }
}
