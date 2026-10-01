/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubSubscriptionToPubSubTopicYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubSubscriptionToPubSubTopicYaml.class)
@RunWith(JUnit4.class)
public final class PubSubSubscriptionToPubSubTopicYamlIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PubSubSubscriptionToPubSubTopicYamlIT.class);

  private PubsubResourceManager pubsubResourceManager;

  private static final int MESSAGES_COUNT = 10;

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
  public void testPubSubSubscriptionToPubSubTopic() throws IOException {
    pubSubSubscriptionToPubSubTopic(Function.identity());
  }

  public void pubSubSubscriptionToPubSubTopic(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubSubscriptionToPubSubTopic test.");

    String nameSuffix = RandomStringUtils.randomAlphanumeric(8);
    TopicName inputTopic = pubsubResourceManager.createTopic("input-" + nameSuffix);
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-sub-" + nameSuffix);
    TopicName outputTopic = pubsubResourceManager.createTopic("output-" + nameSuffix);
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-sub-" + nameSuffix);

    String schema =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"job\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}}}";

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("subscription", inputSubscription.toString())
                .addParameter("format", "JSON")
                .addParameter("schema", schema)
                .addParameter("outputTopic", outputTopic.toString())
                .addParameter("outputFormat", "JSON"));

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<String> expectedMessages = new ArrayList<>();
    List<ByteString> messageDataList = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      String messageJson =
          new JSONObject(Map.of("id", i, "job", testName, "name", "message")).toString();
      messageDataList.add(ByteString.copyFromUtf8(messageJson));
      expectedMessages.add(messageJson);
    }

    Publisher publisher = null;
    try {
      publisher =
          Publisher.newBuilder(inputTopic).setCredentialsProvider(credentialsProvider).build();
      final Publisher finalPublisher = publisher;

      PubsubMessagesCheck pubsubCheck =
          PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
              .setMinMessages(MESSAGES_COUNT)
              .build();

      PipelineOperator.Result result =
          pipelineOperator()
              .waitForConditionsAndFinish(
                  createConfig(info),
                  () -> {
                    LOG.info("Publishing messages to input topic...");
                    List<ApiFuture<String>> futures = new ArrayList<>();
                    for (ByteString data : messageDataList) {
                      futures.add(
                          finalPublisher.publish(PubsubMessage.newBuilder().setData(data).build()));
                    }
                    try {
                      ApiFutures.allAsList(futures).get();
                      Thread.sleep(2000);
                    } catch (Exception e) {
                      throw new RuntimeException("Error publishing messages", e);
                    }
                    return true;
                  },
                  pubsubCheck);

      assertThatResult(result).meetsConditions();

      List<String> actualMessages =
          pubsubCheck.getReceivedMessageList().stream()
              .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
              .collect(Collectors.toList());
      assertThat(actualMessages).isNotEmpty();
    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }
  }
}
