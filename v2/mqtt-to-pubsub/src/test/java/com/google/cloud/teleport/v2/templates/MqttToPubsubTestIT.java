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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.utility.DockerImageName;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(MqttToPubsub.class)
@RunWith(JUnit4.class)
public class MqttToPubsubTestIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MqttToPubsubTestIT.class);

  private HiveMQContainer hiveMQContainer;
  private PubsubResourceManager pubsubClient;
  private Mqtt5BlockingClient mqttClient;

  @Before
  public void setup() throws IOException {
    hiveMQContainer =
        new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce").withTag("2021.3"));
    pubsubClient =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    hiveMQContainer.start();
    mqttClient =
        Mqtt5Client.builder()
            .serverPort(hiveMQContainer.getMqttPort())
            .serverHost(hiveMQContainer.getHost())
            .buildBlocking();
    mqttClient.connect();
  }

  @After
  public void tearDownClass() {
    boolean producedError = false;

    try {
      pubsubClient.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete PubSub resources.", e);
      producedError = true;
    }

    try {
      hiveMQContainer.stop();
    } catch (Exception e) {
      LOG.error("Failed to delete MQTT Container resources.", e);
      producedError = true;
    }

    if (producedError) {
      throw new IllegalStateException("Failed to delete resources. Check above for errors.");
    }
  }

  @Test
  public void testMqttToPubSub() throws IOException {
    // Arrange

    String jobName = testName;
    String inputTopic = testName + "input";
    String psTopic = testName + "output";
    TopicName topicName = pubsubClient.createTopic(psTopic);
    SubscriptionName subscriptionName = pubsubClient.createSubscription(topicName, "subscription");
    mqttClient
        .publishWith()
        .topic(inputTopic)
        .qos(MqttQos.AT_LEAST_ONCE)
        .payload("1".getBytes())
        .send();

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("brokerServer", "tcp://" + HOST_IP + ":" + hiveMQContainer.getMqttPort())
            .addParameter("inputTopic", inputTopic)
            .addParameter("outputTopic", topicName.toString())
            .addParameter("username", "")
            .addParameter("password", "");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                () -> {
                  mqttClient
                      .publishWith()
                      .topic(inputTopic)
                      .qos(MqttQos.AT_LEAST_ONCE)
                      .payload("1".getBytes())
                      .send();
                  return pubsubClient
                      .pull(subscriptionName, 1)
                      .getReceivedMessages(0)
                      .getMessage()
                      .getData()
                      .toString(StandardCharsets.UTF_8)
                      .equalsIgnoreCase("1");
                });

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
