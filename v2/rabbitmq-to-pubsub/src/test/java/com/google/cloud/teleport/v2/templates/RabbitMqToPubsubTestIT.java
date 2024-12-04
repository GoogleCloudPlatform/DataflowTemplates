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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

//
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(RabbitMqToPubsub.class)
@RunWith(JUnit4.class)
public class RabbitMqToPubsubTestIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMqToPubsubTestIT.class);

  private RabbitMQContainer rabbitMQContainer;
  private PubsubResourceManager pubsubClient;

  private Channel channel;

  @Before
  public void setup() throws IOException {
    rabbitMQContainer =
        new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine"))
            .withQueue("testQueueOne");
    pubsubClient = PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    rabbitMQContainer.start();

    // 2. Establish Connection
    try {
      // 1. Create Connection Factory
      ConnectionFactory factory = new ConnectionFactory();
      factory.setUri(rabbitMQContainer.getAmqpUrl());
      Connection connection = factory.newConnection();
      // 3. Create Channel
      this.channel = connection.createChannel();
      // 4. Declare the Queue (optional, but good practice)
      this.channel.queueDeclare("testQueueOne", true, false, false, null);
      // 5. Prepare Message
      String message = "Hello World!";
      // 6. Publish Message
      this.channel.basicPublish("", "testQueueOne", null, message.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.out.println(e);
    }
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
      rabbitMQContainer.stop();
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
    String inputQueueName = testName + "queue";
    String psTopic = testName + "output";
    TopicName topicName = pubsubClient.createTopic(psTopic);
    SubscriptionName subscriptionName = pubsubClient.createSubscription(topicName, "subscription");
    String message = "Hello, world 123";
    channel.basicPublish("", "testQueueOne", null, message.getBytes(StandardCharsets.UTF_8));
    System.out.println(" [x] Sent '" + message + "'");

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("connectionUrl", rabbitMQContainer.getAmqpUrl())
            .addParameter("queue", "testQueueOne")
            .addParameter("outputTopic", topicName.toString());

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  try {
                    channel.basicPublish(
                        "", "testQueueOne", null, "abracadabra".getBytes(StandardCharsets.UTF_8));
                  } catch (Exception e) {
                    System.err.println(e);
                  }

                  return pubsubClient
                      .pull(subscriptionName, 1)
                      .getReceivedMessages(0)
                      .getMessage()
                      .getData()
                      .toString(StandardCharsets.UTF_8)
                      .equalsIgnoreCase("abracadabra");
                });

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
