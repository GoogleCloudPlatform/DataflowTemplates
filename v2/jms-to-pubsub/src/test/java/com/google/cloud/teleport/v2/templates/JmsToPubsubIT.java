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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Integration test for {@link JmsToPubsub}. */
@TemplateIntegrationTest(JmsToPubsub.class)
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class JmsToPubsubIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(JmsToPubsub.class);

  private PubsubResourceManager pubsubResourceManager;
  private GenericContainer<?> activeMqContainer;

  @Before
  public void setup() throws IOException {

    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    activeMqContainer =
        new GenericContainer<>(DockerImageName.parse("rmohr/activemq:5.14.3"))
            .withExposedPorts(61616);
    activeMqContainer.start();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testJmsToPubsubQueue() throws IOException, JMSException {
    String inputName = testName;
    TopicName tc = pubsubResourceManager.createTopic(testName);
    String outTopicName = tc.getTopic();
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(tc, "subscription");
    String connectionUri =
        "tcp://" + TestProperties.hostIp() + ":" + activeMqContainer.getFirstMappedPort();
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
    Connection connection = factory.createConnection();
    connection.start();

    LaunchConfig.Builder optionsQueue =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jmsServer", connectionUri)
            .addParameter("inputName", inputName)
            .addParameter("inputType", "queue")
            .addParameter("outputTopic", "projects/" + PROJECT + "/topics/" + outTopicName)
            .addParameter("username", "")
            .addParameter("password", "");

    // Act
    LaunchInfo infoQueue = launchTemplate(optionsQueue);
    LOG.info("Triggered template job for JMS Queue");

    assertThatPipeline(infoQueue).isRunning();

    String inMessage = "firstMessage";

    Session sessionQueue = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destQueue = new ActiveMQQueue(inputName);
    MessageProducer producerQueue = sessionQueue.createProducer(destQueue);
    TextMessage messageQueue = sessionQueue.createTextMessage(inMessage);
    producerQueue.send(messageQueue);

    Result resultQueue =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(infoQueue),
                () ->
                    pubsubResourceManager
                        .pull(subscriptionName, 1)
                        .getReceivedMessages(0)
                        .getMessage()
                        .getData()
                        .toString(StandardCharsets.UTF_8)
                        .equalsIgnoreCase(inMessage));

    // Assert
    assertThatResult(resultQueue).meetsConditions();
  }

  @Test
  public void testJmsToPubsubTopic() throws IOException, JMSException {
    String inputName = testName;
    TopicName tc = pubsubResourceManager.createTopic(testName);
    String outTopicName = tc.getTopic();
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(tc, "subscription");
    String connectionUri =
        "tcp://" + TestProperties.hostIp() + ":" + activeMqContainer.getFirstMappedPort();
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
    Connection connection = factory.createConnection();
    connection.start();

    String inMessage = "firstMessage";

    Session sessionQueue = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destQueue = new ActiveMQQueue(inputName);
    MessageProducer producerQueue = sessionQueue.createProducer(destQueue);
    TextMessage messageQueue = sessionQueue.createTextMessage(inMessage);
    producerQueue.send(messageQueue);

    LaunchConfig.Builder optionsTopic =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jmsServer", connectionUri)
            .addParameter("inputName", inputName)
            .addParameter("inputType", "topic")
            .addParameter("outputTopic", "projects/" + PROJECT + "/topics/" + outTopicName)
            .addParameter("username", "")
            .addParameter("password", "");
    LaunchInfo infoTopic = launchTemplate(optionsTopic);
    LOG.info("Triggered template job for JMS Topic");

    assertThatPipeline(infoTopic).isRunning();

    Session sessionTopic = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination destTopic = new ActiveMQTopic(inputName);
    MessageProducer producerTopic = sessionTopic.createProducer(destTopic);
    TextMessage messageTopic = sessionTopic.createTextMessage(inMessage);

    Result resultTopic =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(infoTopic),
                () -> {
                  try {
                    producerTopic.send(messageTopic);
                  } catch (Exception e) {
                    System.out.println(e.getMessage());
                  }
                  return pubsubResourceManager
                      .pull(subscriptionName, 1)
                      .getReceivedMessages(0)
                      .getMessage()
                      .getData()
                      .toString(StandardCharsets.UTF_8)
                      .equalsIgnoreCase(inMessage);
                });

    // Assert
    assertThatResult(resultTopic).meetsConditions();
  }
}
