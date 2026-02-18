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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
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

/** Integration test for {@link PubsubToJms}. */
@TemplateIntegrationTest(PubsubToJms.class)
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
public final class PubsubToJmsIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToJms.class);

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
  public void testPubsubToJmsQueue() throws IOException, JMSException {
    // Arrange
    String outputName = testName;
    TopicName tc = pubsubResourceManager.createTopic(testName);
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(tc, "sub-" + testName);
    String connectionUri =
        "tcp://" + TestProperties.hostIp() + ":" + activeMqContainer.getFirstMappedPort();

    LaunchConfig.Builder optionsQueue =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", subscriptionName.toString())
            .addParameter("jmsServer", connectionUri)
            .addParameter("outputName", outputName)
            .addParameter("outputType", "queue")
            .addParameter("username", "")
            .addParameter("password", "");

    // Act
    LaunchInfo infoQueue = launchTemplate(optionsQueue);
    LOG.info("Triggered template job for JMS Queue");

    assertThatPipeline(infoQueue).isRunning();

    String inMessage = "firstMessage";

    Session sessionQueue = createSession(connectionUri);
    Destination destQueue = new ActiveMQQueue(outputName);
    MessageConsumer consumerQueue = sessionQueue.createConsumer(destQueue);

    ByteString data = ByteString.copyFromUtf8(inMessage);
    pubsubResourceManager.publish(tc, ImmutableMap.of(), data);

    Result resultQueue =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(infoQueue),
                () -> {
                  String outMessage = "";
                  try {
                    Message message = consumerQueue.receive();
                    outMessage = ((TextMessage) message).getText();
                  } catch (Exception e) {
                    LOG.error("Error receiving message", e);
                  }
                  return outMessage.equalsIgnoreCase(inMessage);
                });

    assertThatResult(resultQueue).meetsConditions();
  }

  private static Session createSession(String connectionUri) throws JMSException {
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
    Connection connection = factory.createConnection();
    connection.start();
    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  @Test
  public void testPubsubToJmsTopic() throws IOException, JMSException {
    // Arrange
    String outputName = testName;
    TopicName tc = pubsubResourceManager.createTopic(testName);
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(tc, "sub-" + testName);
    String connectionUri =
        "tcp://" + TestProperties.hostIp() + ":" + activeMqContainer.getFirstMappedPort();
    String inMessage = "firstMessage";
    ByteString data = ByteString.copyFromUtf8(inMessage);

    LaunchConfig.Builder optionsTopic =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", subscriptionName.toString())
            .addParameter("jmsServer", connectionUri)
            .addParameter("outputName", outputName)
            .addParameter("outputType", "topic")
            .addParameter("username", "")
            .addParameter("password", "");
    LaunchInfo infoTopic = launchTemplate(optionsTopic);
    LOG.info("Triggered template job for JMS Topic");

    assertThatPipeline(infoTopic).isRunning();

    Session sessionQueue = createSession(connectionUri);
    Destination destTopic = new ActiveMQTopic(outputName);
    MessageConsumer consumerTopic = sessionQueue.createConsumer(destTopic);
    pubsubResourceManager.publish(tc, ImmutableMap.of(), data);

    Result resultTopic =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(infoTopic),
                () -> {
                  String outMessage = "";
                  try {
                    Message message = consumerTopic.receive();
                    outMessage = ((TextMessage) message).getText();
                  } catch (Exception e) {
                    LOG.error("Error receiving message", e);
                  }
                  return outMessage.equalsIgnoreCase(inMessage);
                });

    // Assert
    assertThatResult(resultTopic).meetsConditions();
  }
}
