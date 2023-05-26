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

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
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
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();

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
  public void testPubsubToJms()
      throws IOException, ExecutionException, InterruptedException, JMSException {
    pubsubToJms(Function.identity()); // no extra parameters
  }

  public void pubsubToJms(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, JMSException {
    // Arrange
    String outputName = testName;
    TopicName tc = pubsubResourceManager.createTopic(testName);
    String outTopicName = tc.getTopic();
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(tc, "sub-" + testName);
    String connectionUri =
        "tcp://" + TestProperties.hostIp() + ":" + activeMqContainer.getFirstMappedPort();
    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
    Connection connection = factory.createConnection();
    connection.start();

    LaunchConfig.Builder optionsQueue =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscriptionName.toString())
                .addParameter("jmsServer", connectionUri)
                .addParameter("outputName", outputName)
                .addParameter("outputType", "queue")
                .addParameter("username", "")
                .addParameter("password", ""));

    // Act
    LaunchInfo infoQueue = launchTemplate(optionsQueue);
    LOG.info("Triggered template job for JMS Queue");

    assertThatPipeline(infoQueue).isRunning();

    String inMessage = "firstMessage";

    Session sessionQueue = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
                    ;

                  } catch (Exception e) {
                    System.out.println(e.getMessage());
                  }
                  System.out.println(outMessage);
                  return outMessage.equalsIgnoreCase(inMessage);
                });

    assertThatResult(resultQueue).meetsConditions();

    // Assert

    LaunchConfig.Builder optionsTopic =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscriptionName.toString())
                .addParameter("jmsServer", connectionUri)
                .addParameter("outputName", outputName)
                .addParameter("outputType", "topic")
                .addParameter("username", "")
                .addParameter("password", ""));
    LaunchInfo infoTopic = launchTemplate(optionsTopic);
    LOG.info("Triggered template job for JMS Topic");

    assertThatPipeline(infoTopic).isRunning();

    Session sessionTopic = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
                    ;

                  } catch (Exception e) {
                    System.out.println(e.getMessage());
                  }
                  System.out.println(outMessage);
                  return outMessage.equalsIgnoreCase(inMessage);
                });

    // Assert
    assertThatResult(resultTopic).meetsConditions();
  }
}
