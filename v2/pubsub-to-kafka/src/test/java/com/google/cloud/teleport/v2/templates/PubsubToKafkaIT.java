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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.kafka.DefaultKafkaResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToKafka}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubToKafka.class)
@RunWith(JUnit4.class)
public final class PubsubToKafkaIT extends TemplateTestBase {

  @Rule public final TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToKafka.class);

  private DefaultKafkaResourceManager kafkaResourceManager;

  private DefaultPubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {

    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();

    kafkaResourceManager =
        DefaultKafkaResourceManager.builder(testName.getMethodName()).setHost(HOST_IP).build();
  }

  @After
  public void tearDownClass() {
    boolean producedError = false;

    try {
      pubsubResourceManager.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete Pubsub resources.", e);
      producedError = true;
    }

    try {
      // kafkaResourceManager.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete Kafka resources.", e);
      producedError = true;
    }

    if (producedError) {
      throw new IllegalStateException("Failed to delete resources. Check above for errors.");
    }
  }

  @Test
  public void testPubsubToKafka() throws IOException, ExecutionException, InterruptedException {
    pubsubToKafka(Function.identity()); // no extra parameters
  }

  public void pubsubToKafka(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ExecutionException, InterruptedException {
    // Arrange
    TopicName tc = pubsubResourceManager.createTopic(testName.getMethodName());
    String inTopicName = tc.getTopic();

    String outTopicName = kafkaResourceManager.createTopic(testName.getMethodName(), 5);

    String outDeadLetterTopicName =
        pubsubResourceManager.createTopic("outDead" + testName.getMethodName()).getTopic();

    KafkaConsumer<String, String> consumer =
        kafkaResourceManager.buildConsumer(new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(Arrays.asList(outTopicName));
    LOG.info("Created Kafka Consumer");

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "bootstrapServer",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                .addParameter("inputTopic", "projects/" + PROJECT + "/topics/" + inTopicName)
                .addParameter("outputTopic", outTopicName)
                .addParameter(
                    "outputDeadLetterTopic",
                    "projects/" + PROJECT + "/topics/" + outDeadLetterTopicName));

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered Dataflow job");

    assertThatPipeline(info).isRunning();

    List<String> inMessages = Arrays.asList("first message", "second message");
    Publisher publisher = null;
    for (final String message : inMessages) {
      ByteString data = ByteString.copyFromUtf8(message);
      pubsubResourceManager.publish(tc, ImmutableMap.of(), data);
    }
    LOG.info("Published messages to Pubsub Topic");

    List<String> outMessages = new ArrayList<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  ConsumerRecords<String, String> outMessage =
                      consumer.poll(Duration.ofMillis(100));
                  for (ConsumerRecord<String, String> message : outMessage) {
                    outMessages.add(message.value());
                  }
                  return outMessages.size() >= 2;
                });

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(outMessages).isEqualTo(inMessages);
  }
}
