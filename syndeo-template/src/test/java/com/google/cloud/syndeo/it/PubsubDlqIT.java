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
package com.google.cloud.syndeo.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class PubsubDlqIT {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubDlqIT.class);

  private static final List<ResourceManager> RESOURCE_MANAGERS = new ArrayList<>();

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String TEST_ID = "pubsub-dlq-it-" + UUID.randomUUID();
  private static final String SPEC_PATH = "gs://dippatel-syndeo/syndeo-template.json";
  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);

  private static final Integer MESSAGE_COUNT = 10;
  private static final Long ONE_MINUTE_MILLIS = 60 * 1000L;

  @Rule public final TestName testName = new TestName();

  @Rule public final TestPipeline mainPipeline = TestPipeline.create();

  private TopicName pubsubSourceTopic = null;
  private SubscriptionName pubsubSourceSubscription = null;
  private TopicName pubsubSinkTopic = null;
  private SubscriptionName pubsubSinkSubscription = null;
  private TopicName pubsubDlqTopic = null;
  private SubscriptionName pubsubDlqSubscription = null;
  private PubsubResourceManager pubsubResourceManager = null;

  @Before
  public void setUpPubSub() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(TEST_ID, PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    RESOURCE_MANAGERS.add(pubsubResourceManager);

    pubsubSourceTopic =
        pubsubResourceManager.createTopic("source-" + TEST_ID + testName.getMethodName());
    LOG.info("Successfully created topic {}", pubsubSourceTopic);
    pubsubSourceSubscription =
        pubsubResourceManager.createSubscription(
            pubsubSourceTopic, "sub-" + pubsubSourceTopic.getTopic());
    LOG.info("Successfully created subscription {}", pubsubSourceSubscription);

    pubsubSinkTopic =
        pubsubResourceManager.createTopic("sink-" + TEST_ID + testName.getMethodName());
    LOG.info("Successfully created topic {}", pubsubSinkTopic);
    pubsubSinkSubscription =
        pubsubResourceManager.createSubscription(
            pubsubSinkTopic, "sub-" + pubsubSinkTopic.getTopic());
    LOG.info("Successfully created subscription {}", pubsubSinkSubscription);

    pubsubDlqTopic = pubsubResourceManager.createTopic("dlq-" + TEST_ID + testName.getMethodName());
    LOG.info("Successfully created topic {}", pubsubDlqTopic);
    pubsubDlqSubscription =
        pubsubResourceManager.createSubscription(
            pubsubDlqTopic, "sub-" + pubsubDlqTopic.getTopic());
    LOG.info("Successfully created subscription {}", pubsubDlqSubscription);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(RESOURCE_MANAGERS.toArray(new ResourceManager[0]));
    pubsubSourceTopic = null;
    pubsubSinkTopic = null;
    pubsubDlqTopic = null;
    pubsubResourceManager = null;
  }

  @Test
  public void testOnlyWriteDataToPubsub() throws Exception {
    // Make sure that Kafka Server exists
    // Start data generation pipeline
    publishCorrectDataToPubsub();
    PullResponse response = pubsubResourceManager.pull(pubsubSourceSubscription, 100);
    if (response.getReceivedMessagesCount() != MESSAGE_COUNT) {
      throw new RuntimeException(
          String.format(
              "Expected 10 messages but found %d. Cancelling test",
              response.getReceivedMessagesCount()));
    }
  }

  @Test
  public void testPubsubToPubsubSuccessful() throws Exception {
    // Start Syndeo pipeline
    FlexTemplateClient templateClient =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    PipelineOperator operator = new PipelineOperator(templateClient);

    PipelineLauncher.LaunchInfo syndeoPipeline = kickstartSyndeoPipeline();

    // Publish correct messages to the source queue
    publishCorrectDataToPubsub();

    // Wait for the Syndeo pipeline to move all the data.
    Thread.sleep(3 * ONE_MINUTE_MILLIS);

    // Wait for a while to check the reults in the pubsub sink
    {
      Instant start = Instant.now();
      while (true) {
        PullResponse response = pubsubResourceManager.pull(pubsubSinkSubscription, 100);
        if (response.getReceivedMessagesCount() == MESSAGE_COUNT) {
          // Test sucessful
          break;
        } else if (response.getReceivedMessagesCount() > MESSAGE_COUNT) {
          throw new AssertionError(
              String.format(
                  "Expected at most %s elements to be inserted to Pubsub sink, but found %s.",
                  MESSAGE_COUNT, response.getReceivedMessagesCount()));
        } else if (java.time.Duration.between(start, Instant.now())
                .compareTo(java.time.Duration.ofMinutes(20))
            > 0) {
          // If we spent over 20 minutes waiting for the job, then we must exit
          throw new AssertionError(
              String.format(
                  "Expected %s elements to be inserted to Pubsub sink, but found %s after over 20 minutes.",
                  MESSAGE_COUNT, response.getReceivedMessagesCount()));
        } else {
          // Iterate once more
          Thread.sleep(ONE_MINUTE_MILLIS);
          continue;
        }
      }
    }

    // Wait five minutes while the pipeline drains
    operator.drainJobAndFinish(
        PipelineOperator.Config.builder()
            .setProject(PROJECT)
            .setRegion(REGION)
            .setJobId(syndeoPipeline.jobId())
            .setTimeoutAfter(java.time.Duration.ofMinutes(5))
            .build());
  }

  @Test
  public void testPubsubToPubsubFailure() throws Exception {
    // Start Syndeo pipeline
    FlexTemplateClient templateClient =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    PipelineOperator operator = new PipelineOperator(templateClient);

    PipelineLauncher.LaunchInfo syndeoPipeline = kickstartSyndeoPipeline();

    // Publish incorrect messages to the source queue
    publishIncorrectDataToPubsub();

    // Wait for the Syndeo pipeline to move all the data.
    Thread.sleep(3 * ONE_MINUTE_MILLIS);

    // Wait for a while to check the reults in the pubsub sink
    {
      Instant start = Instant.now();
      while (true) {
        PullResponse response = pubsubResourceManager.pull(pubsubDlqSubscription, 100);
        if (response.getReceivedMessagesCount() == MESSAGE_COUNT) {
          // Test sucessful
          break;
        } else if (response.getReceivedMessagesCount() > MESSAGE_COUNT) {
          throw new AssertionError(
              String.format(
                  "Expected at most %s elements to be inserted to Pubsub sink, but found %s.",
                  MESSAGE_COUNT, response.getReceivedMessagesCount()));
        } else if (java.time.Duration.between(start, Instant.now())
                .compareTo(java.time.Duration.ofMinutes(20))
            > 0) {
          // If we spent over 20 minutes waiting for the job, then we must exit
          throw new AssertionError(
              String.format(
                  "Expected %s elements to be inserted to Pubsub sink, but found %s after over 20 minutes.",
                  MESSAGE_COUNT, response.getReceivedMessagesCount()));
        } else {
          // Iterate once more
          Thread.sleep(ONE_MINUTE_MILLIS);
          continue;
        }
      }
    }

    // Wait five minutes while the pipeline drains
    operator.drainJobAndFinish(
        PipelineOperator.Config.builder()
            .setProject(PROJECT)
            .setRegion(REGION)
            .setJobId(syndeoPipeline.jobId())
            .setTimeoutAfter(java.time.Duration.ofMinutes(5))
            .build());
  }

  PipelineLauncher.LaunchInfo kickstartSyndeoPipeline() throws Exception {
    JsonNode templateConfiguration =
        generateBaseRootConfiguration(
            generateFullTopicName(pubsubSourceTopic),
            generateFullTopicName(pubsubSinkTopic),
            generateFullTopicName(pubsubDlqTopic));

    String jobName = "syndeo-job-" + UUID.randomUUID();

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("jsonSpecPayload", templateConfiguration.toString())
            .build();

    PipelineLauncher.LaunchInfo actual =
        FlexTemplateClient.builder()
            .setCredentials(CREDENTIALS)
            .build()
            .launch(PROJECT, REGION, options);

    return actual;
  }

  public static JsonNode generateBaseRootConfiguration(
      String sourceTopic, String sinkTopic, String dlqTopic) {
    JsonNode rootConfiguration = JsonNodeFactory.instance.objectNode();

    JsonNode pubsubSourceNode = ((ObjectNode) rootConfiguration).putObject("source");
    ((ObjectNode) pubsubSourceNode)
        .put("urn", "syndeo:schematransform:com.google.cloud:pubsub_read:v1");
    JsonNode sourceConfigParams =
        ((ObjectNode) pubsubSourceNode).putObject("configurationParameters");

    ((ObjectNode) sourceConfigParams).put("topic", sourceTopic);
    ((ObjectNode) sourceConfigParams).put("format", "JSON");
    ((ObjectNode) sourceConfigParams).put("schema", generateSchema().toString());

    JsonNode pubsubSinkNode = ((ObjectNode) rootConfiguration).putObject("sink");
    ((ObjectNode) pubsubSinkNode)
        .put("urn", "syndeo:schematransform:com.google.cloud:pubsub_write:v1");
    JsonNode sinkConfigParams = ((ObjectNode) pubsubSinkNode).putObject("configurationParameters");

    ((ObjectNode) sinkConfigParams).put("topic", sinkTopic);
    ((ObjectNode) sinkConfigParams).put("format", "JSON");

    JsonNode pubsubDlqNode = ((ObjectNode) rootConfiguration).putObject("dlq");
    ((ObjectNode) pubsubDlqNode)
        .put("urn", "syndeo:schematransform:com.google.cloud:pubsub_dlq_write:v1");
    JsonNode dlqConfigParams = ((ObjectNode) pubsubDlqNode).putObject("configurationParameters");

    ((ObjectNode) dlqConfigParams).put("topic", dlqTopic);

    return rootConfiguration;
  }

  private static JsonNode generateSchema() {
    JsonNode rootConfiguration = JsonNodeFactory.instance.objectNode();
    ((ObjectNode) rootConfiguration).put("$id", "https://example.com/person.schema.json");
    ((ObjectNode) rootConfiguration).put("$schema", "https://json-schema.org/draft-07/schema");
    ((ObjectNode) rootConfiguration).put("title", "pubsub data schema");
    ((ObjectNode) rootConfiguration).put("type", "object");
    JsonNode properties = ((ObjectNode) rootConfiguration).putObject("properties");
    JsonNode name = ((ObjectNode) properties).putObject("name");
    ((ObjectNode) name).put("type", "string");
    return rootConfiguration;
  }

  private static String generateFullTopicName(TopicName topic) {
    return String.format("projects/%1$s/topics/%2$s", topic.getProject(), topic.getTopic());
  }

  private void publishCorrectDataToPubsub() {
    // Publish ten messages with the correct schema
    for (int i = 0; i < MESSAGE_COUNT; i++) {
      pubsubResourceManager.publish(
          pubsubSourceTopic,
          Map.of(),
          ByteString.copyFromUtf8(String.format("{\"name\":\"%d\"}", i)));
    }
  }

  private void publishIncorrectDataToPubsub() {
    // Publish ten messages with the incorrect schema
    for (int i = 0; i < MESSAGE_COUNT; i++) {
      pubsubResourceManager.publish(
          pubsubSourceTopic, Map.of(), ByteString.copyFromUtf8(String.format("{failure}")));
    }
  }
}
