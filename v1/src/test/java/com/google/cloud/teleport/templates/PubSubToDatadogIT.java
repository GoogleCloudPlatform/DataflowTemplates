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
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import com.google.cloud.teleport.it.datadog.DatadogResourceManager;
import com.google.cloud.teleport.it.datadog.conditions.DatadogEventsCheck;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.apache.beam.sdk.io.datadog.DatadogEvent;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.datadogEventToMap;
import static com.google.cloud.teleport.it.datadog.matchers.DatadogAsserts.assertThatDatadogEvents;
import static com.google.cloud.teleport.it.datadog.matchers.DatadogAsserts.datadogEventsToRecords;

/** Integration test for {@link PubSubToDatadog} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubSubToDatadog.class)
@RunWith(JUnit4.class)
public class PubSubToDatadogIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 100;
  private static final int BAD_MESSAGES_COUNT = 50;

  private PubsubResourceManager pubsubResourceManager;
  private DatadogResourceManager datadogResourceManager;

  private TopicName pubSubTopic;
  private TopicName pubSubDlqTopic;
  private SubscriptionName pubSubSubscription;
  private SubscriptionName pubSubDlqSubscription;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    datadogResourceManager = DatadogResourceManager.builder(testName).build();

    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseHost(value) {\n"
            + "  const obj = JSON.parse(value);\n"
            + "  const includePubsubMessage = obj.data && obj.attributes;\n"
            + "  const data = includePubsubMessage ? obj.data : obj;"
            + "  data._metadata.host = data._metadata.host.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    pubSubTopic = pubsubResourceManager.createTopic("input");
    pubSubSubscription = pubsubResourceManager.createSubscription(pubSubTopic, "input-sub-1");
    pubSubDlqTopic = pubsubResourceManager.createTopic("output-dlq");
    pubSubDlqSubscription = pubsubResourceManager.createSubscription(pubSubDlqTopic, "dlq-sub-1");
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, datadogResourceManager);
  }

  private void testPubSubToDatadogMain(
      PipelineLauncher.LaunchConfig.Builder parameters, boolean allDlq) throws IOException {
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            parameters
                .addParameter("inputSubscription", pubSubSubscription.toString())
                .addParameter("url", datadogResourceManager.getHttpEndpoint())
                .addParameter("disableCertificateValidation", "true")
                .addParameter("outputDeadletterTopic", pubSubDlqTopic.toString())
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseHost"));
    assertThatPipeline(info).isRunning();

    List<DatadogEvent> httpEventsSent = new ArrayList<>();

    String source = RandomStringUtils.randomAlphabetic(1, 20);
    String host = RandomStringUtils.randomAlphabetic(1, 20).toUpperCase();
    String sourceType = RandomStringUtils.randomAlphabetic(1, 20);
    long currentTime = System.currentTimeMillis();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      String event = RandomStringUtils.randomAlphabetic(1, 20);
      long usingEpochTime = currentTime + i;
      DatadogEvent.Builder datadogEventBuilder =
          DatadogEvent.newBuilder()
              .withEvent(event)
              .withSource(source)
              .withSourceType(sourceType)
              .withTime(usingEpochTime);
      DatadogEvent datadogEventBeforeUdf = datadogEventBuilder.withHost(host.toLowerCase()).create();
      DatadogEvent datadogEventAfterUdf = datadogEventBuilder.withHost(host.toUpperCase()).create();

      Map<String, Object> datadogMap = datadogEventToMap(datadogEventBeforeUdf);
      datadogMap.put("time", Instant.ofEpochMilli(usingEpochTime));
      String pubSubMessage = new JSONObject(Map.of("_metadata", datadogMap)).toString();
      ByteString messageData = ByteString.copyFromUtf8(pubSubMessage);
      pubsubResourceManager.publish(pubSubTopic, ImmutableMap.of(), messageData);

      httpEventsSent.add(datadogEventAfterUdf);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(pubSubTopic, ImmutableMap.of(), messageData);
    }

    PubsubMessagesCheck dlqCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, pubSubDlqSubscription)
            .setMinMessages(allDlq ? MESSAGES_COUNT + BAD_MESSAGES_COUNT : BAD_MESSAGES_COUNT)
            .build();

    String query = "search source=" + source + " sourcetype=" + sourceType + " host=" + host;
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                DatadogEventsCheck.builder(datadogResourceManager)
                    .setQuery(query)
                    .setMinEvents(allDlq ? 0 : MESSAGES_COUNT)
                    .build(),
                dlqCheck);
    assertThatResult(result).meetsConditions();

    List<DatadogEvent> httpEventsReceived = datadogResourceManager.getEvents(query);

    assertThatDatadogEvents(httpEventsSent)
        .hasRecordsUnordered(datadogEventsToRecords(httpEventsReceived));
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogBase() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("token", datadogResourceManager.getHecToken())
            .addParameter("batchCount", "1");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogWithBatch() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("token", datadogResourceManager.getHecToken())
            .addParameter("batchCount", "20");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogUnBatchDeadLetter() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("token", "invalid-token")
            .addParameter("batchCount", "5");
    testPubSubToDatadogMain(parameters, true);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogIncludePubSub() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("token", datadogResourceManager.getHecToken())
            .addParameter("batchCount", "1")
            .addParameter("includePubsubMessage", "true");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  public void testPubSubToDatadogWithBatchAndParallelism() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("token", datadogResourceManager.getHecToken())
            .addParameter("batchCount", "10")
            .addParameter("parallelism", "5");
    testPubSubToDatadogMain(parameters, false);
  }
}
