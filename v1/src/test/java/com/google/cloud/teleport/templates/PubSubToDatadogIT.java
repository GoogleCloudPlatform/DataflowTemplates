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

import static com.google.cloud.teleport.it.datadog.matchers.DatadogAsserts.datadogEntriesToRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.datadog.DatadogEvent;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.datadog.DatadogLogEntry;
import com.google.cloud.teleport.it.datadog.DatadogResourceManager;
import com.google.cloud.teleport.it.datadog.conditions.DatadogLogEntriesCheck;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

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
        "function uppercaseHostname(value) {\n"
            + "  const obj = JSON.parse(value);\n"
            + "  const includePubsubMessage = obj.data && obj.attributes;\n"
            + "  const data = includePubsubMessage ? obj.data : obj;"
            + "  data._metadata = {hostname: data.hostname.toUpperCase()};\n"
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
                .addParameter("outputDeadletterTopic", pubSubDlqTopic.toString())
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseHostname"));
    assertThatPipeline(info).isRunning();

    List<DatadogEvent> sentEvents = new ArrayList<>();

    String hostname = RandomStringUtils.randomAlphabetic(1, 20).toUpperCase();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      DatadogEvent sentEvent =
          DatadogEvent.newBuilder()
              .withHostname(hostname.toLowerCase())
              .withMessage(RandomStringUtils.randomAlphabetic(1, 20))
              .build();
      String pubSubMessage = new JSONObject(datadogEventToRecord(sentEvent)).toString();

      DatadogEvent transformedEvent =
          DatadogEvent.newBuilder()
              .withSource("gcp") // done by the conversion from FailSafeElement to DatadogEvent
              .withHostname(hostname.toUpperCase()) // done by the UDF being specified above
              .withMessage(pubSubMessage)
              .build();

      ByteString messageData = ByteString.copyFromUtf8(pubSubMessage);
      pubsubResourceManager.publish(pubSubTopic, ImmutableMap.of(), messageData);

      sentEvents.add(transformedEvent);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(pubSubTopic, ImmutableMap.of(), messageData);
    }

    PubsubMessagesCheck dlqCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, pubSubDlqSubscription)
            .setMinMessages(allDlq ? MESSAGES_COUNT + BAD_MESSAGES_COUNT : BAD_MESSAGES_COUNT)
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                DatadogLogEntriesCheck.builder(datadogResourceManager)
                    .setMinEntries(allDlq ? 0 : MESSAGES_COUNT)
                    .build(),
                dlqCheck);
    assertThatResult(result).meetsConditions();

    List<DatadogLogEntry> receivedEntries = datadogResourceManager.getEntries();

    assertThatRecords(datadogEventsToRecords(sentEvents))
        .hasRecordsUnordered(datadogEntriesToRecords(receivedEntries));
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogBase() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("apiKey", datadogResourceManager.getApiKey())
            .addParameter("batchCount", "1");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogWithBatch() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("apiKey", datadogResourceManager.getApiKey())
            .addParameter("batchCount", "20");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogUnBatchDeadLetter() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("apiKey", "invalid-api-key")
            .addParameter("batchCount", "5");
    testPubSubToDatadogMain(parameters, true);
  }

  @Test
  @Category(DirectRunnerTest.class)
  public void testPubSubToDatadogIncludePubSub() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("apiKey", datadogResourceManager.getApiKey())
            .addParameter("batchCount", "1")
            .addParameter("includePubsubMessage", "true");
    testPubSubToDatadogMain(parameters, false);
  }

  @Test
  // TODO: Skip DirectRunner because batching/timers are not working appropriately
  @Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
  public void testPubSubToDatadogWithBatchAndParallelism() throws IOException {
    PipelineLauncher.LaunchConfig.Builder parameters =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("apiKey", datadogResourceManager.getApiKey())
            .addParameter("batchCount", "10")
            .addParameter("parallelism", "5");
    testPubSubToDatadogMain(parameters, false);
  }

  private static List<Map<String, Object>> datadogEventsToRecords(List<DatadogEvent> events) {
    return events.stream()
        .map(PubSubToDatadogIT::datadogEventToRecord)
        .collect(Collectors.toList());
  }

  private static Map<String, Object> datadogEventToRecord(DatadogEvent e) {
    Map<String, Object> map = new HashMap<>();
    map.put("ddsource", e.ddsource());
    map.put("ddtags", e.ddtags());
    map.put("hostname", e.hostname());
    map.put("service", e.service());
    map.put("message", e.message());
    return map;
  }
}
