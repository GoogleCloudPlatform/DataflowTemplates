/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static com.google.cloud.teleport.it.gcp.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.gcp.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.gcp.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.elasticsearch.DefaultElasticsearchResourceManager;
import com.google.cloud.teleport.it.elasticsearch.ElasticsearchResourceManager;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.gcp.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.gcp.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.gcp.matchers.ListAccumulator;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.utils.ResourceManagerUtils;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubSubToElasticsearch} (PubSub_to_Elasticsearch). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubSubToElasticsearch.class)
@RunWith(JUnit4.class)
public final class PubSubToElasticsearchIT extends TemplateTestBase {

  private PubsubResourceManager pubsubResourceManager;
  private ElasticsearchResourceManager elasticsearchResourceManager;

  public static final int MESSAGES_TO_SEND = 25;
  private static final int MALFORMED_MESSAGES_TO_SEND = 5;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    elasticsearchResourceManager =
        DefaultElasticsearchResourceManager.builder(testId).setHost(HOST_IP).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, elasticsearchResourceManager);
  }

  @Test
  public void testPubSubToElasticsearch() throws IOException {
    basePubSubToElasticsearch(Function.identity(), Map.of("id", "1", "name", testName));
  }

  @Test
  public void testPubSubToElasticsearchWithUdf() throws IOException {
    artifactClient.createArtifact(
        "udf-name-upper-case.js",
        "function transform(inJson) {\n"
            + "  var obj = JSON.parse(inJson);\n"
            + "  obj.name = obj.name.toUpperCase();\n"
            + "  return JSON.stringify(obj);\n"
            + "}");

    basePubSubToElasticsearch(
        builder ->
            builder
                .addParameter(
                    "javascriptTextTransformGcsPath", getGcsPath("udf-name-upper-case.js"))
                .addParameter("javascriptTextTransformFunctionName", "transform"),
        Map.of("id", "1", "name", testName.toUpperCase()));
  }

  public void basePubSubToElasticsearch(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder,
      Map<String, Object> expectedRow)
      throws IOException {

    // Arrange
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-1");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "dlq-1");
    String indexName = "logs-gcp.pubsub-default";

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("inputSubscription", subscription.toString())
                    .addParameter("errorOutputTopic", dlqTopic.toString())
                    .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
                    .addParameter("apiKey", "elastic")));
    assertThatPipeline(info).isRunning();

    for (int i = 0; i < MESSAGES_TO_SEND; i++) {
      ByteString data =
          ByteString.copyFromUtf8(String.format("{\"id\": \"%d\", \"name\": \"%s\"}", i, testName));
      pubsubResourceManager.publish(topic, ImmutableMap.of(), data);
    }

    // Send bad messages
    for (int i = 0; i < MALFORMED_MESSAGES_TO_SEND; i++) {
      ByteString data = ByteString.copyFromUtf8(String.format("{\"id\": \"100%d\", ", i));
      pubsubResourceManager.publish(topic, ImmutableMap.of(), data);
    }

    ListAccumulator<ReceivedMessage> accumulator = new ListAccumulator<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () ->
                    elasticsearchResourceManager.count(indexName) >= MESSAGES_TO_SEND
                        && accumulator.accumulate(
                                pubsubResourceManager
                                    .pull(dlqSubscription, MALFORMED_MESSAGES_TO_SEND + 1)
                                    .getReceivedMessagesList())
                            >= MALFORMED_MESSAGES_TO_SEND);

    // Assert
    assertThatResult(result).meetsConditions();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(MESSAGES_TO_SEND);
    List<Map<String, Object>> records = elasticsearchResourceManager.fetchAll(indexName);
    assertThatRecords(records).hasRecordSubset(expectedRow);
    assertThat(accumulator.count()).isEqualTo(MALFORMED_MESSAGES_TO_SEND);
  }
}
