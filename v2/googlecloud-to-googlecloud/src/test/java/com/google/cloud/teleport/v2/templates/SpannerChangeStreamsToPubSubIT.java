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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.JsonTestUtil;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link SpannerChangeStreamsToPubSub Spanner Change Streams to PubSub}
 * template.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerChangeStreamsToPubSub.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToPubSubIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 20;

  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, pubsubResourceManager);
  }

  @Test
  public void testSpannerChangeStreamsToPubsubJson() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + "  Float32Col FLOAT32,\n"
                + "  Float64Col FLOAT64,\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerResourceManager.executeDdlStatement(createTableStatement);

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);

    TopicName outputTopic = pubsubResourceManager.createTopic(String.format("%s-topic", testName));
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(
            outputTopic, String.format("%s-subscription", testName));

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
            .addParameter("spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
            .addParameter("spannerChangeStreamName", testName + "_stream")
            .addParameter("pubsubTopic", outputTopic.getTopic())
            .addParameter("outputDataFormat", "JSON")
            .addParameter("rpcPriority", "HIGH")
            .addParameter("includeSpannerSource", "true")
            .addParameter("outputMessageMetadata", "us-central1");

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<Mutation> expectedData = generateTableRows(testName);
    spannerResourceManager.write(expectedData);

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            // Expecting only 1 message, but that message has all of spanner table's data
            .setMinMessages(1)
            .build();

    PipelineOperator.Result result =
        pipelineOperator().waitForConditionAndCancel(createConfig(info), pubsubCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> records = new ArrayList<>();
    pubsubCheck
        .getReceivedMessageList()
        .forEach(
            receivedMessage -> {
              JsonObject o =
                  new JsonParser()
                      .parse(receivedMessage.getMessage().getData().toStringUtf8())
                      .getAsJsonObject();
              assertEquals(
                  o.get("spannerDatabaseId").getAsString(), spannerResourceManager.getDatabaseId());
              assertEquals(
                  o.get("spannerInstanceId").getAsString(), spannerResourceManager.getInstanceId());
              assertEquals(o.get("outputMessageMetadata").getAsString(), "us-central1");
              o.remove("spannerDatabaseId");
              o.remove("spannerInstanceId");
              o.remove("outputMessageMetadata");
              DataChangeRecord s = new Gson().fromJson(o, DataChangeRecord.class);
              for (Mod mod : s.getMods()) {
                Map<String, Object> record = new HashMap<>();
                try {
                  record.putAll(JsonTestUtil.readRecord(mod.getKeysJson()));
                } catch (Exception e) {
                  throw new RuntimeException("Error reading " + mod.getKeysJson() + " as JSON.", e);
                }
                try {
                  record.putAll(JsonTestUtil.readRecord(mod.getNewValuesJson()));
                } catch (Exception e) {
                  throw new RuntimeException(
                      "Error reading " + mod.getNewValuesJson() + " as JSON.", e);
                }
                records.add(record);
              }
            });

    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    expectedData.forEach(
        mutation -> {
          Map<String, Object> expectedRecord =
              mutation.asMap().entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey(), e -> valueToString(e.getValue())));
          expectedRecords.add(expectedRecord);
        });
    assertThatRecords(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }

  @Test
  public void testSpannerChangeStreamsToPubsubAvro() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + "  Float32Col FLOAT32,\n"
                + "  Float64Col FLOAT64,\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerResourceManager.executeDdlStatement(createTableStatement);

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);

    TopicName outputTopic = pubsubResourceManager.createTopic(String.format("%s-topic", testName));
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(
            outputTopic, String.format("%s-subscription", testName));

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
            .addParameter("spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
            .addParameter("spannerChangeStreamName", testName + "_stream")
            .addParameter("pubsubTopic", outputTopic.getTopic())
            .addParameter("outputDataFormat", "AVRO")
            .addParameter("rpcPriority", "HIGH")
            .addParameter("includeSpannerSource", "true")
            .addParameter("outputMessageMetadata", "us-central1");

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<Mutation> expectedData = generateTableRows(testName);
    spannerResourceManager.write(expectedData);

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            // Expecting only 1 message, but that message has all of spanner table's data
            .setMinMessages(1)
            .build();

    PipelineOperator.Result result =
        pipelineOperator().waitForConditionAndCancel(createConfig(info), pubsubCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    AvroCoder<com.google.cloud.teleport.v2.DataChangeRecord> coder =
        AvroCoder.of(com.google.cloud.teleport.v2.DataChangeRecord.class);
    pubsubCheck
        .getReceivedMessageList()
        .forEach(
            receivedMessage -> {
              try {
                com.google.cloud.teleport.v2.DataChangeRecord avroRecord =
                    CoderUtils.decodeFromByteArray(
                        coder, receivedMessage.getMessage().getData().toByteArray());
                assertEquals(
                    avroRecord.get("spannerDatabaseId"), spannerResourceManager.getDatabaseId());
                assertEquals(
                    avroRecord.get("spannerInstanceId"), spannerResourceManager.getInstanceId());
                assertEquals(avroRecord.get("outputMessageMetadata"), "us-central1");
              } catch (IOException e) {
                throw new RuntimeException(
                    "Error reading " + outputTopic.toString() + " as AVRO.", e);
              }
            });
  }

  private static String valueToString(Value val) {
    switch (val.getType().getCode()) {
      case INT64:
        return Long.toString(val.getInt64());
      case FLOAT32:
        return Float.toString(val.getFloat32());
      case FLOAT64:
        return Double.toString(val.getFloat64());
      case STRING:
        return val.getString();
      default:
        throw new IllegalArgumentException("Unsupported type: " + val.getType());
    }
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      mutation.set("FirstName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("LastName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      // Avoiding random numbers as asserting their presence via string match is
      // error-prone for floats.
      mutation.set("Float32Col").to(i + 0.5);
      mutation.set("Float64Col").to(i + 1.5);
      mutations.add(mutation.build());
    }

    return mutations;
  }
}
