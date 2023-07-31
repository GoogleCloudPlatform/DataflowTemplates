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

import static org.apache.beam.it.mongodb.matchers.MongoDBAsserts.assertThatMongoDBDocuments;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.jsonRecordsToRecords;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.mongodb.MongoDBResourceManager;
import org.apache.beam.it.mongodb.conditions.MongoDBDocumentsCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToMongoDB}. */
@TemplateIntegrationTest(PubSubToMongoDB.class)
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class PubSubToMongoDBIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToMongoDBIT.class);

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  private MongoDBResourceManager mongoResourceManager;

  private PubsubResourceManager pubsubResourceManager;

  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {

    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();

    mongoResourceManager = MongoDBResourceManager.builder(testName).build();

    bigQueryClient =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager, mongoResourceManager, bigQueryClient);
  }

  @Test
  public void testPubsubToMongoDB() throws IOException {
    // Arrange
    TopicName tc = pubsubResourceManager.createTopic(testName);
    SubscriptionName subscription = pubsubResourceManager.createSubscription(tc, "sub-" + testName);

    bigQueryClient.createDataset(REGION);
    TableId dlqTableId = TableId.of(bigQueryClient.getDatasetId(), "dlq");

    mongoResourceManager.createCollection(testName);

    String udfFileName = "transform.js";
    gcsClient.createArtifact(
        "input/" + udfFileName,
        "function transform(inJson) {\n"
            + "    var outJson = JSON.parse(inJson);\n"
            + "    outJson.udf = \"out\";\n"
            + "    return JSON.stringify(outJson);\n"
            + "}");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("mongoDBUri", mongoResourceManager.getUri())
            .addParameter("database", mongoResourceManager.getDatabaseName())
            .addParameter("collection", testName)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter("deadletterTable", toTableSpecLegacy(dlqTableId))
            .addParameter("batchSize", "1")
            .addParameter("sslEnabled", "false")
            .addParameter("javascriptTextTransformGcsPath", getGcsPath("input/" + udfFileName))
            .addParameter("javascriptTextTransformFunctionName", "transform");

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered template job");

    assertThatPipeline(info).isRunning();

    List<String> inMessages = generateMessages();
    for (final String message : inMessages) {
      ByteString data = ByteString.copyFromUtf8(message);
      pubsubResourceManager.publish(tc, ImmutableMap.of(), data);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(tc, ImmutableMap.of(), messageData);
    }

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                MongoDBDocumentsCheck.builder(mongoResourceManager, testName)
                    .setMinDocuments(MESSAGES_COUNT)
                    .build(),
                BigQueryRowsCheck.builder(bigQueryClient, dlqTableId)
                    .setMinRows(BAD_MESSAGES_COUNT)
                    .build());

    List<Document> documents = mongoResourceManager.readCollection(testName);
    documents.forEach(document -> document.remove("_id"));
    List<String> outMessages = new ArrayList<>();
    inMessages.forEach(
        message -> outMessages.add(message.replace("\"udf\": \"in\"", "\"udf\": \"out\"")));

    // Assert
    assertThatResult(result).meetsConditions();
    assertThatMongoDBDocuments(documents).hasRecordsUnordered(jsonRecordsToRecords(outMessages));
  }

  private static List<String> generateMessages() {
    List<String> messages = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      messages.add(
          String.format(
              "{\"id\": %d, \"name\": \"%s\", \"udf\": \"in\"}",
              i, RandomStringUtils.randomAlphabetic(1, 20)));
    }
    return messages;
  }
}
