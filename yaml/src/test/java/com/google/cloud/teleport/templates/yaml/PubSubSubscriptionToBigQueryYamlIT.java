/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubSubscriptionToBigQueryYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubSubscriptionToBigQueryYaml.class)
@RunWith(JUnit4.class)
public final class PubSubSubscriptionToBigQueryYamlIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PubSubSubscriptionToBigQueryYamlIT.class);

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final int MESSAGES_COUNT = 10;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testPubSubSubscriptionToBigQuery() throws IOException {
    pubSubSubscriptionToBigQuery(Function.identity());
  }

  public void pubSubSubscriptionToBigQuery(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubSubscriptionToBigQuery test.");

    // Arrange BigQuery
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("name", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);
    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    // Arrange - Pub/Sub topic and subscription setup
    //
    // Note on test design:
    // The Dataflow pipeline strictly reads from inputSubscription and writes to BigQuery.
    // However, in Google Cloud Pub/Sub, messages cannot be published directly to a
    // subscription; they must be published to a topic (inputTopic) that routes them to
    // inputSubscription.
    String nameSuffix = RandomStringUtils.randomAlphanumeric(8);
    TopicName inputTopic = pubsubResourceManager.createTopic("input-" + nameSuffix);
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-sub-" + nameSuffix);

    String schema =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"job\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}}}";

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("subscription", inputSubscription.toString())
                .addParameter("format", "JSON")
                .addParameter("schema", schema)
                .addParameter("table", toTableSpecStandard(table)));

    // Act - Launch pipeline
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Publish messages to inputTopic (retained by inputSubscription until read by pipeline)
    LOG.info("Publishing {} messages to input topic...", MESSAGES_COUNT);
    List<Map<String, Object>> expectedMessages = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "message");
      String messageJson = new JSONObject(message).toString();
      pubsubResourceManager.publish(inputTopic, Map.of(), ByteString.copyFromUtf8(messageJson));
      expectedMessages.add(message);
    }

    // Wait for pipeline processing
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                    .setMinRows(MESSAGES_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();

    TableResult records = bigQueryResourceManager.readTable(table);
    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedMessages);
  }
}
