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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
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

/** Integration test for {@link PubSubToBigQueryYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubToBigQueryYaml.class)
@RunWith(JUnit4.class)
public final class PubSubToBigQueryYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQueryYamlIT.class);

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final int MESSAGES_COUNT = 10;

  @Before
  public void setup() throws IOException {
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
  public void testPubSubToBigQuery() throws IOException {
    pubSubToBigQuery(Function.identity());
  }

  public void pubSubToBigQuery(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubToBigQuery test.");

    // Arrange BigQuery
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("name", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);
    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    // Arrange PubSub
    String nameSuffix = RandomStringUtils.randomAlphanumeric(8);
    TopicName topic = pubsubResourceManager.createTopic("input-" + nameSuffix);

    String schema =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"job\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}}}";

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("topic", topic.toString())
                .addParameter("format", "JSON")
                .addParameter("schema", schema)
                .addParameter("table", toTableSpecStandard(table)));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedMessages = new ArrayList<>();
    List<ByteString> messageDataList = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "message");
      messageDataList.add(ByteString.copyFromUtf8(new JSONObject(message).toString()));
      expectedMessages.add(message);
    }

    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topic).setCredentialsProvider(credentialsProvider).build();
      final Publisher finalPublisher = publisher;

      PipelineOperator.Result result =
          pipelineOperator()
              .waitForConditionsAndFinish(
                  createConfig(info),
                  // Custom condition loop to continuously publish & check, preventing watermark
                  // stalls
                  () -> {
                    LOG.info("Publishing messages to topic...");
                    List<ApiFuture<String>> futures = new ArrayList<>();
                    for (ByteString data : messageDataList) {
                      futures.add(
                          finalPublisher.publish(PubsubMessage.newBuilder().setData(data).build()));
                    }
                    try {
                      ApiFutures.allAsList(futures).get();
                      Thread.sleep(2000);
                    } catch (Exception e) {
                      throw new RuntimeException("Error publishing messages", e);
                    }

                    TableResult rows = bigQueryResourceManager.readTable(table);
                    if (rows == null) {
                      return false;
                    }
                    int totalFound = 0;
                    for (var r : rows.getValues()) {
                      totalFound++;
                    }
                    LOG.info("Checking BigQuery rows. Current size: {}", totalFound);
                    return totalFound >= MESSAGES_COUNT;
                  });

      // Assert
      assertThatResult(result).meetsConditions();

    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }

    TableResult records = bigQueryResourceManager.readTable(table);
    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedMessages);
  }
}
