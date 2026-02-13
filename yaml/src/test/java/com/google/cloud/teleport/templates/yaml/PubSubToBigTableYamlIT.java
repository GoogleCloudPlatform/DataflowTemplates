/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToBigTableYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubToBigTableYaml.class)
@RunWith(JUnit4.class)
public final class PubSubToBigTableYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigTableYamlIT.class);

  private PubsubResourceManager pubsubResourceManager;
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setup() throws IOException {
    // Pubsub and BigTable resource managers
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    // Clean up resource managers
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigtableResourceManager);
  }

  @Test
  public void testPubSubToBigTable() throws IOException {
    pubSubToBigTable(Function.identity());
  }

  public void pubSubToBigTable(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubToBigTable test. Test name: {}. Spec path: {}", testName, specPath);

    /******************************* Arrange ********************************/

    LOG.info("Creating main and dead letter queue topics...");
    TopicName topic = pubsubResourceManager.createTopic("input");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");

    LOG.info("Creating Bigtable table...");
    String tableId = "test_table";
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf1"));

    LOG.info("Creating launch config with yaml pipeline parameters...");
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("topic", topic.toString())
                .addParameter("format", "JSON")
                .addParameter(
                    "schema",
                    "{\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"},\"type\":{\"type\":\"string\"},\"family_name\":{\"type\":\"string\"},\"column_qualifier\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"},\"timestamp_micros\":{\"type\":\"integer\"}}}")
                .addParameter("windowing", "{\"type\":\"fixed\",\"size\":\"10s\"}")
                .addParameter("tableId", tableId)
                .addParameter("instanceId", bigtableResourceManager.getInstanceId())
                .addParameter("projectId", PROJECT)
                .addParameter("language", "python")
                .addParameter(
                    "fields",
                    "{"
                        + "\"key\": {\"expression\": \"key.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"type\": {\"expression\": \"type\", \"output_type\": \"string\"},"
                        + "\"family_name\": {\"expression\": \"family_name\", \"output_type\": \"string\"},"
                        + "\"column_qualifier\": {\"expression\": \"column_qualifier.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"value\": {\"expression\": \"value.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"timestamp_micros\": {\"expression\": \"timestamp_micros\", \"output_type\": \"integer\"}"
                        + "}")
                .addParameter("outputDeadLetterPubSubTopic", dlqTopic.toString()));

    /********************************* Act **********************************/

    LOG.info("Launching template with options...");
    PipelineLauncher.LaunchInfo info = launchTemplate(options);

    LOG.info("Template launched. LaunchInfo: {}", info);
    assertThatPipeline(info).isRunning();

    LOG.info("Creating messages to be published into the topic...");
    List<ByteString> successMessages = new ArrayList<>();
    List<ByteString> failureMessages = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      long id1 = Long.parseLong(i + "1");
      long id2 = Long.parseLong(i + "2");
      successMessages.add(
          ByteString.copyFromUtf8(
              "{\"key\": \"row"
                  + id1
                  + "\", \"type\": \"SetCell\", \"family_name\": \"cf1\", \"column_qualifier\": \"cq1\", \"value\": \"value1\", \"timestamp_micros\": 5000}"));
      successMessages.add(
          ByteString.copyFromUtf8(
              "{\"key\": \"row"
                  + id2
                  + "\", \"type\": \"SetCell\", \"family_name\": \"cf1\", \"column_qualifier\": \"cq2\", \"value\": \"value2\", \"timestamp_micros\": 1000}"));

      // Missing a field, which during MapToFields transformation will cause a failure
      String invalidRow =
          "{\"type\": \"SetCell\", \"family_name\": \"cf1\", \"column_qualifier\": \"cq-invalid\", \"value\": \"invalid-value\", \"timestamp_micros\": 0}";
      failureMessages.add(ByteString.copyFromUtf8(invalidRow));
    }

    LOG.info("Waiting for pipeline condition...");

    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topic).setCredentialsProvider(credentialsProvider).build();
      final Publisher finalPublisher = publisher;

      SubscriptionName dlqSubscription =
          pubsubResourceManager.createSubscription(dlqTopic, "dlq-subscription");

      PubsubMessagesCheck deadLetterCheck =
          PubsubMessagesCheck.builder(pubsubResourceManager, dlqSubscription)
              .setMinMessages(10)
              .build();

      PipelineOperator.Result result =
          pipelineOperator()
              .waitForConditionsAndFinish(
                  createConfig(info),
                  // Publish messages and check that 20 rows are in the BigTable
                  () -> {
                    LOG.info(
                        "Publishing messages to the topic to ensure pipeline has messages to"
                            + " process...");
                    List<ApiFuture<String>> futures = new ArrayList<>();
                    for (ByteString successMessage : successMessages) {
                      futures.add(
                          finalPublisher.publish(
                              PubsubMessage.newBuilder().setData(successMessage).build()));
                    }
                    for (ByteString failureMessage : failureMessages) {
                      futures.add(
                          finalPublisher.publish(
                              PubsubMessage.newBuilder().setData(failureMessage).build()));
                    }
                    try {
                      ApiFutures.allAsList(futures).get();
                      LOG.info("All messages published successfully for this check.");
                      Thread.sleep(1000); // Sleep for 1 second to avoid crashing the vm
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException("Action interrupted", e);
                    } catch (java.util.concurrent.ExecutionException e) {
                      throw new RuntimeException("Error publishing messages", e);
                    }

                    List<Row> rows = bigtableResourceManager.readTable(tableId);
                    if (rows == null) {
                      LOG.warn(
                          "bigtableResourceManager.readTable(tableId) returned null. Retrying.");
                      return false;
                    }
                    int tableSize = rows.size();
                    LOG.info("Checking table size. Current size: {}", tableSize);
                    return tableSize == 20;
                  },
                  deadLetterCheck);

      /******************************** Assert ********************************/
      assertThatResult(result).meetsConditions();

    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }

    LOG.info("Verifying 20 rows in the BigTable still exist...");
    List<Row> tableRows = bigtableResourceManager.readTable(tableId);
    assertThat(tableRows).hasSize(20);

    LOG.info("Verifying the exact 20 rows in BigTable...");
    Map<String, Row> rowMap =
        tableRows.stream()
            .collect(Collectors.toMap(row -> row.getKey().toStringUtf8(), row -> row));

    for (int i = 1; i <= 10; i++) {
      String key1 = "row" + i + "1";
      assertThat(rowMap).containsKey(key1);
      Row row1 = rowMap.get(key1);
      assertThat(row1.getCells()).hasSize(1);
      assertThat(row1.getCells().get(0).getFamily()).isEqualTo("cf1");
      assertThat(row1.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo("cq1");
      assertThat(row1.getCells().get(0).getValue().toStringUtf8()).isEqualTo("value1");
      assertThat(row1.getCells().get(0).getTimestamp()).isEqualTo(5000L);

      String key2 = "row" + i + "2";
      assertThat(rowMap).containsKey(key2);
      Row row2 = rowMap.get(key2);
      assertThat(row2.getCells()).hasSize(1);
      assertThat(row2.getCells().get(0).getFamily()).isEqualTo("cf1");
      assertThat(row2.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo("cq2");
      assertThat(row2.getCells().get(0).getValue().toStringUtf8()).isEqualTo("value2");
      assertThat(row2.getCells().get(0).getTimestamp()).isEqualTo(1000L);
    }
  }
}
