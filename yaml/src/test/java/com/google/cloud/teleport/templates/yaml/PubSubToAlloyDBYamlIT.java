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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToAlloyDbYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubToAlloyDbYaml.class)
@RunWith(JUnit4.class)
public final class PubSubToAlloyDBYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToAlloyDBYamlIT.class);
  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  private PubsubResourceManager pubsubResourceManager;
  private PostgresResourceManager postgresResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, postgresResourceManager);
  }

  @Test
  public void testPubSubToAlloyDb() throws IOException {
    pubSubToAlloyDb(Function.identity());
  }

  @SuppressWarnings("unchecked")
  public void pubSubToAlloyDb(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting PubSubToAlloyDb test. Test name: {}. Spec path: {}", testName, specPath);

    /******************************* Arrange ********************************/

    LOG.info("Creating main and dead letter queue topics...");
    TopicName topic = pubsubResourceManager.createTopic("input");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");

    LOG.info("Creating AlloyDb (Postgres) table...");
    String tableName = "test_table";
    Map<String, String> columns = Map.of("id", "INTEGER", "name_upper", "VARCHAR(64)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
    postgresResourceManager.createTable(tableName, schema);

    LOG.info("Creating launch config with yaml pipeline parameters...");
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("topic", topic.toString())
                .addParameter("format", "JSON")
                .addParameter(
                    "schema",
                    "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}")
                .addParameter("windowing", "{\"type\":\"fixed\",\"size\":\"10s\"}")
                .addParameter("language", "python")
                .addParameter(
                    "fields",
                    "{"
                        + "\"id\": {\"expression\": \"int(id)\", \"output_type\": \"integer\"},"
                        + "\"name_upper\": {\"expression\": \"name.upper()\", \"output_type\": \"string\"}"
                        + "}")
                .addParameter("jdbcUrl", postgresResourceManager.getUri())
                .addParameter("username", postgresResourceManager.getUsername())
                .addParameter("password", postgresResourceManager.getPassword())
                .addParameter("location", tableName)
                .addParameter(
                    "writeStatement",
                    "INSERT INTO " + tableName + " (id, name_upper) VALUES (?, ?)")
                .addParameter("outputDeadLetterPubSubTopic", dlqTopic.toString()));

    /********************************* Act **********************************/

    LOG.info("Launching template with options...");
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    LOG.info("Creating messages to be published into the topic...");
    List<ByteString> successMessages = new ArrayList<>();
    List<ByteString> failureMessages = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      successMessages.add(
          ByteString.copyFromUtf8("{\"id\":" + i + ", \"name\":\"name_" + i + "\"}"));
    }
    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      failureMessages.add(
          ByteString.copyFromUtf8("{\"id\":\"not-a-number-" + i + "\", \"name\":\"bad\"}"));
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
              .setMinMessages(BAD_MESSAGES_COUNT)
              .build();

      PipelineOperator.Result result =
          pipelineOperator()
              .waitForConditionsAndFinish(
                  createConfig(info),
                  () -> {
                    LOG.info("Publishing messages to the topic...");
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
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException("Action interrupted", e);
                    } catch (java.util.concurrent.ExecutionException e) {
                      throw new RuntimeException("Error publishing messages", e);
                    }

                    long rowCount = postgresResourceManager.getRowCount(tableName);
                    LOG.info("Checking table size. Current size: {}", rowCount);
                    return rowCount >= MESSAGES_COUNT;
                  },
                  deadLetterCheck);

      /******************************** Assert ********************************/
      assertThatResult(result).meetsConditions();
    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }

    LOG.info("Verifying rows in AlloyDb...");
    long finalRowCount = postgresResourceManager.getRowCount(tableName);
    assertThat(finalRowCount).isEqualTo(MESSAGES_COUNT);
  }
}
