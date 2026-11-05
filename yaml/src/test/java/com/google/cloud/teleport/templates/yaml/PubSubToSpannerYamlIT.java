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
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToSpannerYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubToSpannerYaml.class)
@RunWith(JUnit4.class)
public final class PubSubToSpannerYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToSpannerYamlIT.class);

  private PubsubResourceManager pubsubResourceManager;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .setCredentials(credentials)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, spannerResourceManager);
  }

  @Test
  public void testPubSubToSpanner() throws IOException {
    pubSubToSpanner(Function.identity());
  }

  public void pubSubToSpanner(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubToSpanner test. Test name: {}. Spec path: {}", testName, specPath);

    // Arrange
    LOG.info("Creating Pub/Sub topic...");
    TopicName topic = pubsubResourceManager.createTopic("input");

    LOG.info("Creating Spanner table...");
    String tableId = testName;
    spannerResourceManager.executeDdlStatement(
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  id INT64 NOT NULL,\n"
                + "  name STRING(1024)\n"
                + ") PRIMARY KEY (id)",
            tableId));

    LOG.info("Creating launch config with yaml pipeline parameters...");
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("topic", topic.toString())
                .addParameter("format", "JSON")
                .addParameter(
                    "schema",
                    "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}")
                .addParameter("projectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("tableId", tableId));

    // Act
    LOG.info("Launching template with options...");
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    LOG.info("Template launched. LaunchInfo: {}", info);
    assertThatPipeline(info).isRunning();

    LOG.info("Preparing messages to be published into the Pub/Sub topic...");
    List<ByteString> messages = new ArrayList<>();
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      long id1 = Long.parseLong(i + "1");
      long id2 = Long.parseLong(i + "2");
      messages.add(ByteString.copyFromUtf8("{\"id\": " + id1 + ", \"name\": \"Dataflow\"}"));
      messages.add(ByteString.copyFromUtf8("{\"id\": " + id2 + ", \"name\": \"Spanner\"}"));
      expectedRecords.add(Map.of("id", id1, "name", "Dataflow"));
      expectedRecords.add(Map.of("id", id2, "name", "Spanner"));
    }

    LOG.info("Waiting for pipeline condition...");
    Publisher publisher = null;
    try {
      publisher = Publisher.newBuilder(topic).setCredentialsProvider(credentialsProvider).build();
      final Publisher finalPublisher = publisher;

      PipelineOperator.Result result =
          pipelineOperator()
              .waitForConditionAndFinish(
                  createConfig(info),
                  () -> {
                    LOG.info(
                        "Publishing messages to the topic to ensure pipeline has messages to"
                            + " process...");
                    List<ApiFuture<String>> futures = new ArrayList<>();
                    for (ByteString message : messages) {
                      futures.add(
                          finalPublisher.publish(
                              PubsubMessage.newBuilder().setData(message).build()));
                    }
                    try {
                      ApiFutures.allAsList(futures).get();
                      LOG.info("All messages published successfully for this check.");
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException("Action interrupted", e);
                    } catch (ExecutionException e) {
                      throw new RuntimeException("Error publishing messages", e);
                    }

                    List<Struct> records =
                        spannerResourceManager.readTableRecords(tableId, List.of("id", "name"));
                    int tableSize = records.size();
                    LOG.info("Checking Spanner table size. Current size: {}", tableSize);
                    return tableSize == 20;
                  });

      // Assert
      assertThatResult(result).meetsConditions();
    } finally {
      if (publisher != null) {
        publisher.shutdown();
      }
    }

    LOG.info("Verifying 20 rows in Spanner table...");
    List<Struct> records = spannerResourceManager.readTableRecords(tableId, List.of("id", "name"));
    assertThat(records).hasSize(20);
    assertThatStructs(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }
}
