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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link KafkaToSpannerYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(KafkaToSpannerYaml.class)
@RunWith(JUnit4.class)
public final class KafkaToSpannerYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToSpannerYamlIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .setCredentials(credentials)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, spannerResourceManager);
  }

  @Test
  public void testKafkaToSpanner() throws IOException {
    kafkaToSpanner(Function.identity());
  }

  public void kafkaToSpanner(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting kafkaToSpanner test. Test name: {}. Spec path: {}", testName, specPath);

    // Arrange
    LOG.info("Creating Kafka topic...");
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LOG.info("Creating Spanner table...");
    String tableId = testName;
    spannerResourceManager.executeDdlStatement(
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  id INT64 NOT NULL,\n"
                + "  name STRING(1024)\n"
                + ") PRIMARY KEY (id)",
            tableId));

    String bootstrapServers =
        kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");
    LOG.info("Using Kafka Bootstrap Servers: {}", bootstrapServers);

    LOG.info("Creating launch config with yaml pipeline parameters...");
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("bootstrapServers", bootstrapServers)
                .addParameter("topic", topicName)
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

    LOG.info("Publishing messages into the Kafka topic...");
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    try (KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer())) {
      for (int i = 1; i <= 10; i++) {
        long id1 = Long.parseLong(i + "1");
        long id2 = Long.parseLong(i + "2");
        publish(
            kafkaProducer,
            topicName,
            String.valueOf(id1),
            "{\"id\": " + id1 + ", \"name\": \"Dataflow\"}");
        publish(
            kafkaProducer,
            topicName,
            String.valueOf(id2),
            "{\"id\": " + id2 + ", \"name\": \"Spanner\"}");
        expectedRecords.add(Map.of("id", id1, "name", "Dataflow"));
        expectedRecords.add(Map.of("id", id2, "name", "Spanner"));
      }
    }

    LOG.info("Waiting for pipeline condition...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                SpannerRowsCheck.builder(spannerResourceManager, tableId).setMinRows(20).build());

    // Assert
    assertThatResult(result).meetsConditions();

    LOG.info("Verifying 20 rows in Spanner table...");
    List<Struct> records = spannerResourceManager.readTableRecords(tableId, List.of("id", "name"));
    assertThat(records).hasSize(20);
    assertThatStructs(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }

  private void publish(
      KafkaProducer<String, String> producer, String topicName, String key, String value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
      LOG.info("Published record with key: {}, value: {}", key, value);
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
