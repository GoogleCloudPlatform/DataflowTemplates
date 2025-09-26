/*
 * Copyright (C) 2024 Google LLC
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(KafkaToBigQueryYaml.class)
@RunWith(JUnit4.class)
public final class KafkaToBigQueryYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQueryYamlIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bigQueryClient.createDataset(REGION);

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigQueryClient);
  }

  @Test
  public void testKafkaToBigQuery() throws IOException {
    baseKafkaToBigQuery(Function.identity()); // no extra parameters
  }

  private Schema getDeadletterSchema() {
    return Schema.of(
        Field.of("failed_row", StandardSQLTypeName.BYTES),
        Field.of("error_message", StandardSQLTypeName.STRING));
  }

  public void baseKafkaToBigQuery(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    String bqTable = testName;
    Schema bqSchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("name", StandardSQLTypeName.STRING));

    TableId tableId = bigQueryClient.createTable(bqTable, bqSchema);
    String bqTableDlq = bqTable + "_dlq";
    TableId deadletterTableId = bigQueryClient.createTable(bqTableDlq, getDeadletterSchema());

    String bootstrapServers =
        kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");
    LOG.info("Using Kafka Bootstrap Servers: " + bootstrapServers);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("kafkaReadTopics", topicName)
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("messageFormat", "JSON")
                .addParameter(
                    "schema",
                    "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "1")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter(
                    "readBootstrapServers",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());

    List<Map<String, Object>> expectedSuccessRecords = new ArrayList<>();
    List<Map<String, Object>> expectedFailureRecords = new ArrayList<>();
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
          "{\"id\": " + id2 + ", \"name\": \"Pub/Sub\"}");
      expectedSuccessRecords.add(Map.of("id", id1, "name", "Dataflow"));
      expectedSuccessRecords.add(Map.of("id", id2, "name", "Pub/Sub"));

      // Invalid schema
      String invalidRow = "{\"id\": \"not-a-number\", \"name\": \"bad-record\"}";
      publish(kafkaProducer, topicName, i + "3", invalidRow);
      expectedFailureRecords.add(
          Map.of(
              "error_message",
              "Unable to get value from field 'id'. Schema type 'INT64'. JSON node type STRING",
              "failed_row",
              Base64.getEncoder().encodeToString(invalidRow.getBytes())));

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build(),
                BigQueryRowsCheck.builder(bigQueryClient, deadletterTableId)
                    .setMinRows(10)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();

    TableResult tableRows = bigQueryClient.readTable(bqTable);
    assertThatBigQueryRecords(tableRows).hasRecordsUnordered(expectedSuccessRecords);

    TableResult tableDlqRows = bigQueryClient.readTable(bqTableDlq);
    assertThatBigQueryRecords(tableDlqRows).hasRecordsUnordered(expectedFailureRecords);
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
