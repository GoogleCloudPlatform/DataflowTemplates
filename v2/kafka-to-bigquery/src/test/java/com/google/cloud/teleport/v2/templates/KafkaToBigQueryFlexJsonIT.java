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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
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

/** Integration test for {@link KafkaToBigQuery} (Kafka_To_BigQuery). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToBigQueryFlex.class)
@RunWith(JUnit4.class)
public final class KafkaToBigQueryFlexJsonIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableResourceManager.class);

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
    baseKafkaToBigQuery(
        b ->
            b.addParameter("messageFormat", "JSON")
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  @Test
  public void testKafkaToBigQueryWithUdfFunction() throws RestClientException, IOException {
    String udfFileName = "input/transform.js";
    gcsClient.createArtifact(
        udfFileName,
        "function transform(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.name = data.name.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    baseKafkaToBigQuery(
        b ->
            b.addParameter("messageFormat", "JSON")
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE")
                .addParameter("javascriptTextTransformGcsPath", getGcsPath(udfFileName))
                .addParameter("javascriptTextTransformFunctionName", "transform"),
        s -> s == null ? null : s.toUpperCase());
  }

  @Test
  @TemplateIntegrationTest(value = KafkaToBigQueryFlex.class, template = "Kafka_to_BigQuery_Flex")
  public void testKafkaToBigQueryWithExistingDLQ() throws IOException {
    TableId deadletterTableId =
        bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQuery(
        b ->
            b.addParameter("messageFormat", "JSON")
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("useBigQueryDLQ", "true")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  private Schema getDeadletterSchema() {
    Schema dlqSchema =
        Schema.of(
            Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP)
                .setMode(Mode.REQUIRED)
                .build(),
            Field.newBuilder("payloadString", StandardSQLTypeName.STRING)
                .setMode(Mode.REQUIRED)
                .build(),
            Field.newBuilder("payloadBytes", StandardSQLTypeName.BYTES)
                .setMode(Mode.REQUIRED)
                .build(),
            Field.newBuilder(
                    "attributes",
                    StandardSQLTypeName.STRUCT,
                    Field.newBuilder("key", StandardSQLTypeName.STRING)
                        .setMode(Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("value", StandardSQLTypeName.STRING)
                        .setMode(Mode.NULLABLE)
                        .build())
                .setMode(Mode.REPEATED)
                .build(),
            Field.newBuilder("errorMessage", StandardSQLTypeName.STRING)
                .setMode(Mode.NULLABLE)
                .build(),
            Field.newBuilder("stacktrace", StandardSQLTypeName.STRING)
                .setMode(Mode.NULLABLE)
                .build());
    return dlqSchema;
  }

  public void baseKafkaToBigQuery(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    baseKafkaToBigQuery(paramsAdder, Function.identity());
  }

  public void baseKafkaToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder,
      Function<String, String> namePostProcessor)
      throws IOException {
    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    String bqTable = testName;
    Schema bqSchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("name", StandardSQLTypeName.STRING));

    TableId tableId = bigQueryClient.createTable(bqTable, bqSchema);
    TableId deadletterTableId = TableId.of(tableId.getDataset(), tableId.getTable() + "_dlq");

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("kafkaReadOffset", "earliest")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId)));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());

    for (int i = 1; i <= 10; i++) {
      publish(kafkaProducer, topicName, i + "1", "{\"id\": " + i + "1, \"name\": \"Dataflow\"}");
      publish(kafkaProducer, topicName, i + "2", "{\"id\": " + i + "2, \"name\": \"Pub/Sub\"}");
      // Invalid schema to test the DLQ
      publish(kafkaProducer, topicName, i + "3", "bad json string");

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    Result result =
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
    TableResult dlqRows = bigQueryClient.readTable(deadletterTableId);

    assertThatBigQueryRecords(tableRows)
        .hasRecordsUnordered(
            List.of(
                Map.of("id", 11, "name", namePostProcessor.apply("Dataflow")),
                Map.of("id", 12, "name", namePostProcessor.apply("Pub/Sub"))));
    assertThatBigQueryRecords(dlqRows).hasRecordsWithStrings(List.of("bad json string"));
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
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
