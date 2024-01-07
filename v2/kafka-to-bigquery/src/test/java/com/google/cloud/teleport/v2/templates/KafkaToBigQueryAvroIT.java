/*
 * Copyright (C) 2022 Google LLC
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
import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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
@TemplateIntegrationTest(KafkaToBigQuery.class)
@RunWith(JUnit4.class)
public final class KafkaToBigQueryAvroIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableResourceManager.class);

  private KafkaResourceManager kafkaResourceManager;
  private BigQueryResourceManager bigQueryClient;
  private org.apache.avro.Schema avroSchema;
  private org.apache.avro.Schema avroDlqSchema;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bigQueryClient.createDataset(REGION);

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();

    URL avroSchemaResource = Resources.getResource("KafkaToBigQueryAvroIT/avro_schema.avsc");
    gcsClient.uploadArtifact("schema.avsc", avroSchemaResource.getPath());
    avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaResource.openStream());
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigQueryClient);
  }

  @Test
  public void testKafkaToBigQueryAvro() throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(Function.identity(), null); // no extra parameters
  }

  @Test
  public void testKafkaToBigQueryAvroWithExistingDLQ() throws IOException, RestClientException {
    TableId deadletterTableId =
        bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b -> b.addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId)), null);
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApi() throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3"),
        null);
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApiExistingDLQ()
      throws IOException, RestClientException {
    TableId deadletterTableId =
        bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId)),
        null);
  }

  @Test
  public void testKafkaToBigQueryAvroWithUdfFunction() throws RestClientException, IOException {
    String udfFileName = "transform.js";
    gcsClient.createArtifact(
        "input/" + udfFileName,
        "function transform(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.productName = data.productName.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("javascriptTextTransformGcsPath", getGcsPath("input/" + udfFileName))
                .addParameter("javascriptTextTransformFunctionName", "transform"),
        tableResult ->
            assertThatBigQueryRecords(tableResult)
                .hasRecordsUnordered(
                    List.of(
                        Map.of("productId", 11, "productName", "DATAFLOW"),
                        Map.of("productId", 12, "productName", "PUB/SUB"))));
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

  public void baseKafkaToBigQueryAvro(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder,
      Consumer<TableResult> assertFunction)
      throws IOException, RestClientException {
    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    String bqTable = testName;
    Schema bqSchema =
        Schema.of(
            Field.of("productId", StandardSQLTypeName.INT64),
            Field.newBuilder("productName", StandardSQLTypeName.STRING).setMaxLength(10L).build());

    TableId tableId = bigQueryClient.createTable(bqTable, bqSchema);
    TableId deadletterTableId = TableId.of(tableId.getDataset(), tableId.getTable() + "_dlq");

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "bootstrapServers",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                .addParameter("inputTopics", topicName)
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("messageFormat", "AVRO")
                .addParameter("avroSchemaPath", getGcsPath("schema.avsc")));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    registryClient.register(topicName + "-value", avroSchema, 1, 1);

    KafkaProducer<String, Object> kafkaProducer =
        kafkaResourceManager.buildProducer(
            new StringSerializer(), new KafkaAvroSerializer(registryClient));

    for (int i = 1; i <= 10; i++) {
      GenericRecord dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 0);
      publish(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 0);
      publish(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid = createRecord(Integer.valueOf(i + "3"), "InvalidNameTooLong", 0);
      publish(kafkaProducer, topicName, i + "3", invalid);

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
    if (assertFunction != null) {
      assertFunction.accept(tableRows);
    } else {
      assertThatBigQueryRecords(tableRows)
          .hasRecordsUnordered(
              List.of(
                  Map.of("productId", 11, "productName", "Dataflow"),
                  Map.of("productId", 12, "productName", "Pub/Sub")));
    }
  }

  private void publish(
      KafkaProducer<String, Object> producer, String topicName, String key, GenericRecord value) {
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

  private GenericRecord createRecord(int id, String name, double value) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", name)
        .build();
  }
}
