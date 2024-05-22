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
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroSerializer;
import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import net.jcip.annotations.NotThreadSafe;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
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

/** Integration test for {@link KafkaToBigQuery} (Kafka_To_BigQuery). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToBigQueryFlex.class)
@RunWith(JUnit4.class)
@NotThreadSafe
public final class KafkaToBigQueryFlexAvroIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQueryFlexAvroIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private BigQueryResourceManager bigQueryClient;
  private String bqDatasetId;
  private TableId deadletterTableId;
  private TableId tableId;
  private Schema bqSchema;
  private org.apache.avro.Schema avroSchema;
  private org.apache.avro.Schema otherAvroSchema;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bqDatasetId = bigQueryClient.createDataset(REGION);
    bqSchema =
        Schema.of(
            Field.of("productId", StandardSQLTypeName.INT64),
            Field.newBuilder("productName", StandardSQLTypeName.STRING).setMaxLength(10L).build());

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();

    URL avroSchemaResource = Resources.getResource("KafkaToBigQueryFlexAvroIT/avro_schema.avsc");
    gcsClient.uploadArtifact("avro_schema.avsc", avroSchemaResource.getPath());
    avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaResource.openStream());

    URL otherAvroSchemaResource =
        Resources.getResource("KafkaToBigQueryFlexAvroIT/other_avro_schema.avsc");
    gcsClient.uploadArtifact("other_avro_schema.avsc", otherAvroSchemaResource.getPath());
    otherAvroSchema =
        new org.apache.avro.Schema.Parser().parse(otherAvroSchemaResource.openStream());
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigQueryClient);
  }

  @Test
  public void testKafkaToBigQueryAvroInConfluentFormat() throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "CONFLUENT_WIRE_FORMAT")
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroInConfluentFormatWithKey()
      throws IOException, RestClientException {
    List<Field> fields = new ArrayList<>(bqSchema.getFields());
    fields.add(Field.of("_key", StandardSQLTypeName.BYTES));
    bqSchema = Schema.of(fields);
    tableId = bigQueryClient.createTable(testName + "WithKey", bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "CONFLUENT_WIRE_FORMAT")
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("persistKafkaKey", "true")
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithSchemaRegistry() throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaRegistryConnectionUrl", "http://10.128.0.60:8081")
                .addParameter("outputDataset", bqDatasetId)
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithSchemaRegistryWithKey()
      throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaRegistryConnectionUrl", "http://10.128.0.60:8081")
                .addParameter("outputDataset", bqDatasetId)
                .addParameter("persistKafkaKey", "true")
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroInNonConfluentFormat()
      throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "NON_WIRE_FORMAT")
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroInNonConfluentFormatWithKey()
      throws IOException, RestClientException {
    List<Field> fields = new ArrayList<>(bqSchema.getFields());
    fields.add(Field.of("_key", StandardSQLTypeName.BYTES));
    bqSchema = Schema.of(fields);
    tableId = bigQueryClient.createTable(testName + "WithKey", bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("avroFormat", "NON_WIRE_FORMAT")
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("persistKafkaKey", "true")
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithExistingDLQ() throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApi() throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApiExistingDLQ()
      throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("avroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("enableKafkaDlq", "false")
                .addParameter("deadLetterQueueKafkaTopic", "false"));
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

  private void baseKafkaToBigQueryAvro(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, RestClientException {
    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServers",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                .addParameter("kafkaReadTopics", topicName)
                .addParameter("kafkaReadOffset", "earliest")
                .addParameter("messageFormat", "AVRO"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<ConditionCheck> conditions = new ArrayList<ConditionCheck>();

    if (options.getParameter("avroFormat") != null
        && options.getParameter("avroFormat").equals("CONFLUENT_WIRE_FORMAT")
        && options.getParameter("schemaRegistryConnectionUrl") != null) {

      publishDoubleSchemaMessages(topicName);
      tableId = TableId.of(bqDatasetId, avroSchema.getFullName().replace(".", "-"));
      TableId otherTableId =
          TableId.of(bqDatasetId, otherAvroSchema.getFullName().replace(".", "-"));

      conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build());
      conditions.add(
          BigQueryRowsCheck.builder(bigQueryClient, otherTableId).setMinRows(20).build());

    } else if (options.getParameter("avroFormat") != null
        && options.getParameter("avroFormat").equals("NON_WIRE_FORMAT")
        && options.getParameter("avroSchemaPath") != null) {

      publishBinaryMessages(topicName);
      conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build());

    } else {

      publishSingleSchemaMessages(topicName);
      conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build());
    }

    if (options.getParameter("outputDeadletterTable") != null) {
      conditions.add(
          BigQueryRowsCheck.builder(bigQueryClient, deadletterTableId).setMinRows(10).build());
    }

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info), conditions.toArray(new ConditionCheck[0]));

    // Assert
    assertThatResult(result).meetsConditions();

    TableResult tableRows = bigQueryClient.readTable(tableId);
    if (options.getParameter("persistKafkaKey") != null
        && options.getParameter("persistKafkaKey").equals("true")) {
      assertThatBigQueryRecords(tableRows)
          .hasRecordsUnordered(
              List.of(
                  Map.of(
                      "productId",
                      11,
                      "productName",
                      "Dataflow",
                      "_key",
                      Base64.getEncoder().encodeToString("11".getBytes())),
                  Map.of(
                      "productId",
                      12,
                      "productName",
                      "Pub/Sub",
                      "_key",
                      Base64.getEncoder().encodeToString("12".getBytes()))));
    } else {
      assertThatBigQueryRecords(tableRows)
          .hasRecordsUnordered(
              List.of(
                  Map.of("productId", 11, "productName", "Dataflow"),
                  Map.of("productId", 12, "productName", "Pub/Sub")));
    }
  }

  private void publishSingleSchemaMessages(String topicName)
      throws IOException, RestClientException {
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
  }

  private void publishDoubleSchemaMessages(String topicName)
      throws IOException, RestClientException {
    MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    registryClient.register(topicName + "-value", avroSchema, 1, 3);
    registryClient.register(topicName + "-value", otherAvroSchema, 1, 4);

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

      GenericRecord otherDataflow =
          createOtherRecord(Integer.valueOf(i + "4"), "Dataflow", "dataflow", 0);
      publish(kafkaProducer, topicName, i + "4", otherDataflow);

      GenericRecord otherPubsub =
          createOtherRecord(Integer.valueOf(i + "5"), "Pub/Sub", "pubsub", 0);
      publish(kafkaProducer, topicName, i + "5", otherPubsub);

      GenericRecord otherInvalid =
          createOtherRecord(
              Integer.valueOf(i + "6"), "InvalidNameTooLong", "InvalidNameTooLong", 0);
      publish(kafkaProducer, topicName, i + "6", otherInvalid);

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void publishBinaryMessages(String topicName) throws IOException {
    KafkaProducer<String, GenericRecord> kafkaProducer =
        kafkaResourceManager.buildProducer(
            new StringSerializer(), new BinaryAvroSerializer(avroSchema));

    for (int i = 1; i <= 10; i++) {
      GenericRecord dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 0);
      publishBinary(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 0);
      publishBinary(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid = createRecord(Integer.valueOf(i + "3"), "InvalidNameTooLong", 0);
      publishBinary(kafkaProducer, topicName, i + "3", invalid);

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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

  private void publishBinary(
      KafkaProducer<String, GenericRecord> producer,
      String topicName,
      String key,
      GenericRecord value) {
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

  private GenericRecord createRecord(int id, String productName, double value) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", productName)
        .build();
  }

  private GenericRecord createOtherRecord(int id, String productName, String name, double value) {
    return new GenericRecordBuilder(otherAvroSchema)
        .set("productId", id)
        .set("productName", productName)
        .set("name", name)
        .build();
  }
}
