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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroSerializer;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
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
import org.apache.avro.generic.GenericData;
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
  private org.apache.avro.Schema avroSchemaUsageEnum;
  private org.apache.avro.Schema otherAvroSchema;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bqDatasetId = bigQueryClient.createDataset(REGION);
    bqSchema =
        Schema.of(
            Field.of("productId", StandardSQLTypeName.INT64),
            Field.newBuilder("productName", StandardSQLTypeName.STRING).setMaxLength(10L).build(),
            Field.of("productSize", StandardSQLTypeName.FLOAT64),
            Field.of("productUsage", StandardSQLTypeName.STRING));

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();

    URL avroSchemaResource = Resources.getResource("KafkaToBigQueryFlexAvroIT/avro_schema.avsc");
    gcsClient.uploadArtifact("avro_schema.avsc", avroSchemaResource.getPath());
    avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaResource.openStream());
    avroSchemaUsageEnum = avroSchema.getField("productUsage").schema();

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
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
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
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE")
                .addParameter("persistKafkaKey", "true"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithSchemaRegistry() throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SCHEMA_REGISTRY")
                // If this test fails, check if the below schema registry has
                // correct schemas registered with the following IDs:
                // - 5 (avro_schema.avsc)
                // - 4 (other_avro_schema.avsc)
                .addParameter("schemaRegistryConnectionUrl", "http://10.128.0.60:8081")
                .addParameter("writeMode", "DYNAMIC_TABLE_NAMES")
                .addParameter("outputProject", PROJECT)
                .addParameter("outputDataset", bqDatasetId)
                .addParameter("bqTableNamePrefix", "")
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithSchemaRegistryWithKey()
      throws IOException, RestClientException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SCHEMA_REGISTRY")
                // If this test fails, check if the below schema registry has
                // correct schemas registered with the following IDs:
                // - 5 (avro_schema.avsc)
                // - 4 (other_avro_schema.avsc)
                .addParameter("schemaRegistryConnectionUrl", "http://10.128.0.60:8081")
                .addParameter("writeMode", "DYNAMIC_TABLE_NAMES")
                .addParameter("outputProject", PROJECT)
                .addParameter("outputDataset", bqDatasetId)
                .addParameter("bqTableNamePrefix", "")
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE")
                .addParameter("persistKafkaKey", "true"));
  }

  @Test
  public void testKafkaToBigQueryAvroInNonConfluentFormat()
      throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("messageFormat", "AVRO_BINARY_ENCODING")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("binaryAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  @Test
  public void testKafkaToBigQueryAvroInNonConfluentFormatWithDLQ()
      throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = TableId.of(bigQueryClient.getDatasetId(), testName + "_dlq");
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("messageFormat", "AVRO_BINARY_ENCODING")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("binaryAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "true")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
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
            b.addParameter("messageFormat", "AVRO_BINARY_ENCODING")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("binaryAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("kafkaReadAuthenticationMode", "NONE")
                .addParameter("persistKafkaKey", "true"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithExistingDLQ() throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = TableId.of(bigQueryClient.getDatasetId(), testName + "_dlq");

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "true")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  @Test
  public void testKafkaToBigQueryAvroSerializationErrorWithDLQ()
      throws IOException, RestClientException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = TableId.of(bigQueryClient.getDatasetId(), testName + "_dlq");

    baseKafkaToBigQueryAvroSerializationErrorWithDLQ(
        b ->
            b.addParameter("messageFormat", "AVRO_CONFLUENT_WIRE_FORMAT")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "true")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("kafkaReadAuthenticationMode", "NONE"));
  }

  private void baseKafkaToBigQueryAvroSerializationErrorWithDLQ(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, RestClientException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("kafkaReadOffset", "earliest"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<ConditionCheck> conditions = new ArrayList<ConditionCheck>();

    // Publish messages with IDs 1 and 2.
    publishDoubleSchemaMessages(topicName, 1, 2);

    // For dead letter queue test, pass a single schema file as schema source and
    // use CONFLUENT_WIRE_FORMAT as message format. This expects all the messages in the single
    // topic
    // to contain the schema ID to be 1. The messages with schema id 2 will fail to deserialize and
    // will go
    // into dead letter queue.

    // 20 elements in expected success table. 40 messages will be in the dead letter queue table.
    conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build());
    conditions.add(
        BigQueryRowsCheck.builder(bigQueryClient, deadletterTableId).setMinRows(40).build());

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info), conditions.toArray(new ConditionCheck[0]));
    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testKafkaToBigQueryBinarySerializationErrorWithDLQ() throws IOException {
    tableId = bigQueryClient.createTable(testName, bqSchema);
    deadletterTableId = TableId.of(bigQueryClient.getDatasetId(), testName + "_dlq");

    baseKafkaToBigQueryBinarySerializationWithDLQ(
        b ->
            b.addParameter(
                    "messageFormat",
                    KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
                .addParameter(
                    "schemaFormat", KafkaTemplateParameters.SchemaFormat.SINGLE_SCHEMA_FILE)
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("writeMode", "SINGLE_TABLE_NAME")
                .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
                .addParameter("useBigQueryDLQ", "true")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId))
                .addParameter("kafkaReadAuthenticationMode", "NONE"),
        0,
        30);
  }

  private void baseKafkaToBigQueryBinarySerializationWithDLQ(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder,
      int successRowCount,
      int failRowCount)
      throws IOException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("kafkaReadOffset", "earliest"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<ConditionCheck> conditions = new ArrayList<ConditionCheck>();

    // Publishing the binary messages.
    publishBinaryMessages(topicName);

    // Reading the AVRO_CONFLUENT_WIRE_FORMAT messages. All the messages should go into DLQ since
    // confluent wire format can't deserialize binary format messages since it requires magic byte
    // and schema id in the byte payload.
    conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(0).build());
    conditions.add(
        BigQueryRowsCheck.builder(bigQueryClient, deadletterTableId).setMinRows(30).build());

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info), conditions.toArray(new ConditionCheck[0]));
    // Assert
    assertThatResult(result).meetsConditions();
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
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("kafkaReadOffset", "earliest"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<ConditionCheck> conditions = new ArrayList<ConditionCheck>();

    if (options.getParameter("messageFormat") != null
        && options.getParameter("messageFormat").equals("AVRO_CONFLUENT_WIRE_FORMAT")
        && options.getParameter("schemaFormat") != null
        && options.getParameter("schemaFormat").equals("SCHEMA_REGISTRY")
        && options.getParameter("schemaRegistryConnectionUrl") != null) {

      // Schemas are registered with ids 5 (avro_schema.avsc) and 4 (other_avro_schema.avsc).
      publishDoubleSchemaMessages(topicName, 5, 4);
      tableId = TableId.of(bqDatasetId, avroSchema.getFullName().replace(".", "-"));
      TableId otherTableId =
          TableId.of(bqDatasetId, otherAvroSchema.getFullName().replace(".", "-"));

      conditions.add(BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(20).build());
      conditions.add(
          BigQueryRowsCheck.builder(bigQueryClient, otherTableId).setMinRows(20).build());

    } else if (options.getParameter("messageFormat") != null
        && options.getParameter("messageFormat").equals("AVRO_BINARY_ENCODING")
        && options.getParameter("binaryAvroSchemaPath") != null) {

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
                      "productSize",
                      2.5d,
                      "productUsage",
                      "HIGH",
                      "_key",
                      Base64.getEncoder().encodeToString("11".getBytes())),
                  Map.of(
                      "productId",
                      12,
                      "productName",
                      "Pub/Sub",
                      "productSize",
                      123.125d,
                      "productUsage",
                      "MEDIUM",
                      "_key",
                      Base64.getEncoder().encodeToString("12".getBytes()))));
    } else {
      assertThatBigQueryRecords(tableRows)
          .hasRecordsUnordered(
              List.of(
                  Map.of(
                      "productId",
                      11,
                      "productName",
                      "Dataflow",
                      "productSize",
                      2.5d,
                      "productUsage",
                      "HIGH"),
                  Map.of(
                      "productId",
                      12,
                      "productName",
                      "Pub/Sub",
                      "productSize",
                      123.125d,
                      "productUsage",
                      "MEDIUM")));
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
      GenericRecord dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 2.5f, "HIGH");
      publish(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 123.125f, "MEDIUM");
      publish(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid =
          createRecord(Integer.valueOf(i + "3"), "InvalidNameTooLong", 0f, "UNDEFINED");
      publish(kafkaProducer, topicName, i + "3", invalid);

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void publishDoubleSchemaMessages(String topicName, int schemaId1, int schemaId2)
      throws IOException, RestClientException {
    MockSchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    registryClient.register(topicName + "-value", avroSchema, 1, schemaId1);
    registryClient.register(topicName + "-value", otherAvroSchema, 1, schemaId2);

    KafkaProducer<String, Object> kafkaProducer =
        kafkaResourceManager.buildProducer(
            new StringSerializer(), new KafkaAvroSerializer(registryClient));

    for (int i = 1; i <= 10; i++) {
      GenericRecord dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 2.5f, "HIGH");
      publish(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 123.125f, "MEDIUM");
      publish(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid =
          createRecord(Integer.valueOf(i + "3"), "InvalidNameTooLong", 0f, "UNDEFINED");
      publish(kafkaProducer, topicName, i + "3", invalid);

      GenericRecord otherDataflow =
          createOtherRecord(Integer.valueOf(i + "4"), "Dataflow", "dataflow");
      publish(kafkaProducer, topicName, i + "4", otherDataflow);

      GenericRecord otherPubsub = createOtherRecord(Integer.valueOf(i + "5"), "Pub/Sub", "pubsub");
      publish(kafkaProducer, topicName, i + "5", otherPubsub);

      GenericRecord otherInvalid =
          createOtherRecord(Integer.valueOf(i + "6"), "InvalidNameTooLong", "InvalidNameTooLong");
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
      GenericRecord dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 2.5f, "HIGH");
      publishBinary(kafkaProducer, topicName, i + "1", dataflow);

      GenericRecord pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 123.125f, "MEDIUM");
      publishBinary(kafkaProducer, topicName, i + "2", pubsub);

      GenericRecord invalid =
          createRecord(Integer.valueOf(i + "3"), "InvalidNameTooLong", 0f, "UNDEFINED");
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

  private GenericRecord createRecord(
      int id, String productName, float productSize, String productUsage) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", productName)
        .set("productSize", productSize)
        .set("productUsage", new GenericData.EnumSymbol(avroSchemaUsageEnum, productUsage))
        .build();
  }

  private GenericRecord createOtherRecord(int id, String productName, String name) {
    return new GenericRecordBuilder(otherAvroSchema)
        .set("productId", id)
        .set("productName", productName)
        .set("name", name)
        .build();
  }
}
