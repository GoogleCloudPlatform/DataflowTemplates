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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
  public void testKafkaToBigQueryAvro() throws IOException {
    baseKafkaToBigQueryAvro(Function.identity()); // no extra parameters
  }

  @Test
  public void testKafkaToBigQueryAvroWithExistingDLQ() throws IOException {
    TableId deadletterTableId =
        bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b -> b.addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId)));
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApi() throws IOException {
    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3"));
  }

  @Test
  public void testKafkaToBigQueryAvroWithStorageApiExistingDLQ() throws IOException {
    TableId deadletterTableId =
        bigQueryClient.createTable(testName + "_dlq", getDeadletterSchema());

    baseKafkaToBigQueryAvro(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "3")
                .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(deadletterTableId)));
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
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    String bqTable = testName;
    Schema bqSchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("value", StandardSQLTypeName.NUMERIC));

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
    KafkaProducer<String, byte[]> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new ByteArraySerializer());

    for (int i = 1; i <= 10; i++) {
      ByteString dataflow = createRecord(Integer.valueOf(i + "1"), "Dataflow", 10.5);
      publish(kafkaProducer, topicName, i + "1", dataflow);

      ByteString pubsub = createRecord(Integer.valueOf(i + "2"), "Pub/Sub", 0);
      publish(kafkaProducer, topicName, i + "2", pubsub);

      ByteString invalid = ByteString.copyFrom(new byte[8]);
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
    assertThatBigQueryRecords(tableRows)
        .hasRecordsUnordered(
            List.of(
                Map.of("id", 11, "name", "Dataflow", "value", 10.5),
                Map.of("id", 12, "name", "Pub/Sub")));
  }

  private void publish(
      KafkaProducer<String, byte[]> producer, String topicName, String key, ByteString value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value.toByteArray())).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }

  private ByteString createRecord(int id, String name, double value) throws IOException {
    GenericRecord record =
        new GenericRecordBuilder(avroSchema)
            .set("id", id)
            .set("name", name)
            .set(
                "decimal",
                ByteBuffer.wrap(
                    new BigDecimal(value, MathContext.DECIMAL32)
                        .setScale(17, RoundingMode.HALF_UP)
                        .unscaledValue()
                        .toByteArray()))
            .build();

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(record, encoder);
    encoder.flush();

    return ByteString.copyFrom(output.toByteArray());
  }
}
