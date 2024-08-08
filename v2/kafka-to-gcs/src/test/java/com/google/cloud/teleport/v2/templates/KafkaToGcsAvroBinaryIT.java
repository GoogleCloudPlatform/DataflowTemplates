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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.common.io.Resources;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.storage.conditions.GCSArtifactsCheck;
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

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToGcsFlex.class)
@RunWith(JUnit4.class)
public class KafkaToGcsAvroBinaryIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToGcsAvroBinaryIT.class);
  private static final Pattern RESULT_REGEX = Pattern.compile(".*\\.avro$");

  private KafkaResourceManager kafkaResourceManager;
  private Schema avroSchema;
  private String prefix;
  private String recordClassName = "AvroProductKafkaRecord";

  @Before
  public void setup() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    URL avroschemaResource = Resources.getResource("KafkaToGcsAvroBinaryIT/avro_schema.avsc");
    gcsClient.uploadArtifact("avro_schema.avsc", avroschemaResource.getPath());
    avroSchema = new Schema.Parser().parse(avroschemaResource.openStream());
    prefix = testName + '-';
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, gcsClient);
  }

  @Test
  public void testKafkaToGcsBinaryEncoding() throws IOException {
    baseKafkaToGcs(
        b ->
            b.addParameter(
                    "messageFormat",
                    KafkaTemplateParameters.MessageFormatConstants.AVRO_BINARY_ENCODING)
                .addParameter("binaryAvroSchemaPath", getGcsPath("avro_schema.avsc")));
  }

  private void baseKafkaToGcs(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("windowDuration", "10s")
                .addParameter("kafkaReadOffset", "earliest")
                .addParameter("outputDirectory", getGcsBasePath())
                .addParameter("outputFilenamePrefix", prefix)
                .addParameter("useBigQueryDLQ", "false")
                .addParameter("numShards", "2")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    KafkaProducer<String, byte[]> producer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new ByteArraySerializer());

    // Create GenericRecord
    for (int i = 0; i < 10; i++) {
      GenericRecord record = createRecord(i, "Kafka templates", i);
      publish(producer, topicName, String.valueOf(i), convertGenericRecordToBytes(record));
    }

    List<ConditionCheck> conditions = new ArrayList<ConditionCheck>();
    ConditionCheck gcsConditionCheck =
        GCSArtifactsCheck.builder(gcsClient, recordClassName, RESULT_REGEX)
            .setMinSize(2)
            .setMaxSize(10)
            .build();
    conditions.add(gcsConditionCheck);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info), conditions.toArray(new ConditionCheck[0]));
    // Assert
    assertThatResult(result).meetsConditions();
  }

  private GenericRecord createRecord(int id, String productName, double value) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", productName)
        .build();
  }

  public byte[] convertGenericRecordToBytes(GenericRecord record) {
    try {
      GenericDatumWriter<GenericRecord> userDatumWriter =
          new GenericDatumWriter<>(record.getSchema());
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      userDatumWriter.write(record, encoder);
      encoder.flush();
      byte[] serializedBytes = outputStream.toByteArray();
      outputStream.close();
      return serializedBytes;
    } catch (Exception e) {
      throw new RuntimeException("Error serializing Avro message to bytes");
    }
  }

  private void publish(
      KafkaProducer<String, byte[]> producer, String topicName, String key, byte[] value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
