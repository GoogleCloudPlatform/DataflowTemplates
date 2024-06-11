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
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
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

/** Integration test for {@link KafkaToGcsFlex} (Kafka_to_GCS_2). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToGcsFlex.class)
@RunWith(JUnit4.class)
public class KafkaToGcsIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToGcsIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private Schema avroSchema;

  @Before
  public void setup() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();

    URL avroschemaResource = Resources.getResource("KafkaToGcsIT/avro_schema.avsc");
    gcsClient.uploadArtifact("avro_schema.avsc", avroschemaResource.getPath());
    avroSchema = new Schema.Parser().parse(avroschemaResource.openStream());
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager);
  }

  @Test
  public void testKafkaToGcsText() throws IOException, RestClientException {
    baseKafkaToGcs(b -> b.addParameter("messageFormat", MessageFormatConstants.JSON));
  }

  @Test
  public void testKafkaToGcsAvro() throws IOException, RestClientException {
    baseKafkaToGcs(
        b -> b.addParameter("messageFormat", MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT));
  }

  private void baseKafkaToGcs(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
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
                .addParameter("windowDuration", "10s")
                .addParameter("schemaFormat", "SINGLE_SCHEMA_FILE")
                .addParameter("confluentAvroSchemaPath", getGcsPath("avro_schema.avsc"))
                .addParameter("kafkaReadOffset", "earliest")
                .addParameter("outputDirectory", getGcsPath(testName))
                .addParameter("outputFilenamePrefix", testName + "-")
                .addParameter("numShards", "2")
                // TODO: Move these to separate tests once they are added
                .addParameter("kafkaReadAuthenticationMode", "NONE")
                .addParameter("kafkaReadUsernameSecretId", "")
                .addParameter("kafkaReadPasswordSecretId", ""));

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

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Pattern expectedFilePattern = Pattern.compile(".*" + testName + "-.*");

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  artifacts.set(gcsClient.listArtifacts(testName, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();
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

  private GenericRecord createRecord(int id, String productName, double value) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", productName)
        .build();
  }
}
