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
package com.google.cloud.syndeo.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.syndeo.SyndeoTemplate;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class KafkaToBigQueryLocalTest {
  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
          .withEmbeddedZookeeper();

  // TODO(pabloem): Rely on single implementation for row generator.
  private static final Random RND = new Random();

  private static final Schema INTEGRATION_TEST_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addBooleanField("vaccinated")
          .addDoubleField("temperature")
          // The following fields cannot be included in local tests
          // due to limitations of the testing utilities.
          .addInt32Field("age")
          .addInt64Field("networth")
          .addDateTimeField("birthday")
          .build();

  public static String randomString(Integer length) {
    byte[] byteStr = new byte[length];
    RND.nextBytes(byteStr);
    return Base64.getEncoder().encodeToString(byteStr);
  }

  private static Row generateRow() {
    return Row.withSchema(INTEGRATION_TEST_SCHEMA)
        .addValue(randomString(10))
        .addValue(RND.nextBoolean())
        .addValue(RND.nextDouble())
        .addValue(RND.nextInt())
        .addValue(RND.nextLong())
        .addValue(new DateTime(Math.abs(RND.nextLong()) % Instant.now().toEpochMilli()))
        .build();
  }

  private static final String KAFKA_TOPIC = "sampleTopic" + UUID.randomUUID();
  // TODO(pabloem): add more kafka partitions to test a more realistic scenario.
  private static final Integer KAFKA_TOPIC_PARTITIONS = 1;
  private static final String AVRO_SCHEMA =
      AvroUtils.toAvroSchema(INTEGRATION_TEST_SCHEMA).toString();

  @Before
  public void setUpKafka() throws Exception {
    try (AdminClient adminClient =
            AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
        KafkaProducer<byte[], byte[]> producer =
            new KafkaProducer<byte[], byte[]>(
                Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    kafka.getBootstrapServers(),
                    ProducerConfig.CLIENT_ID_CONFIG,
                    UUID.randomUUID().toString()),
                new ByteArraySerializer(),
                new ByteArraySerializer()); ) {
      Collection<NewTopic> topics =
          Collections.singletonList(new NewTopic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, (short) 1));
      adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

      SimpleFunction<Row, byte[]> toBytesFn =
          AvroUtils.getRowToAvroBytesFunction(INTEGRATION_TEST_SCHEMA);
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {0}, toBytesFn.apply(generateRow())))
          .get();
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {1}, toBytesFn.apply(generateRow())))
          .get();
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {2}, toBytesFn.apply(generateRow())))
          .get();
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {3}, toBytesFn.apply(generateRow())))
          .get();
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {4}, toBytesFn.apply(generateRow())))
          .get();
      producer
          .send(
              new ProducerRecord<byte[], byte[]>(
                  KAFKA_TOPIC, new byte[] {5}, toBytesFn.apply(generateRow())))
          .get();
    }
  }

  @Test
  public void testSyndeoKafkaToBQWithEmulators() throws Exception {
    JsonNode rootConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode kafkaSourceNode = ((ObjectNode) rootConfiguration).putObject("source");
    ((ObjectNode) kafkaSourceNode).put("urn", "kafka:read");
    JsonNode kafkaConfigParams =
        ((ObjectNode) kafkaSourceNode).putObject("configurationParameters");

    ((ObjectNode) kafkaConfigParams).put("bootstrapServers", kafka.getBootstrapServers());
    ((ObjectNode) kafkaConfigParams).put("topic", KAFKA_TOPIC);
    ((ObjectNode) kafkaConfigParams).put("dataFormat", "AVRO");
    ((ObjectNode) kafkaConfigParams).put("avroSchema", AVRO_SCHEMA);

    FakeDatasetService.setUp();
    new FakeDatasetService().createDataset("anyproject", "anydataset", null, null, null);
    JsonNode bqSinkNode = ((ObjectNode) rootConfiguration).putObject("sink");
    ((ObjectNode) bqSinkNode).put("urn", "schemaIO:bigquery:write");
    JsonNode bqConfigParams = ((ObjectNode) bqSinkNode).putObject("configurationParameters");
    // TODO(pabloem): Test with different formats for tableSpec.
    ((ObjectNode) bqConfigParams).put("table", "anyproject:anydataset.sampletable");
    ((ObjectNode) bqConfigParams).put("useTestingBigQueryServices", true);

    SyndeoTemplate.main(
        new String[] {
          "--jsonSpecPayload=" + rootConfiguration,
          "--streaming",
          "--experiments=use_deprecated_read"
        });
  }

  static class MyPrintingDoFn extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {
    @ProcessElement
    public void process(
        @DoFn.Element KV<byte[], byte[]> elm, OutputReceiver<KV<byte[], byte[]>> receiver) {
      System.out.println("ELEMENT: " + elm.toString());
      receiver.output(elm);
    }
  }

  @Test
  public void testBasicKafkaRead() {

    Pipeline p = Pipeline.create();
    PAssert.that(
            p.apply(
                    KafkaIO.readBytes()
                        .withBootstrapServers(kafka.getBootstrapServers())
                        .withTopic(KAFKA_TOPIC)
                        .withMaxReadTime(Duration.standardSeconds(30))
                        .withoutMetadata())
                .apply(ParDo.of(new MyPrintingDoFn()))
                .apply(Count.globally()))
        .containsInAnyOrder(5L);
    p.run().waitUntilFinish();
  }
}
