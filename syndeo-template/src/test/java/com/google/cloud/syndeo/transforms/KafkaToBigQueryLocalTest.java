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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.syndeo.SyndeoTemplate;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Ignore("TODO(pabloem): Ensure this can be tested locally with BQ")
@RunWith(JUnit4.class)
public class KafkaToBigQueryLocalTest {
  @Rule
  public KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  @Rule public final TestName testName = new TestName();

  // TODO(pabloem): Rely on single implementation for row generator.
  private static final Random RND = new Random();

  public static final Schema INTEGRATION_TEST_SCHEMA =
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

  public static Row generateRow() {
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

  private void writeRowsToKafka(Integer numRows) throws Exception {
    SimpleFunction<Row, byte[]> toBytesFn =
        AvroUtils.getRowToAvroBytesFunction(INTEGRATION_TEST_SCHEMA);
    try (KafkaProducer<byte[], byte[]> producer =
        new KafkaProducer<byte[], byte[]>(
            Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG,
                UUID.randomUUID().toString()),
            new ByteArraySerializer(),
            new ByteArraySerializer()); ) {
      for (int i = 0; i < numRows; i++) {
        producer
            .send(
                new ProducerRecord<byte[], byte[]>(
                    KAFKA_TOPIC, new byte[] {(byte) i}, toBytesFn.apply(generateRow())))
            .get();
      }
    }
  }

  public static JsonNode generateConfigurationWithKafkaBootstrap(
      String kafkaBootstrap, String tableName) {
    JsonNode rootConfig = generateBaseRootConfiguration(tableName);
    ((ObjectNode) rootConfig.get("source").get("configurationParameters"))
        .put("bootstrapServers", kafkaBootstrap);
    return rootConfig;
  }

  public static JsonNode generateBaseRootConfiguration(String tableName) {
    if (tableName == null) {
      tableName = "sampletable";
    }
    JsonNode rootConfiguration = JsonNodeFactory.instance.objectNode();
    JsonNode kafkaSourceNode = ((ObjectNode) rootConfiguration).putObject("source");
    ((ObjectNode) kafkaSourceNode).put("urn", "kafka:read");
    JsonNode kafkaConfigParams =
        ((ObjectNode) kafkaSourceNode).putObject("configurationParameters");

    ((ObjectNode) kafkaConfigParams).put("bootstrapServers", "");
    ((ObjectNode) kafkaConfigParams).put("topic", KAFKA_TOPIC);
    ((ObjectNode) kafkaConfigParams).put("dataFormat", "AVRO");
    // TODO(pabloem): Test using Confluent Schema Registry
    ((ObjectNode) kafkaConfigParams).put("avroSchema", AVRO_SCHEMA);
    ((ObjectNode) kafkaConfigParams).put("autoOffsetResetConfig", "earliest");
    ((ObjectNode) kafkaConfigParams).putObject("consumerConfigUpdates");
    ((ObjectNode) kafkaConfigParams.get("consumerConfigUpdates"))
        .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
    ((ObjectNode) kafkaConfigParams.get("consumerConfigUpdates"))
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    JsonNode bqSinkNode = ((ObjectNode) rootConfiguration).putObject("sink");
    ((ObjectNode) bqSinkNode)
        .put("urn", "beam:schematransform:org.apache.beam:bigquery_storage_write:v1");
    JsonNode bqConfigParams = ((ObjectNode) bqSinkNode).putObject("configurationParameters");
    // TODO(pabloem): Test with different formats for tableSpec.
    ((ObjectNode) bqConfigParams).put("table", "anyproject.anydataset." + tableName);
    ((ObjectNode) bqConfigParams).put("writeDisposition", "WRITE_APPEND");
    ((ObjectNode) bqConfigParams).put("createDisposition", "CREATE_IF_NEEDED");
    ((ObjectNode) bqConfigParams).put("useTestingBigQueryServices", true);
    return rootConfiguration;
  }

  @Before
  public void setUpKafka() throws Exception {
    try (AdminClient adminClient =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      Collection<NewTopic> topics =
          Collections.singletonList(new NewTopic(KAFKA_TOPIC, KAFKA_TOPIC_PARTITIONS, (short) 1));
      try {
        adminClient
            .deleteTopics(Collections.singletonList(KAFKA_TOPIC))
            .all()
            .get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Ignore this exception
      }
      adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }
    writeRowsToKafka(6);
  }

  @Test
  public void testSyndeoKafkaToBQWithEmulatorsWithRestarts() throws Exception {
    FakeDatasetService.setUp();
    new FakeDatasetService().createDataset("anyproject", "anydataset", null, null, null);
    PipelineResult result =
        SyndeoTemplate.run(
            new String[] {
              "--jsonSpecPayload="
                  + generateConfigurationWithKafkaBootstrap(
                      kafka.getBootstrapServers(), testName.getMethodName()),
              "--streaming",
              "--experiments=use_deprecated_read",
              // We need to set this option because otherwise the pipeline will block on p.run() and
              // never
              // reach Thread.sleep (and never be cancelled).
              "--blockOnRun=false"
            });

    writeRowsToKafka(6);
    Thread.sleep(30 * 1000); // Wait 30 seconds
    result.cancel();
    writeRowsToKafka(6);
    result =
        SyndeoTemplate.run(
            new String[] {
              "--jsonSpecPayload="
                  + generateConfigurationWithKafkaBootstrap(
                      kafka.getBootstrapServers(), testName.getMethodName()),
              "--streaming",
              "--experiments=use_deprecated_read",
              // We need to set this option because otherwise the pipeline will block on p.run() and
              // never
              // reach Thread.sleep (and never be cancelled).
              "--blockOnRun=false"
            });
    writeRowsToKafka(6);
    Thread.sleep(30 * 1000); // Wait 30 seconds
    result.cancel();

    // We expect 6 rows added on setup and 6 rows added while the pipeline runs, 6 rows added before
    // restart, and
    // 6 rows added after restart.
    assertEquals(
        24,
        new FakeDatasetService()
            .getAllRows("anyproject", "anydataset", testName.getMethodName())
            .size());
  }

  @Test
  public void testSyndeoKafkaToBQWithEmulators() throws Exception {
    FakeDatasetService.setUp();
    new FakeDatasetService().createDataset("anyproject", "anydataset", null, null, null);
    new FakeDatasetService().getDataset("anyproject", "anydataset").get(testName.getMethodName());
    PipelineResult result =
        SyndeoTemplate.run(
            new String[] {
              "--jsonSpecPayload="
                  + generateConfigurationWithKafkaBootstrap(
                      kafka.getBootstrapServers(), testName.getMethodName()),
              "--streaming",
              "--experiments=use_deprecated_read",
              // We need to set this option because otherwise the pipeline will block on p.run() and
              // never
              // reach Thread.sleep (and never be cancelled).
              "--blockOnRun=false"
            });

    writeRowsToKafka(6);
    Thread.sleep(30 * 1000); // Wait 30 seconds
    result.cancel();

    // We expect 6 rows added on setup and 6 rows added while the pipeline runs.
    assertEquals(
        12,
        new FakeDatasetService()
            .getAllRows("anyproject", "anydataset", testName.getMethodName())
            .size());
  }

  @Test
  public void testBasicKafkaRead() throws Exception {
    Pipeline p = Pipeline.create();
    // We need to set this option because otherwise the pipeline will block on p.run()
    // and terminate before reaching writeRowsToKafka.
    p.getOptions().as(DirectOptions.class).setBlockOnRun(false);
    PAssert.that(
            p.apply(
                    KafkaIO.readBytes()
                        .withBootstrapServers(kafka.getBootstrapServers())
                        .withTopic(KAFKA_TOPIC)
                        .withConsumerConfigUpdates(
                            Map.of(
                                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                100,
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"))
                        .withMaxReadTime(Duration.standardSeconds(20))
                        .withoutMetadata())
                .apply(Count.globally()))
        // We expect 6 rows added on setup and 6 rows added while the pipeline runs.
        .containsInAnyOrder(12L);
    PipelineResult result = p.run();
    writeRowsToKafka(6);
    result.waitUntilFinish();
  }
}
