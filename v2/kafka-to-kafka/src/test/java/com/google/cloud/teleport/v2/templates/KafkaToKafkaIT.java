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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
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
@TemplateIntegrationTest(KafkaToKafka.class)
@RunWith(JUnit4.class)
public final class KafkaToKafkaIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);
  private KafkaResourceManager kafkaResourceManager;

  @Before
  public void setup() throws IOException {

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager);
  }

  @Test
  public void testKafkaToKafka() throws IOException {
    kafkaToKafka(Function.identity());
  }

  public void kafkaToKafka(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    String inputTopicName = kafkaResourceManager.createTopic(testName, 5);
    String outputTopicName = kafkaResourceManager.createTopic(testName, 5);
    KafkaConsumer<String, String> consumer =
        kafkaResourceManager.buildConsumer(new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(Collections.singleton(outputTopicName));
    LOG.info("Created Kafka Consumer");
    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "sourceBootstrapServers",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
                .addParameter("inputTopic", inputTopicName)
                .addParameter("outputTopic", outputTopicName)
                .addParameter(
                    "destinationBootstrapServer",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")));

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());
    List<String> inMessages = new ArrayList<>();
    inMessages.add("test-message-1");
    inMessages.add("test-message-2");

    List<String> outMessages = new ArrayList<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  for (String message : inMessages) {
                    publish(kafkaProducer, inputTopicName, message);
                  }
                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                  records.forEach(record -> outMessages.add(record.value()));

                  return outMessages.containsAll(inMessages);
                });
    assertThatResult(result).meetsConditions();
    assertThat(outMessages).containsExactlyElementsIn(inMessages);
  }

  private void publish(KafkaProducer<String, String> producer, String topicName, String message) {
    try {
      RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topicName, message)).get();
      LOG.info(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
