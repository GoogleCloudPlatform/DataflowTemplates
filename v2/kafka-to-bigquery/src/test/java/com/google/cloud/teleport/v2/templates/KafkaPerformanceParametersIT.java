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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
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

/** Integration test for Kafka performance parameters in {@link KafkaToBigQueryFlex}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToBigQueryFlex.class)
@RunWith(JUnit4.class)
@NotThreadSafe
public final class KafkaPerformanceParametersIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPerformanceParametersIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private BigQueryResourceManager bigQueryClient;
  private String bqDatasetId;
  private TableId tableId;
  private Schema bqSchema;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bqDatasetId = bigQueryClient.createDataset(REGION);
    bqSchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("message", StandardSQLTypeName.STRING),
            Field.of("timestamp", StandardSQLTypeName.TIMESTAMP));

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigQueryClient);
  }

  @Test
  public void testKafkaPerformanceParametersWithRedistribution() throws IOException {
    // Test with redistribution enabled and optimized consumer settings
    baseKafkaPerformanceTest(
        builder ->
            builder
                .addParameter("enableKafkaRedistribution", "true")
                .addParameter("kafkaRedistributeNumKeys", "8")
                .addParameter("enableOffsetDeduplication", "true")
                .addParameter("maxPollRecords", "1000")
                .addParameter("fetchMinBytes", "51200")
                .addParameter("fetchMaxWaitMs", "100")
                .addParameter("receiveBufferBytes", "65536")
                .addParameter("sendBufferBytes", "131072"));
  }

  @Test
  public void testKafkaPerformanceParametersWithDuplicates() throws IOException {
    // Test with duplicates allowed for at-least-once semantics
    baseKafkaPerformanceTest(
        builder ->
            builder
                .addParameter("allowDuplicates", "true")
                .addParameter("maxPollRecords", "2000")
                .addParameter("fetchMinBytes", "1024")
                .addParameter("fetchMaxWaitMs", "50"));
  }

  @Test
  public void testKafkaPerformanceParametersLowLatency() throws IOException {
    // Test with low latency configuration
    baseKafkaPerformanceTest(
        builder ->
            builder
                .addParameter("maxPollRecords", "100")
                .addParameter("fetchMinBytes", "1")
                .addParameter("fetchMaxWaitMs", "50")
                .addParameter("receiveBufferBytes", "32768")
                .addParameter("sendBufferBytes", "32768"));
  }

  @Test
  public void testKafkaPerformanceParametersHighThroughput() throws IOException {
    // Test with high throughput configuration
    baseKafkaPerformanceTest(
        builder ->
            builder
                .addParameter("enableKafkaRedistribution", "true")
                .addParameter("kafkaRedistributeNumKeys", "16")
                .addParameter("enableOffsetDeduplication", "false")
                .addParameter("maxPollRecords", "2000")
                .addParameter("fetchMinBytes", "102400")
                .addParameter("fetchMaxWaitMs", "500")
                .addParameter("receiveBufferBytes", "131072")
                .addParameter("sendBufferBytes", "262144"));
  }

  private void baseKafkaPerformanceTest(
      java.util.function.Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Create Kafka topic and BigQuery table
    String topicName = kafkaResourceManager.createTopic(testName, 3);
    tableId = bigQueryClient.createTable(testName, bqSchema);

    // Publish test messages to Kafka
    publishTestMessages(topicName, 100);

    // Launch pipeline with performance parameters
    LaunchConfig.Builder configBuilder =
        LaunchConfig.builder(testName, specPath)
            .addParameter(
                "readBootstrapServerAndTopic",
                kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                    + ";"
                    + topicName)
            .addParameter("outputTableSpec", toTableSpecLegacy(tableId))
            .addParameter("inputFormat", "JSON")
            .addParameter("outputDeadletterTable", toTableSpecLegacy(tableId) + "_dlq")
            .addParameter("kafkaReadAuthenticationMode", "NONE")
            .addParameter("kafkaReadOffset", "earliest");

    LaunchInfo info = launchTemplate(paramsAdder.apply(configBuilder));
    assertThatPipeline(info).isRunning();

    // Wait for messages to be processed
    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, tableId)
                    .setMinRows(50) // Expect at least 50 messages processed
                    .build());

    assertThatResult(result).isLaunchFinished();

    // Verify that messages were processed
    TableResult tableResult = bigQueryClient.readTable(tableId);
    assertThatBigQueryRecords(tableResult).hasRecordsUnordered(List.of());
  }

  private void publishTestMessages(String topicName, int messageCount) throws IOException {
    Map<String, Object> producerProps =
        ImmutableMap.of(
            "bootstrap.servers", kafkaResourceManager.getBootstrapServers(),
            "key.serializer", StringSerializer.class.getName(),
            "value.serializer", StringSerializer.class.getName());

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      for (int i = 0; i < messageCount; i++) {
        String message =
            String.format(
                "{\"id\": %d, \"message\": \"test message %d\", \"timestamp\": \"%s\"}",
                i, i, java.time.Instant.now().toString());

        ProducerRecord<String, String> record =
            new ProducerRecord<>(topicName, "key-" + i, message);

        RecordMetadata metadata = producer.send(record).get();
        LOG.info(
            "Published message {} to partition {} at offset {}",
            i,
            metadata.partition(),
            metadata.offset());
      }
      producer.flush();
    } catch (Exception e) {
      throw new IOException("Failed to publish test messages", e);
    }
  }
}
