/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
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

/** Integration test for {@link KafkaToBigtableYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(KafkaToBigtableYaml.class)
@RunWith(JUnit4.class)
public final class KafkaToBigtableYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigtableYamlIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setup() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigtableResourceManager);
  }

  @Test
  public void testKafkaToBigtable() throws IOException {
    kafkaToBigtable(Function.identity());
  }

  public void kafkaToBigtable(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting kafkaToBigtable test. Test name: {}. Spec path: {}", testName, specPath);

    /******************************* Arrange ********************************/

    LOG.info("Creating Kafka topic...");
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LOG.info("Creating Bigtable table...");
    String tableId = generateTableId("test_table");
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf1"));

    String bootstrapServers =
        kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");
    LOG.info("Using Kafka Bootstrap Servers: {}", bootstrapServers);

    LOG.info("Creating launch config with yaml pipeline parameters...");
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("bootstrapServers", bootstrapServers)
                .addParameter("topic", topicName)
                .addParameter("format", "JSON")
                .addParameter(
                    "schema",
                    "{\"type\":\"object\",\"properties\":{\"key\":{\"type\":\"string\"},\"type\":{\"type\":\"string\"},\"family_name\":{\"type\":\"string\"},\"column_qualifier\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"},\"timestamp_micros\":{\"type\":\"integer\"}}}")
                .addParameter("windowing", "{\"type\":\"fixed\",\"size\":\"10s\"}")
                .addParameter("tableId", tableId)
                .addParameter("instanceId", bigtableResourceManager.getInstanceId())
                .addParameter("projectId", PROJECT)
                .addParameter("language", "python")
                .addParameter(
                    "fields",
                    "{"
                        + "\"key\": {\"expression\": \"key.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"type\": {\"expression\": \"type\", \"output_type\": \"string\"},"
                        + "\"family_name\": {\"expression\": \"family_name\", \"output_type\": \"string\"},"
                        + "\"column_qualifier\": {\"expression\": \"column_qualifier.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"value\": {\"expression\": \"value.encode('utf-8')\", \"output_type\": \"bytes\"},"
                        + "\"timestamp_micros\": {\"expression\": \"timestamp_micros\", \"output_type\": \"integer\"}"
                        + "}"));

    /********************************* Act **********************************/

    LOG.info("Launching template with options...");
    PipelineLauncher.LaunchInfo info = launchTemplate(options);

    LOG.info("Template launched. LaunchInfo: {}", info);
    assertThatPipeline(info).isRunning();

    LOG.info("Publishing messages into the Kafka topic...");
    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());

    for (int i = 1; i <= 10; i++) {
      long id1 = Long.parseLong(i + "1");
      long id2 = Long.parseLong(i + "2");
      publish(
          kafkaProducer,
          topicName,
          String.valueOf(id1),
          "{\"key\": \"row"
              + id1
              + "\", \"type\": \"SetCell\", \"family_name\": \"cf1\", \"column_qualifier\": \"cq1\", \"value\": \"value1\", \"timestamp_micros\": 5000}");
      publish(
          kafkaProducer,
          topicName,
          String.valueOf(id2),
          "{\"key\": \"row"
              + id2
              + "\", \"type\": \"SetCell\", \"family_name\": \"cf1\", \"column_qualifier\": \"cq2\", \"value\": \"value2\", \"timestamp_micros\": 1000}");
    }

    LOG.info("Waiting for pipeline condition...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  List<Row> rows = bigtableResourceManager.readTable(tableId);
                  if (rows == null) {
                    LOG.warn("bigtableResourceManager.readTable(tableId) returned null. Retrying.");
                    return false;
                  }
                  int tableSize = rows.size();
                  LOG.info("Checking table size. Current size: {}", tableSize);
                  return tableSize == 20;
                });

    /******************************** Assert ********************************/
    assertThatResult(result).meetsConditions();

    LOG.info("Verifying 20 rows in the Bigtable still exist...");
    List<Row> tableRows = bigtableResourceManager.readTable(tableId);
    assertThat(tableRows).hasSize(20);

    LOG.info("Verifying the exact 20 rows in Bigtable...");
    Map<String, Row> rowMap =
        tableRows.stream()
            .collect(Collectors.toMap(row -> row.getKey().toStringUtf8(), row -> row));

    for (int i = 1; i <= 10; i++) {
      String key1 = "row" + i + "1";
      assertThat(rowMap).containsKey(key1);
      Row row1 = rowMap.get(key1);
      assertThat(row1.getCells()).hasSize(1);
      assertThat(row1.getCells().get(0).getFamily()).isEqualTo("cf1");
      assertThat(row1.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo("cq1");
      assertThat(row1.getCells().get(0).getValue().toStringUtf8()).isEqualTo("value1");
      assertThat(row1.getCells().get(0).getTimestamp()).isEqualTo(5000L);

      String key2 = "row" + i + "2";
      assertThat(rowMap).containsKey(key2);
      Row row2 = rowMap.get(key2);
      assertThat(row2.getCells()).hasSize(1);
      assertThat(row2.getCells().get(0).getFamily()).isEqualTo("cf1");
      assertThat(row2.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo("cq2");
      assertThat(row2.getCells().get(0).getValue().toStringUtf8()).isEqualTo("value2");
      assertThat(row2.getCells().get(0).getTimestamp()).isEqualTo(1000L);
    }
  }

  private void publish(
      KafkaProducer<String, String> producer, String topicName, String key, String value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
      LOG.info("Published record with key: {}, value: {}", key, value);
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
