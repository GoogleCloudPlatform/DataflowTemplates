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
package com.google.cloud.teleport.templates.yaml;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.iceberg.data.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link KafkaToIcebergYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(KafkaToIcebergYaml.class)
@RunWith(JUnit4.class)
public class KafkaToIcebergYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToIcebergYamlIT.class);

  private KafkaResourceManager kafkaResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "kafka_iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "kafka_iceberg_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    warehouseGcsResourceManager =
        GcsResourceManager.builder(getClass().getSimpleName(), credentials).build();
    warehouseGcsResourceManager.registerTempDir(NAMESPACE);
    LOG.info(
        "Warehouse GCS bucket created successfully: {}", warehouseGcsResourceManager.getBucket());

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        kafkaResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testKafkaToIceberg() throws IOException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(
                "bootstrapServers",
                kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
            .addParameter("topic", topicName)
            .addParameter(
                "schema",
                "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"},\"ignored\":{\"type\":\"string\"}}}")
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("triggeringFrequencySeconds", "5")
            .addParameter("keep", "[\"id\", \"name\"]");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    KafkaProducer<String, String> kafkaProducer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new StringSerializer());

    List<String> expectedNames = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      String json =
          String.format("{\"id\": %d, \"name\": \"name_%d\", \"ignored\": \"value_%d\"}", i, i, i);
      publish(kafkaProducer, topicName, String.valueOf(i), json);
      expectedNames.add("name_" + i);
    }

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  try {
                    List<Record> records = icebergResourceManager.read(ICEBERG_TABLE_IDENTIFIER);
                    LOG.info("Number of records in Iceberg table: {}", records.size());
                    return records.size() >= 5;
                  } catch (Exception e) {
                    return false;
                  }
                });

    // Assert
    assertThatResult(result).meetsConditions();

    // Verify the records are written to the Iceberg table
    List<Record> icebergRecords = icebergResourceManager.read(ICEBERG_TABLE_IDENTIFIER);
    LOG.info("Iceberg records: {}", icebergRecords);
    assertEquals(5, icebergRecords.size());

    // Verify the data correctness
    icebergRecords.sort(Comparator.comparingInt(r -> (Integer) r.getField("id")));
    Record firstRecord = icebergRecords.get(0);
    assertEquals(1, firstRecord.getField("id"));
    assertEquals("name_1", firstRecord.getField("name"));
  }

  private void publish(
      KafkaProducer<String, String> producer, String topicName, String key, String value) {
    try {
      producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info("Published record to Kafka: with key {} and value {}", key, value);
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type",
        "rest",
        "uri",
        "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
        "warehouse",
        "gs://" + warehouseGcsResourceManager.getBucket(),
        "header.x-goog-user-project",
        PROJECT,
        "rest.auth.type",
        "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled",
        "false");
  }
}
