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

import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.gcp.TemplateTestBase.toTableSpecLegacy;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link KafkaToBigQuery Kafka to BigQuery} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(KafkaToBigQuery.class)
@RunWith(JUnit4.class)
public class KafkaToBigQueryLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Kafka_to_BigQuery");

  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final int NUM_MESSAGES = 35_000_000;
  // schema should match schema supplied to generate fake records.
  private static final Schema SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.INT64),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL),
          // add a insert timestamp column to query latency values
          Field.newBuilder("_metadata_insert_timestamp", StandardSQLTypeName.TIMESTAMP)
              .setDefaultValueExpression("CURRENT_TIMESTAMP()")
              .build());

  private static final String INPUT_PCOLLECTION =
      "ReadFromKafka/KafkaIO.Read/KafkaIO.Read.ReadFromKafkaViaSDF/Read(KafkaUnboundedSource)/StripIds.out0";
  private static final String OUTPUT_PCOLLECTION =
      "WriteSuccessfulRecords/StorageApiLoads/AddShard.out0";

  private static BigQueryResourceManager bigQueryResourceManager;
  private static KafkaResourceManager kafkaResourceManager;

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::enableRunnerV2);
  }

  @Test
  public void testSteadyState1hr() throws ParseException, IOException, InterruptedException {
    testSteadyState1hr(this::enableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws ParseException, IOException, InterruptedException {
    testSteadyState1hr(this::enableStreamingEngine);
  }

  public void testBacklog(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, ParseException, InterruptedException {
    String topicName = kafkaResourceManager.createTopic(testName, 5);
    String bootstrapServer = kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");
    // Generate fake data to topic
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setKafkaTopic(topicName)
            .setBootstrapServer(bootstrapServer)
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .setSinkType("KAFKA")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    TableId table = bigQueryResourceManager.createTable(testName, SCHEMA); // expire in 2 hrs
    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 5)
                    .addEnvironment("numWorkers", 4)
                    .addParameter("useStorageWriteApi", "true")
                    .addParameter("numStorageWriteApiStreams", "3")
                    .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                    .addParameter("bootstrapServers", bootstrapServer)
                    .addParameter("inputTopics", topicName)
                    .addParameter("outputTableSpec", toTableSpecLegacy(table))
                    .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED"))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(40)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(NUM_MESSAGES)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  public void testSteadyState1hr(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws ParseException, IOException, InterruptedException {
    // Arrange
    String qps = getProperty("qps", "100000", TestProperties.Type.PROPERTY);
    String topicName = kafkaResourceManager.createTopic(testName, 5);
    String bootstrapServer = kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "");

    TableId tableId =
        bigQueryResourceManager.createTable(
            testName, SCHEMA, System.currentTimeMillis() + 7200000); // expire in 2 hrs
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS(qps)
            .setKafkaTopic(topicName)
            .setBootstrapServer(bootstrapServer)
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .setSinkType("KAFKA")
            .build();

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 7)
                    .addParameter("useStorageWriteApi", "true")
                    .addParameter("numStorageWriteApiStreams", "3")
                    .addParameter("storageWriteApiTriggeringFrequencySec", "3")
                    .addEnvironment("additionalUserLabels", Collections.singletonMap("qps", qps))
                    .addParameter("bootstrapServers", bootstrapServer)
                    .addParameter("inputTopics", topicName)
                    .addParameter("outputTableSpec", toTableSpecLegacy(tableId)))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    Integer expectedMessages = (int) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(20)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, tableId)
                .setMinRows(expectedMessages)
                .build());
    // Assert
    assertThatResult(result).meetsConditions();

    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION);
    // Query end to end latency metrics from BigQuery
    TableResult latencyResult =
        bigQueryResourceManager.runQuery(
            String.format(
                "WITH difference AS (SELECT\n"
                    + "    TIMESTAMP_DIFF(_metadata_insert_timestamp,\n"
                    + "    TIMESTAMP_MILLIS(eventTimestamp), SECOND) AS latency,\n"
                    + "    FROM %s.%s)\n"
                    + "    SELECT\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.5) OVER () AS median,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.9) OVER () as percentile_90,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.95) OVER () as percentile_95,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.99) OVER () as percentile_99\n"
                    + "    FROM difference LIMIT 1",
                bigQueryResourceManager.getDatasetId(), testName));

    FieldValueList latencyValues = latencyResult.getValues().iterator().next();
    metrics.put("median_latency", latencyValues.get(0).getDoubleValue());
    metrics.put("percentile_90_latency", latencyValues.get(1).getDoubleValue());
    metrics.put("percentile_95_latency", latencyValues.get(2).getDoubleValue());
    metrics.put("percentile_99_latency", latencyValues.get(3).getDoubleValue());

    // export results
    exportMetricsToBigQuery(info, metrics);
  }
}
