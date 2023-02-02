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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.TemplateTestBase.toTableSpec;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for testing performance of {@link PubSubToBigQuery} template. */
@TemplateIntegrationTest(PubSubToBigQuery.class)
@RunWith(JUnit4.class)
public class PubsubToBigQueryPerformanceIT extends PerformanceBenchmarkingBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          "gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery",
          TestProperties.specPath());
  private static final long numMessages = 35000000L;
  private static final String INPUT_PCOLLECTION =
      "ReadPubSubSubscription/PubsubUnboundedSource.out0";
  private static final String OUTPUT_PCOLLECTION =
      "WriteSuccessfulRecords/StreamingInserts/StreamingWriteTables/StripShardId/Map.out0";
  private static PubsubResourceManager pubsubResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(CREDENTIALS)
            .build();
  }

  @After
  public void teardown() {
    pubsubResourceManager.cleanupAll();
    bigQueryResourceManager.cleanupAll();
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    // Generate fake data to topic
    String jobName = createJobName(testName.getMethodName());
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    // schema should match schema supplied to generate fake records.
    Schema schema =
        Schema.of(
            Field.of("eventId", StandardSQLTypeName.STRING),
            Field.of("eventTimestamp", StandardSQLTypeName.INT64),
            Field.of("ipv4", StandardSQLTypeName.STRING),
            Field.of("ipv6", StandardSQLTypeName.STRING),
            Field.of("country", StandardSQLTypeName.STRING),
            Field.of("username", StandardSQLTypeName.STRING),
            Field.of("quest", StandardSQLTypeName.STRING),
            Field.of("score", StandardSQLTypeName.INT64),
            Field.of("completed", StandardSQLTypeName.BOOL));
    TableId table = bigQueryResourceManager.createTable(jobName, schema);
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(numMessages))
            .setTopic(backlogTopic.toString())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("inputSubscription", backlogSubscription.toString())
            .addParameter("outputTableSpec", toTableSpec(table))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(40)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, numMessages));

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(bigQueryResourceManager.readTable(jobName).getTotalRows()).isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  @Test
  public void testSteadyState1hr() throws ParseException, IOException, InterruptedException {
    // Generate fake data to topic
    String jobName = createJobName(testName.getMethodName());
    TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
    // schema should match schema supplied to generate fake records.
    Schema schema =
        Schema.of(
            Field.of("eventId", StandardSQLTypeName.STRING),
            Field.of("eventTimestamp", StandardSQLTypeName.INT64),
            Field.of("ipv4", StandardSQLTypeName.STRING),
            Field.of("ipv6", StandardSQLTypeName.STRING),
            Field.of("country", StandardSQLTypeName.STRING),
            Field.of("username", StandardSQLTypeName.STRING),
            Field.of("quest", StandardSQLTypeName.STRING),
            Field.of("score", StandardSQLTypeName.INT64),
            Field.of("completed", StandardSQLTypeName.BOOL));
    TableId table =
        bigQueryResourceManager.createTable(
            jobName, schema, System.currentTimeMillis() + 7200000); // expire in 2 hrs
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("100000")
            .setTopic(inputTopic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("inputSubscription", inputSubscription.toString())
            .addParameter("outputTableSpec", toTableSpec(table))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    dataGenerator.execute(Duration.ofMinutes(60));
    // Validate that the template executed as expected
    Result result = pipelineOperator.drainJobAndFinish(createConfig(info, Duration.ofMinutes(20)));
    // Assert
    assertThat(result).isEqualTo(Result.LAUNCH_FINISHED);
    assertThat(bigQueryResourceManager.readTable(jobName, 5).getTotalRows()).isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  private Map<String, Double> computeMetrics(LaunchInfo info)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION);
    Map<String, Double> outputThroughput =
        getThroughputMetricsOfPcollection(info, OUTPUT_PCOLLECTION);
    Map<String, Double> inputThroughput =
        getThroughputMetricsOfPcollection(info, INPUT_PCOLLECTION);
    metrics.put("AvgInputThroughput", inputThroughput.get("AvgThroughput"));
    metrics.put("MaxInputThroughput", inputThroughput.get("MaxThroughput"));
    metrics.put("AvgOutputThroughput", outputThroughput.get("AvgThroughput"));
    metrics.put("MaxOutputThroughput", outputThroughput.get("MaxThroughput"));
    metrics.putAll(getCpuUtilizationMetrics(info));
    metrics.putAll(getDataFreshnessMetrics(info));
    metrics.putAll(getSystemLatencyMetrics(info));
    return metrics;
  }
}
