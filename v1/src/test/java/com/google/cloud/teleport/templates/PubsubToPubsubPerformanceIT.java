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
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
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

/** Performance test for testing performance of {@link PubsubToPubsub} template. */
@TemplateIntegrationTest(PubsubToPubsub.class)
@RunWith(JUnit4.class)
public class PubsubToPubsubPerformanceIT extends PerformanceBenchmarkingBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          "gs://dataflow-templates/latest/Cloud_PubSub_to_Cloud_PubSub", TestProperties.specPath());
  private static final long numMessages = 35000000L;
  private static final String INPUT_PCOLLECTION = "Read PubSub Events/PubsubUnboundedSource.out0";
  private static final String OUTPUT_PCOLLECTION = "Write PubSub Events/MapElements/Map.out0";
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
  }

  @After
  public void teardown() {
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    // Generate fake data to topic
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName backlogTopic = pubsubResourceManager.createTopic("backlog-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "backlog-subscription");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(backlogTopic, "output-subscription");
    TopicName outputTopic = pubsubResourceManager.createTopic("output");
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
            .addParameter("outputTopic", outputTopic.toString())
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(60)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, numMessages));

    // Assert
    assertThatResult(result).meetsConditions();
    // check to see if messages reached the output topic
    assertThat(pubsubResourceManager.pull(outputSubscription, 5).getReceivedMessagesCount())
        .isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName inputTopic = pubsubResourceManager.createTopic("steady-state-input");
    SubscriptionName backlogSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "steady-state-subscription");
    TopicName outputTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "output-subscription");
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("100000")
            .setTopic(inputTopic.toString())
            .setNumWorkers("10")
            .setMaxNumWorkers("100")
            .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("inputSubscription", backlogSubscription.toString())
            .addParameter("outputTopic", outputTopic.toString())
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    dataGenerator.execute(Duration.ofMinutes(60));
    Result result = pipelineOperator.drainJobAndFinish(createConfig(info, Duration.ofMinutes(20)));
    // Assert
    assertThat(result).isEqualTo(Result.LAUNCH_FINISHED);
    // check to see if messages reached the output topic
    assertThat(pubsubResourceManager.pull(outputSubscription, 5).getReceivedMessagesCount())
        .isGreaterThan(0);

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
