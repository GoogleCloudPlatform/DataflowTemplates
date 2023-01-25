/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link ExportPipeline} (Cloud_Spanner_to_GCS_Avro) template. */
@TemplateIntegrationTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class ExportPipelinePerformanceIT extends PerformanceBenchmarkingBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Cloud_Spanner_to_GCS_Avro");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      ExportPipelinePerformanceIT.class.getSimpleName().toLowerCase();
  private static final String NUM_MESSAGES = "35000000";
  private static final String INPUT_PCOLLECTION =
      "Run Export/Read all rows from Spanner/Read from Cloud Spanner/Read from Partitions.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Run Export/Store Avro files/Write/RewindowIntoGlobal/Window.Assign.out0";
  private static SpannerResourceManager spannerResourceManager;
  private static ArtifactClient artifactClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    spannerResourceManager =
        DefaultSpannerResourceManager.builder(testName.getMethodName(), PROJECT, REGION).build();
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void teardown() {
    spannerResourceManager.cleanupAll();
    artifactClient.cleanupRun();
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    // create spanner table
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  eventId STRING(1024) NOT NULL,\n"
                + "  eventTimestamp INT64,\n"
                + "  ipv4 STRING(1024),\n"
                + "  ipv6 STRING(1024),\n"
                + "  country STRING(1024),\n"
                + "  username STRING(1024),\n"
                + "  quest STRING(1024),\n"
                + "  score INT64,\n"
                + "  completed BOOL,\n"
                + ") PRIMARY KEY(eventId)",
            testName.getMethodName());
    spannerResourceManager.createTable(createTableStatement);
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(jobName + "-data-generator", "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(NUM_MESSAGES)
            .setSinkType("SPANNER")
            .setProjectId(PROJECT)
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(testName.getMethodName())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("outputDir", getTestMethodDirPath())
            .build();

    // Act
    JobInfo info = dataflowClient.launch(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    Result result = dataflowOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
    // check to see if messages reached the output topic
    assertThat(artifactClient.listArtifacts(name, Pattern.compile(".*"))).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, computeMetrics(info));
  }

  private Map<String, Double> computeMetrics(JobInfo info)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION);
    metrics.putAll(getCpuUtilizationMetrics(info));
    Map<String, Double> outputThroughput =
        getThroughputMetricsOfPcollection(info, OUTPUT_PCOLLECTION);
    Map<String, Double> inputThroughput =
        getThroughputMetricsOfPcollection(info, INPUT_PCOLLECTION);
    metrics.put("AvgInputThroughput", inputThroughput.get("AvgThroughput"));
    metrics.put("MaxInputThroughput", inputThroughput.get("MaxThroughput"));
    metrics.put("AvgOutputThroughput", outputThroughput.get("AvgThroughput"));
    metrics.put("MaxOutputThroughput", outputThroughput.get("MaxThroughput"));
    return metrics;
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(
        ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName.getMethodName());
  }
}
