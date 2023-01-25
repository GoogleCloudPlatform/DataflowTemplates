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
package com.google.cloud.teleport.bigtable;

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
import com.google.cloud.teleport.it.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Performance tests for {@link AvroToBigtable} (GCS_Avro_to_Cloud_Bigtable) template. */
@TemplateIntegrationTest(AvroToBigtable.class)
@RunWith(JUnit4.class)
public class AvroToBigtablePerformanceIT extends PerformanceBenchmarkingBase {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToBigtablePerformanceIT.class);
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/GCS_Avro_to_Cloud_Bigtable");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      AvroToBigtablePerformanceIT.class.getSimpleName().toLowerCase();
  // 56,000,000 messages of the given schema approximately make up 10GB
  private static final String NUM_MESSAGES = "56000000";
  private static final String INPUT_PCOLLECTION = "Read from Avro/Read.out0";
  private static final String OUTPUT_PCOLLECTION = "Transform to Bigtable.out0";
  private static BigtableResourceManager bigtableResourceManager;
  private static ArtifactClient artifactClient;
  private static String generatorSchemaPath;
  private static String avroSchemaPath;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    bigtableResourceManager =
        DefaultBigtableResourceManager.builder(testName.getMethodName(), PROJECT).build();
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
    // upload schema files and save path
    generatorSchemaPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            artifactClient
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource("AvroToBigtablePerformanceIT/schema.json").getPath())
                .name());
    avroSchemaPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            artifactClient
                .uploadArtifact(
                    "input/bigtable.avsc",
                    Resources.getResource("schema/avro/bigtable.avsc").getPath())
                .name());
  }

  @After
  public void teardown() {
    bigtableResourceManager.cleanupAll();
    artifactClient.cleanupRun();
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    // Bigtable expects Avro files to meet a specific schema
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(jobName + "-data-generator", generatorSchemaPath)
            .setQPS("1000000")
            .setMessagesLimit(NUM_MESSAGES)
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setOutputType("AVRO")
            .setAvroSchemaLocation(avroSchemaPath)
            .setNumShards("20")
            .setNumWorkers("20")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    // The generated data contains only 1 column family named "SystemMetrics"
    bigtableResourceManager.createTable(name, ImmutableList.of("SystemMetrics"));
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", name)
            .addParameter("inputFilePattern", getTestMethodDirPath() + "/*")
            .build();

    // Act
    JobInfo info = dataflowClient.launch(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    Result result = dataflowOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
    // check to see if messages reached the output bigtable table
    assertThat(bigtableResourceManager.readTable(name)).isNotEmpty();

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
