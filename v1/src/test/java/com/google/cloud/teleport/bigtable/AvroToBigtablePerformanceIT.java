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
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateTableId;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link AvroToBigtable GCS Avro to Bigtable} template. */
@TemplateIntegrationTest(AvroToBigtable.class)
@RunWith(JUnit4.class)
public class AvroToBigtablePerformanceIT extends PerformanceBenchmarkingBase {
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
    testBacklog10gb(Function.identity());
  }

  @Test
  public void testBacklog10gbUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addEnvironment("enableStreamingEngine", true));
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    String tableId = generateTableId(testName.getMethodName());
    // The generated data contains only 1 column family named "SystemMetrics"
    bigtableResourceManager.createTable(tableId, ImmutableList.of("SystemMetrics"));
    // Bigtable expects Avro files to meet a specific schema
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
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
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter("bigtableProjectId", PROJECT)
                    .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableTableId", tableId)
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    // check to see if messages reached the output bigtable table
    assertThat(bigtableResourceManager.readTable(tableId)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(
        ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName.getMethodName());
  }
}
