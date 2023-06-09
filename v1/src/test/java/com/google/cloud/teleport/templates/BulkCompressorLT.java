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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link BulkCompressor BulkCompressor} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(BulkCompressor.class)
@RunWith(JUnit4.class)
public class BulkCompressorLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Bulk_Compress_GCS_Files");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = BulkCompressorLT.class.getSimpleName().toLowerCase();
  private static ArtifactClient artifactClient;

  @Before
  public void setup() {
    // Set up resource managers
    Storage gcsClient = createStorageClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(artifactClient);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    // 35,000,000 messages of the given schema make up approximately 10GB
    testBacklog("35000000");
  }

  @Test
  public void testBacklog100gb() throws IOException, ParseException, InterruptedException {
    // 350,000,000 messages of the given schema make up approximately 100GB
    testBacklog("350000000");
  }

  public void testBacklog(String numMessages)
      throws IOException, ParseException, InterruptedException {
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(numMessages)
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath() + "/input")
            .setNumShards("20")
            .setNumWorkers("20")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(60));
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("inputFilePattern", getTestMethodDirPath() + "/input/*")
            .addParameter("outputDirectory", getTestMethodDirPath() + "/output")
            .addParameter("outputFailureFile", getTestMethodDirPath() + "/failed.csv")
            .addParameter("compression", "GZIP")
            .build();
    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    // check to see if messages reached the output bucket
    assertThat(artifactClient.listArtifacts(testName + "/output", Pattern.compile(".*")))
        .isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, null, null));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName);
  }
}
