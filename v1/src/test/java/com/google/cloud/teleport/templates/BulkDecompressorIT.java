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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BulkDecompressor} (BulkDecompressor). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BulkDecompressor.class)
@RunWith(JUnit4.class)
public final class BulkDecompressorIT extends TemplateTestBase {

  @Before
  public void setup() throws IOException, URISyntaxException {
    artifactClient.uploadArtifact(
        "input/compress.txt.gz",
        Resources.getResource("BulkCompressorIT/compress.txt.gz").getPath());
  }

  @Test
  public void testDecompressGzip() throws IOException {
    // Arrange
    String jobName = createJobName(testName.getMethodName());

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("inputFilePattern", getGcsPath("input") + "/*.txt.gz")
            .addParameter("outputDirectory", getGcsPath("output"))
            .addParameter("outputFailureFile", getGcsPath("output-failure"));

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    Result result = new DataflowOperator(getDataflowClient()).waitUntilDone(createConfig(info));

    // Assert
    assertThat(result).isEqualTo(Result.JOB_FINISHED);

    List<Artifact> artifacts =
        artifactClient.listArtifacts("output/", Pattern.compile(".*compress.*"));
    assertThat(artifacts).hasSize(1);
    assertThat(artifacts.get(0).contents())
        .isEqualTo(
            Resources.getResource("BulkCompressorIT/compress.txt").openStream().readAllBytes());
  }
}
