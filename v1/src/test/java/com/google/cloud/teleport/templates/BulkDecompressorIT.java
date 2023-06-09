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

import static com.google.cloud.teleport.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifact;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.Artifact;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;
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
    gcsClient.uploadArtifact(
        "input/lipsum_gz.txt.gz",
        Resources.getResource("BulkCompressorIT/lipsum.txt.gz").getPath());
    gcsClient.uploadArtifact(
        "input/lipsum_bz.txt.bz2",
        Resources.getResource("BulkCompressorIT/lipsum.txt.bz2").getPath());
  }

  @Test
  public void testDecompress() throws IOException {
    // Arrange
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFilePattern", getGcsPath("input") + "/*")
            .addParameter("outputDirectory", getGcsPath("output"))
            .addParameter("outputFailureFile", getGcsPath("output-failure"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Two files are expected, one for each input (gzip, bz2)
    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*lipsum.*"));
    assertThat(artifacts).hasSize(2);

    // However, they both have the same hash (based on the same text file)
    assertThatArtifact(artifacts.get(0))
        .hasHash("5ae59143e1ec5446e88b0386115c95e03a632f02d96c21a76179fa1110257cfb");
    assertThatArtifact(artifacts.get(1))
        .hasHash("5ae59143e1ec5446e88b0386115c95e03a632f02d96c21a76179fa1110257cfb");
  }
}
