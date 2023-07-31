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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.sdk.io.Compression;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BulkCompressor} (BulkCompressor). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BulkCompressor.class)
@RunWith(JUnit4.class)
public final class BulkCompressorIT extends TemplateTestBase {

  @Before
  public void setup() throws IOException, URISyntaxException {
    gcsClient.uploadArtifact(
        "input/lipsum.txt", Resources.getResource("BulkCompressorIT/lipsum.txt").getPath());
  }

  @Test
  public void testCompressGzip() throws IOException {
    baseCompress(
        Compression.GZIP, "81f7b7afd932b4754caaa9ba6ced7a8bcb2cbfec6857cf823e4d112125c6e939");
  }

  @Test
  public void testCompressBzip2() throws IOException {
    baseCompress(
        Compression.BZIP2, "70d04e7576b6e02cbaff137be03fe70f18ea6646c7bef8198a1d68272d6183ae");
  }

  public void baseCompress(Compression compression, String expectedSha256) throws IOException {
    // Arrange
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFilePattern", getGcsPath("input") + "/*.txt")
            .addParameter("outputDirectory", getGcsPath("output"))
            .addParameter("outputFailureFile", getGcsPath("output-failure"))
            .addParameter("compression", compression.name());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*lipsum.*"));
    assertThat(artifacts).hasSize(1);
    assertThatArtifacts(artifacts).hasHash(expectedSha256);
  }
}
