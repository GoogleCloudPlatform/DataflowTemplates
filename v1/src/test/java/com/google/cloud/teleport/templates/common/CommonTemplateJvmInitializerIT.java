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
package com.google.cloud.teleport.templates.common;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for CommonTemplateJvmInitializer. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(JvmInitializerTemplate.class)
@RunWith(JUnit4.class)
public final class CommonTemplateJvmInitializerIT extends TemplateTestBase {

  @Before
  public void setUp() {
    // Arrange
    gcsClient.createArtifact("test.txt", "This is a test file.");
  }

  @Test
  public void testCommonTemplateJvmInitializerCopiesExtraFiles() throws IOException {
    // Act
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("inputFile", "/extra_files/test.txt")
                .addParameter("output", getGcsPath("output/result"))
                .addParameter("extraFilesToStage", getGcsPath("test.txt")));

    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    Artifact artifact = gcsClient.listArtifacts("output/", Pattern.compile(".*")).get(0);
    String output = new String(artifact.contents()).replace("\n", "");
    assertThat(output).isEqualTo("This is a test file.");
  }
}
