/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link WordCountYaml} template.
 *
 * <p>Test Design:
 *
 * <ul>
 *   <li>The WordCount YAML template is a batch pipeline reading pre-configured input text from
 *       Shakespeare's King Lear ({@code gs://dataflow-samples/shakespeare/kinglear.txt}).
 *   <li>The pipeline extracts words using a Python callable, explodes word arrays, counts word
 *       occurrences using Combine with sum, formats output as {@code word: count}, and writes
 *       results to Cloud Storage.
 *   <li>This test supplies an {@code outputPath} parameter to direct the output into a test GCS
 *       bucket managed by {@link TemplateTestBase#gcsClient}.
 *   <li>After pipeline execution completes, the test verifies that output artifacts exist and
 *       contain expected word count entries from King Lear.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(WordCountYaml.class)
@RunWith(JUnit4.class)
public class WordCountYamlIT extends TemplateTestBase {

  @Test
  public void testWordCount() throws IOException {
    // --------------------------------------------------------------------------------------------
    // 1. Arrange / Setup: Configure output destination and launch parameters
    // --------------------------------------------------------------------------------------------
    String outputPath = getGcsPath("output/counts");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath).addParameter("outputPath", outputPath);

    // --------------------------------------------------------------------------------------------
    // 2. Act: Launch the template and wait for execution to complete
    // --------------------------------------------------------------------------------------------
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // --------------------------------------------------------------------------------------------
    // 3. Assert / Verify: Verify job succeeded and output artifacts contain expected word counts
    // --------------------------------------------------------------------------------------------
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*counts.*"));
    assertThat(artifacts).isNotEmpty();

    // Verify output files contain exact expected word counts from King Lear
    StringBuilder combinedContent = new StringBuilder();
    for (Artifact artifact : artifacts) {
      combinedContent.append(new String(artifact.contents(), StandardCharsets.UTF_8));
    }
    String content = combinedContent.toString();
    assertThat(content).contains("king: 311");
    assertThat(content).contains("lear: 253");
    assertThat(content).contains("cordelia: 64");
    assertThat(content).contains("gloucester: 167");
    assertThat(content).contains("fool: 120");
  }
}
