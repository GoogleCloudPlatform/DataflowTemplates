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
package com.google.cloud.teleport.templates.python;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatJsonRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link YAMLTemplate}. */
// SkipDirectRunnerTest: Python templates are not supported through DirectRunner yet.
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(YAMLTemplate.class)
@RunWith(JUnit4.class)
public final class YAMLTemplateIT extends TemplateTestBase {

  @Test
  public void testSimpleCompositeSpec() throws IOException {
    // Arrange
    String yamlMessage = createSimpleYamlMessage();

    // Act
    testSimpleComposite(params -> params.addParameter("yaml_pipeline", yamlMessage));
  }

  @Test
  public void testSimpleCompositeSpecFile() throws IOException {
    // Arrange
    gcsClient.createArtifact("input/simple.yaml", createSimpleYamlMessage());

    // Act
    testSimpleComposite(
        params -> params.addParameter("yaml_pipeline_file", getGcsPath("input/simple.yaml")));
  }

  private void testSimpleComposite(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    gcsClient.createArtifact("input/test.csv", "num\n0\n1\n2\n4");

    // Act
    runYamlTemplateTest(paramsAdder);

    // Assert
    List<Artifact> goodArtifacts = gcsClient.listArtifacts("output/good-", Pattern.compile(".*"));
    assertThat(goodArtifacts).hasSize(1);

    List<Artifact> badArtifacts = gcsClient.listArtifacts("output/bad-", Pattern.compile(".*"));
    assertThat(badArtifacts).hasSize(2);

    String goodRecords = new String(goodArtifacts.get(0).contents());
    List<Map<String, Object>> expectedGood =
        List.of(
            Map.of("num", 2.0, "inverse", 0.5, "sum", 2.5),
            Map.of("num", 4.0, "inverse", 0.25, "sum", 4.25));
    assertThatJsonRecords(List.of(goodRecords.split("\n"))).hasRecords(expectedGood);

    String indexError = "IndexError('string index out of range')";
    String divError = "ZeroDivisionError('division by zero')";
    String badRecords =
        new String(badArtifacts.get(0).contents()) + new String(badArtifacts.get(1).contents());
    assertThat(badRecords).contains(indexError);
    assertThat(badRecords).contains(divError);
  }

  private String createSimpleYamlMessage() throws IOException {
    String yamlMessage =
        Files.readString(Paths.get(Resources.getResource("YamlTemplateIT.yaml").getPath()));
    yamlMessage = yamlMessage.replaceAll("INPUT_PATH", getGcsBasePath() + "/input/test.csv");
    return yamlMessage.replaceAll("OUTPUT_PATH", getGcsBasePath() + "/output");
  }

  private void runYamlTemplateTest(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(PipelineLauncher.LaunchConfig.builder(testName, specPath));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
  }
}
