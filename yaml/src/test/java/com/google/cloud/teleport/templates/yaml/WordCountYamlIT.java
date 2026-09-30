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
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link WordCountYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(WordCountYaml.class)
@RunWith(JUnit4.class)
public class WordCountYamlIT extends TemplateTestBase {

  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() {
    gcsResourceManager =
        artifactBucketName != null && !artifactBucketName.isEmpty()
            ? GcsResourceManager.builder(artifactBucketName, testName, credentials).build()
            : GcsResourceManager.builder(testName, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(gcsResourceManager);
  }

  @Test
  public void testWordCount() throws IOException {
    // 1. Upload sample text to GCS
    String inputContent = "word count word\nword count\ncount\n";
    gcsResourceManager.createArtifact("input/words.txt", inputContent);

    // 2. Launch the Pipeline
    String inputPath = getGcsPath("input/words.txt", gcsResourceManager);
    String outputPath = getGcsPath("output/counts", gcsResourceManager);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputPath", inputPath)
            .addParameter("outputPath", outputPath);

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // 3. Wait for job to finish and assert results
    Result result = pipelineOperator().waitUntilDone(createConfig(info));
    assertThatResult(result).isLaunchFinished();

    // 4. Verify output files exist
    List<Artifact> artifacts = gcsResourceManager.listArtifacts("output/", null);
    assertThat(artifacts).isNotEmpty();
  }
}
