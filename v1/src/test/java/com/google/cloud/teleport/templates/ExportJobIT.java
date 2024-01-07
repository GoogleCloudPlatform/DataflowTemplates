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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link ExportJobPlaceholder} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(ExportJobPlaceholder.class)
@RunWith(JUnit4.class)
public class ExportJobIT extends TemplateTestBase {

  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setUp() throws IOException {
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager);
  }

  @Test
  public void testExportJob() throws IOException {
    // Arrange
    String tableId = generateTableId(testName);
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf"));

    bigtableResourceManager.write(
        ImmutableList.of(
            RowMutation.create(tableId, "row1").setCell("cf", "", 1529523011838000L, "one"),
            RowMutation.create(tableId, "row2").setCell("cf", "", 1529523013174000L, "two"),
            RowMutation.create(tableId, "row3").setCell("cf", "", 1529523014434000L, "three")));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("bigtableProject", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableId)
            .addParameter("destinationPath", getGcsPath("output/"))
            .addParameter("filenamePrefix", "bigtable-to-sequence-output-");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    for (Artifact artifact : artifacts) {
      assertThat(artifact.contents()).isNotEmpty();
    }
  }
}
