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
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatGenericRecords;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateAppProfileId;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.bigtable.BigtableToParquet;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.ParquetTestUtil;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigtableToParquet} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToParquet.class)
@RunWith(JUnit4.class)
public class BigtableToParquetIT extends TemplateTestBase {

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
  public void testBigtableToParquet() throws IOException {
    // Arrange
    String tableId = generateTableId(testName);
    bigtableResourceManager.createTable(tableId, ImmutableList.of("col1"));

    String appProfileId = generateAppProfileId();
    bigtableResourceManager.createAppProfile(
        appProfileId, false, bigtableResourceManager.getClusterNames());

    long timestamp = System.currentTimeMillis() * 1000;
    bigtableResourceManager.write(
        ImmutableList.of(
            RowMutation.create(tableId, "r1").setCell("col1", "c1", timestamp, "new-value1"),
            RowMutation.create(tableId, "r2").setCell("col1", "c1", timestamp, "new-value2"),
            RowMutation.create(tableId, "r3").setCell("col1", "c1", timestamp, "new-value3")));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableId)
            .addParameter("outputDirectory", getGcsPath("output/"))
            .addParameter("filenamePrefix", "bigtable-to-parquet-output-")
            .addParameter("numShards", "1")
            .addParameter("bigtableAppProfileId", appProfileId);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    List<GenericRecord> allRecords = new ArrayList<>();
    for (Artifact artifact : artifacts) {
      try {
        allRecords.addAll(ParquetTestUtil.readRecords(artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Parquet.", e);
      }
    }

    assertThatGenericRecords(allRecords)
        .hasRecordsWithStrings(ImmutableList.of("new-value1", "new-value2", "new-value3"));
  }
}
