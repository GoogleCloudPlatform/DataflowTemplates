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
import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.bigtable.BigtableToVectorEmbeddings;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.truthmatchers.RecordsSubject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigtableToVectorEmbeddings} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToVectorEmbeddings.class)
@RunWith(JUnit4.class)
public class BigtableToVectorEmbeddingsIT extends TemplateTestBase {

  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setUp() throws IOException {
    bigtableResourceManager =
        BigtableResourceManager.builder(getShortTestName(), PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager);
  }

  @Test
  public void testBigtableToVectorEmbeddings() throws IOException {
    // Arrange
    String tableId = generateTableId(getShortTestName());
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf", "cf1", "cf2"));

    long timestamp = System.currentTimeMillis() * 1000;
    bigtableResourceManager.write(
        ImmutableList.of(
            RowMutation.create(tableId, "r1")
                .setCell(
                    "cf1",
                    toByteString("embedding"),
                    timestamp,
                    ByteString.copyFrom(
                        ArrayUtils.addAll(Bytes.toBytes(1.23d), Bytes.toBytes(2.34d))))
                .setCell("cf2", "crowding", timestamp, "crowd1")
                .setCell("cf", "animal", timestamp, "cat")
                .setCell("cf", "color", timestamp, "white")
                .setCell(
                    "cf",
                    toByteString("some_int_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(45)))
                .setCell(
                    "cf",
                    toByteString("some_float_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(1.23f)))
                .setCell(
                    "cf",
                    toByteString("some_double_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(2.34d))),
            RowMutation.create(tableId, "r2")
                .setCell(
                    "cf1",
                    toByteString("embedding"),
                    timestamp,
                    ByteString.copyFrom(
                        ArrayUtils.addAll(Bytes.toBytes(1.61d), Bytes.toBytes(3.24d))))
                .setCell("cf2", "crowding", timestamp, "crowd2")
                .setCell("cf", "animal", timestamp, "dog")
                .setCell("cf", "color", timestamp, "black")
                .setCell(
                    "cf",
                    toByteString("some_int_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(9)))
                .setCell(
                    "cf",
                    toByteString("some_float_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(1.61f)))
                .setCell(
                    "cf",
                    toByteString("some_double_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(3.24d))),
            RowMutation.create(tableId, "r3")
                .setCell(
                    "cf1",
                    toByteString("embedding"),
                    timestamp,
                    ByteString.copyFrom(
                        ArrayUtils.addAll(Bytes.toBytes(3.14d), Bytes.toBytes(2.71d))))
                .setCell("cf2", "crowding", timestamp, "crowd3")
                .setCell("cf", "animal", timestamp, "fish")
                .setCell("cf", "color", timestamp, "gold")
                .setCell(
                    "cf",
                    toByteString("some_int_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(5)))
                .setCell(
                    "cf",
                    toByteString("some_float_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(3.14f)))
                .setCell(
                    "cf",
                    toByteString("some_double_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(2.71d)))));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableId)
            .addParameter("outputDirectory", getGcsPath("output/"))
            .addParameter("filenamePrefix", "bigtable-to-json-output-")
            .addParameter("idColumn", "_key")
            .addParameter("embeddingColumn", "cf1:embedding")
            .addParameter("crowdingTagColumn", "cf2:crowding")
            .addParameter("embeddingByteSize", "8")
            .addParameter("allowRestrictsMappings", "cf:animal;animal")
            .addParameter("denyRestrictsMappings", "cf:color;color")
            .addParameter("intNumericRestrictsMappings", "cf:some_int_value;some_int_value")
            .addParameter("floatNumericRestrictsMappings", "cf:some_float_value;some_float_value")
            .addParameter(
                "doubleNumericRestrictsMappings", "cf:some_double_value;some_double_value");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    RecordsSubject jsonRecords = assertThatArtifacts(artifacts).asJsonRecords();
    jsonRecords.hasRows(3);
    assertThatArtifacts(artifacts)
        .hasContent(
            "{\"id\":\"r1\",\"embedding\":[1.23,2.34],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"cat\"]},{\"namespace\":\"color\",\"deny\":[\"white\"]}],\"numeric_restricts\":[{\"namespace\":\"some_double_value\",\"value_double\":2.34},{\"namespace\":\"some_float_value\",\"value_float\":1.23},{\"namespace\":\"some_int_value\",\"value_int\":45}]}");
    assertThatArtifacts(artifacts)
        .hasContent(
            "{\"id\":\"r2\",\"embedding\":[1.61,3.24],\"crowding_tag\":\"crowd2\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"dog\"]},{\"namespace\":\"color\",\"deny\":[\"black\"]}],\"numeric_restricts\":[{\"namespace\":\"some_double_value\",\"value_double\":3.24},{\"namespace\":\"some_float_value\",\"value_float\":1.61},{\"namespace\":\"some_int_value\",\"value_int\":9}]}");
    assertThatArtifacts(artifacts)
        .hasContent(
            "{\"id\":\"r3\",\"embedding\":[3.14,2.71],\"crowding_tag\":\"crowd3\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"fish\"]},{\"namespace\":\"color\",\"deny\":[\"gold\"]}],\"numeric_restricts\":[{\"namespace\":\"some_double_value\",\"value_double\":2.71},{\"namespace\":\"some_float_value\",\"value_float\":3.14},{\"namespace\":\"some_int_value\",\"value_int\":5}]}");
  }

  @Test
  public void testBigtableToVectorEmbeddings_timestamp() throws IOException {
    // Arrange
    String tableId = generateTableId(getShortTestName());
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf", "cf1", "cf2"));

    long timestamp = System.currentTimeMillis() * 1000;
    long oldTimestamp = timestamp - 1000;
    bigtableResourceManager.write(
        ImmutableList.of(
            RowMutation.create(tableId, "r1")
                // Old values
                .setCell(
                    "cf1",
                    toByteString("embedding"),
                    oldTimestamp,
                    ByteString.copyFrom(
                        ArrayUtils.addAll(Bytes.toBytes(-1.23d), Bytes.toBytes(-2.34d))))
                .setCell("cf2", "crowding", oldTimestamp, "old")
                .setCell("cf", "animal", oldTimestamp, "also old")
                .setCell("cf", "color", oldTimestamp, "old as well")
                .setCell(
                    "cf",
                    toByteString("some_int_value"),
                    oldTimestamp,
                    ByteString.copyFrom(Bytes.toBytes(-45)))
                .setCell(
                    "cf",
                    toByteString("some_float_value"),
                    oldTimestamp,
                    ByteString.copyFrom(Bytes.toBytes(-1.23f)))
                .setCell(
                    "cf",
                    toByteString("some_double_value"),
                    oldTimestamp,
                    ByteString.copyFrom(Bytes.toBytes(-2.34d)))

                // New values
                .setCell(
                    "cf1",
                    toByteString("embedding"),
                    timestamp,
                    ByteString.copyFrom(
                        ArrayUtils.addAll(Bytes.toBytes(1.23d), Bytes.toBytes(2.34d))))
                .setCell("cf2", "crowding", timestamp, "crowd1")
                .setCell("cf", "animal", timestamp, "cat")
                .setCell("cf", "color", timestamp, "white")
                .setCell(
                    "cf",
                    toByteString("some_int_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(45)))
                .setCell(
                    "cf",
                    toByteString("some_float_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(1.23f)))
                .setCell(
                    "cf",
                    toByteString("some_double_value"),
                    timestamp,
                    ByteString.copyFrom(Bytes.toBytes(2.34d)))));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableId)
            .addParameter("outputDirectory", getGcsPath("output/"))
            .addParameter("filenamePrefix", "bigtable-to-json-output-")
            .addParameter("idColumn", "_key")
            .addParameter("embeddingColumn", "cf1:embedding")
            .addParameter("crowdingTagColumn", "cf2:crowding")
            .addParameter("embeddingByteSize", "8")
            .addParameter("allowRestrictsMappings", "cf:animal;animal")
            .addParameter("denyRestrictsMappings", "cf:color;color")
            .addParameter("intNumericRestrictsMappings", "cf:some_int_value;some_int_value")
            .addParameter("floatNumericRestrictsMappings", "cf:some_float_value;some_float_value")
            .addParameter(
                "doubleNumericRestrictsMappings", "cf:some_double_value;some_double_value");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    RecordsSubject jsonRecords = assertThatArtifacts(artifacts).asJsonRecords();
    jsonRecords.hasRows(1);
    assertThatArtifacts(artifacts)
        .hasContent(
            "{\"id\":\"r1\",\"embedding\":[1.23,2.34],\"crowding_tag\":\"crowd1\",\"restricts\":[{\"namespace\":\"animal\",\"allow\":[\"cat\"]},{\"namespace\":\"color\",\"deny\":[\"white\"]}],\"numeric_restricts\":[{\"namespace\":\"some_double_value\",\"value_double\":2.34},{\"namespace\":\"some_float_value\",\"value_float\":1.23},{\"namespace\":\"some_int_value\",\"value_int\":45}]}");
  }

  static ByteString toByteString(String string) {
    return ByteString.copyFrom(string.getBytes(Charset.forName("UTF-8")));
  }

  private String getShortTestName() {
    return testName.replace("BigtableToVectorEmbeddings", "bt2vec");
  }
}
