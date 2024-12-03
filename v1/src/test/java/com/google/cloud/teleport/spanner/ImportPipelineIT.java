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
package com.google.cloud.teleport.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ImportPipeline} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(ImportPipeline.class)
@RunWith(Parameterized.class)
public class ImportPipelineIT extends SpannerTemplateITBase {

  private SpannerResourceManager spannerResourceManager;

  private void uploadImportPipelineArtifacts(String subdirectory) throws IOException {
    gcsClient.uploadArtifact(
        "input/EmptyTable.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/EmptyTable.avro").getPath());
    gcsClient.uploadArtifact(
        "input/EmptyTable-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/EmptyTable-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Singers.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Singers.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Singers-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Singers-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Float32Table.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Float32Table.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Float32Table-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Float32Table-manifest.json")
            .getPath());

    if (Objects.equals(subdirectory, "googlesql")) {
      gcsClient.uploadArtifact(
          "input/ModelStruct.avro-00000-of-00001",
          Resources.getResource("ImportPipelineIT/" + subdirectory + "/ModelStruct.avro")
              .getPath());
      gcsClient.uploadArtifact(
          "input/ModelStruct-manifest.json",
          Resources.getResource("ImportPipelineIT/" + subdirectory + "/ModelStruct-manifest.json")
              .getPath());
    }

    gcsClient.uploadArtifact(
        "input/spanner-export.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/spanner-export.json")
            .getPath());
  }

  private List<Map<String, Object>> getExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(ImmutableMap.of("Id", 1, "FirstName", "Roger", "LastName", "Waters"));
    expectedRows.add(ImmutableMap.of("Id", 2, "FirstName", "Nick", "LastName", "Mason"));
    expectedRows.add(ImmutableMap.of("Id", 3, "FirstName", "David", "LastName", "Gilmour"));
    expectedRows.add(ImmutableMap.of("Id", 4, "FirstName", "Richard", "LastName", "Wright"));
    return expectedRows;
  }

  private List<Map<String, Object>> getFloat32TableExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(ImmutableMap.of("Key", "1", "Float32Value", 3.14f));
    expectedRows.add(ImmutableMap.of("Key", "2", "Float32Value", 1.1f));
    expectedRows.add(ImmutableMap.of("Key", "3", "Float32Value", 3.402823E38f));
    expectedRows.add(ImmutableMap.of("Key", "4", "Float32Value", Float.NaN));
    expectedRows.add(ImmutableMap.of("Key", "5", "Float32Value", Float.POSITIVE_INFINITY));
    expectedRows.add(ImmutableMap.of("Key", "6", "Float32Value", Float.NEGATIVE_INFINITY));
    expectedRows.add(ImmutableMap.of("Key", "7", "Float32Value", 1.175493E-38));
    expectedRows.add(ImmutableMap.of("Key", "8", "Float32Value", -3.402823E38f));
    // The custom assertions in Beam do not seem to support null values.
    // Using the string NULL to match the string representation created in
    // assertThatStructs. The actual value in avro is a plain `null`.
    expectedRows.add(ImmutableMap.of("Key", "9", "Float32Value", "NULL"));
    return expectedRows;
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testGoogleSqlImportPipeline() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testGoogleSqlImportPipelineBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testGoogleSqlImportPipelineBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    uploadImportPipelineArtifacts("googlesql");
    String createEmptyTableStatement =
        "CREATE TABLE EmptyTable (\n" + "  id INT64 NOT NULL,\n" + ") PRIMARY KEY(id)";
    spannerResourceManager.executeDdlStatement(createEmptyTableStatement);

    String createSingersTableStatement =
        "CREATE TABLE Singers (\n"
            + "  Id INT64,\n"
            + "  FirstName STRING(MAX),\n"
            + "  LastName STRING(MAX),\n"
            + "  Review STRING(MAX),\n"
            + "  MyTokens TOKENLIST AS (TOKENIZE_FULLTEXT(Review)) HIDDEN,\n"
            + ") PRIMARY KEY(Id)";
    spannerResourceManager.executeDdlStatement(createSingersTableStatement);

    String createFloat32TableStatement =
        "CREATE TABLE Float32Table (\n"
            + "  Key STRING(MAX) NOT NULL,\n"
            + "  Float32Value FLOAT32,\n"
            + ") PRIMARY KEY(Key)";
    spannerResourceManager.executeDdlStatement(createFloat32TableStatement);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("inputDir", getGcsPath("input/")));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> emptyTableRecords =
        spannerResourceManager.readTableRecords("EmptyTable", ImmutableList.of("id"));
    assertThat(emptyTableRecords).isEmpty();

    List<Struct> singersRecords =
        spannerResourceManager.readTableRecords(
            "Singers", ImmutableList.of("Id", "FirstName", "LastName"));
    assertThat(singersRecords).hasSize(4);
    assertThatStructs(singersRecords).hasRecordsUnordered(getExpectedRows());

    List<Struct> float32Records =
        spannerResourceManager.readTableRecords(
            "Float32Table", ImmutableList.of("Key", "Float32Value"));

    assertThat(float32Records).hasSize(9);
    assertThatStructs(float32Records).hasRecordsUnordered(getFloat32TableExpectedRows());
  }

  @Test
  public void testPostgresImportPipeline() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testPostgresImportPipelineBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testPostgresImportPipelineBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    uploadImportPipelineArtifacts("postgres");
    String createEmptyTableStatement =
        "CREATE TABLE \"EmptyTable\" (\n" + "  id bigint NOT NULL,\nPRIMARY KEY(id)\n" + ")";
    spannerResourceManager.executeDdlStatement(createEmptyTableStatement);

    String createSingersTableStatement =
        "CREATE TABLE \"Singers\" (\n"
            + "  \"Id\" bigint,\n"
            + "  \"FirstName\" character varying(256),\n"
            + "  \"LastName\" character varying(256),\n"
            + "PRIMARY KEY(\"Id\"))";
    spannerResourceManager.executeDdlStatement(createSingersTableStatement);

    String createFloat32TableStatement =
        "CREATE TABLE \"Float32Table\" (\n"
            + "  \"Key\" character varying NOT NULL,\n"
            + "  \"Float32Value\" real,\n"
            + "PRIMARY KEY(\"Key\"))";
    spannerResourceManager.executeDdlStatement(createFloat32TableStatement);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("inputDir", getGcsPath("input/")));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> emptyTableRecords =
        spannerResourceManager.readTableRecords("EmptyTable", ImmutableList.of("id"));
    assertThat(emptyTableRecords).isEmpty();

    List<Struct> singersRecords =
        spannerResourceManager.readTableRecords(
            "Singers", ImmutableList.of("Id", "FirstName", "LastName"));
    assertThat(singersRecords).hasSize(4);
    assertThatStructs(singersRecords).hasRecordsUnordered(getExpectedRows());

    List<Struct> float32Records =
        spannerResourceManager.readTableRecords(
            "Float32Table", ImmutableList.of("Key", "Float32Value"));

    assertThat(float32Records).hasSize(9);
    assertThatStructs(float32Records).hasRecordsUnordered(getFloat32TableExpectedRows());
  }
}
