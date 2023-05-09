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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatArtifacts;
import static com.google.cloud.teleport.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.Artifact;
import com.google.cloud.teleport.it.gcp.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.TextImportPipeline;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextImportPipeline}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextImportPipeline.class)
@RunWith(JUnit4.class)
public final class TextImportPipelineIT extends TemplateTestBase {

  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setup() throws IOException, URISyntaxException {
    spannerResourceManager = SpannerResourceManager.builder(testName, PROJECT, REGION).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testImportCsv() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/singers1.csv",
        "1,John,Doe,TRUE,1.5,2023-02-01,2023-01-01T17:22:00\n"
            + "2,Jane,Doe,TRUE,2.1,2021-02-03,2023-01-01T17:23:01\n");
    gcsClient.createArtifact(
        "input/singers2.csv", "3,Elvis,Presley,FALSE,3.99,2020-03-05,2023-01-01T17:24:02\n");

    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId      INT64 NOT NULL,\n"
            + "  FirstName     STRING(1024),\n"
            + "  LastName      STRING(1024),\n"
            + "  Active        BOOL,\n"
            + "  Score         FLOAT64,\n"
            + "  BirthDate     DATE,\n"
            + "  LastModified  TIMESTAMP,\n"
            + ") PRIMARY KEY (SingerId)";
    spannerResourceManager.createTable(statement);

    String manifestJson =
        "{\n"
            + "  \"tables\": [\n"
            + "    {\n"
            + "      \"table_name\": \"Singers\",\n"
            + "      \"file_patterns\": [\n"
            + "        \""
            + getGcsPath("input")
            + "/*.csv\"\n"
            + "      ],\n"
            + "      \"columns\": [\n"
            + "        {\"column_name\": \"SingerId\", \"type_name\": \"INT64\"},\n"
            + "        {\"column_name\": \"FirstName\", \"type_name\": \"STRING\"},\n"
            + "        {\"column_name\": \"LastName\", \"type_name\": \"STRING\"},\n"
            + "        {\"column_name\": \"Active\", \"type_name\": \"BOOL\"},\n"
            + "        {\"column_name\": \"Score\", \"type_name\": \"FLOAT64\"},\n"
            + "        {\"column_name\": \"BirthDate\", \"type_name\": \"DATE\"},\n"
            + "        {\"column_name\": \"LastModified\", \"type_name\": \"TIMESTAMP\"}\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    gcsClient.createArtifact("input/manifest.json", manifestJson.getBytes(StandardCharsets.UTF_8));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("importManifest", getGcsPath("input/manifest.json"))
            .addParameter("columnDelimiter", ",")
            .addParameter("fieldQualifier", "\"")
            .addParameter("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    ImmutableList<Struct> structs =
        spannerResourceManager.readTableRecords(
            "Singers",
            List.of(
                "SingerId",
                "FirstName",
                "LastName",
                "Active",
                "Score",
                "BirthDate",
                "LastModified"));
    assertThat(structs).hasSize(3);
    assertThatStructs(structs)
        .hasRecordsUnordered(
            List.of(
                createRecordMap(
                    "1", "John", "Doe", "true", "1.5", "2023-02-01", "2023-01-01T17:22:00Z"),
                createRecordMap(
                    "2", "Jane", "Doe", "true", "2.1", "2021-02-03", "2023-01-01T17:23:01Z"),
                createRecordMap(
                    "3",
                    "Elvis",
                    "Presley",
                    "false",
                    "3.99",
                    "2020-03-05",
                    "2023-01-01T17:24:02Z")));
  }

  @Test
  public void testImportCsvBadRows() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/singers1.csv",
        "1,John,Doe,TRUE,1.5,2023-02-01,2023-01-01T17:22:00\n" + "2,Jane,Doe,5,A\n");

    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId      INT64 NOT NULL,\n"
            + "  FirstName     STRING(1024),\n"
            + "  LastName      STRING(1024),\n"
            + "  Active        BOOL,\n"
            + "  Score         FLOAT64,\n"
            + "  BirthDate     DATE,\n"
            + "  LastModified  TIMESTAMP,\n"
            + ") PRIMARY KEY (SingerId)";
    spannerResourceManager.createTable(statement);

    String manifestJson =
        "{\n"
            + "  \"tables\": [\n"
            + "    {\n"
            + "      \"table_name\": \"Singers\",\n"
            + "      \"file_patterns\": [\n"
            + "        \""
            + getGcsPath("input")
            + "/*.csv\"\n"
            + "      ],\n"
            + "      \"columns\": [\n"
            + "        {\"column_name\": \"SingerId\", \"type_name\": \"INT64\"},\n"
            + "        {\"column_name\": \"FirstName\", \"type_name\": \"STRING\"},\n"
            + "        {\"column_name\": \"LastName\", \"type_name\": \"STRING\"},\n"
            + "        {\"column_name\": \"Active\", \"type_name\": \"BOOL\"},\n"
            + "        {\"column_name\": \"Score\", \"type_name\": \"FLOAT64\"},\n"
            + "        {\"column_name\": \"BirthDate\", \"type_name\": \"DATE\"},\n"
            + "        {\"column_name\": \"LastModified\", \"type_name\": \"TIMESTAMP\"}\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    gcsClient.createArtifact("input/manifest.json", manifestJson.getBytes(StandardCharsets.UTF_8));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("importManifest", getGcsPath("input/manifest.json"))
            .addParameter("columnDelimiter", ",")
            .addParameter("fieldQualifier", "\"")
            .addParameter("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
            .addParameter("invalidOutputPath", getGcsPath("invalid/bad"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    ImmutableList<Struct> structs =
        spannerResourceManager.readTableRecords(
            "Singers",
            List.of(
                "SingerId",
                "FirstName",
                "LastName",
                "Active",
                "Score",
                "BirthDate",
                "LastModified"));
    assertThat(structs).hasSize(1);
    assertThatStructs(structs)
        .hasRecordsUnordered(
            List.of(
                createRecordMap(
                    "1", "John", "Doe", "true", "1.5", "2023-02-01", "2023-01-01T17:22:00Z")));

    List<Artifact> artifacts = gcsClient.listArtifacts("invalid/", Pattern.compile(".*bad.*"));
    assertThat(artifacts).hasSize(1);
    assertThatArtifacts(artifacts).hasContent("2,Jane,Doe,5,A");
  }

  private Map<String, Object> createRecordMap(
      String singerId,
      String firstName,
      String lastName,
      String active,
      String score,
      String birthDate,
      String lastModified) {
    return Map.of(
        "SingerId",
        singerId,
        "FirstName",
        firstName,
        "LastName",
        lastName,
        "Active",
        active,
        "Score",
        score,
        "BirthDate",
        birthDate,
        "LastModified",
        lastModified);
  }
}
