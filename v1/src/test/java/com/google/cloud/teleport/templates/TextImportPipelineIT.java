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
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.TextImportPipeline;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextImportPipeline}. */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
@TemplateIntegrationTest(TextImportPipeline.class)
@RunWith(JUnit4.class)
public final class TextImportPipelineIT extends TemplateTestBase {

  private SpannerResourceManager googleSqlResourceManager;
  private SpannerResourceManager postgresResourceManager;

  @Before
  public void setup() throws IOException, URISyntaxException {
    googleSqlResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    postgresResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(googleSqlResourceManager, postgresResourceManager);
  }

  @Test
  public void testImportCsv() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/singers1.csv",
        "1,John,Doe,TRUE,3.5,1.5,2023-02-01,2023-01-01T17:22:00\n"
            + "2,Jane,Doe,TRUE,4.1,2.1,2021-02-03,2023-01-01T17:23:01\n");
    gcsClient.createArtifact(
        "input/singers2.csv", "3,Elvis,Presley,FALSE,5.0,3.99,2020-03-05,2023-01-01T17:24:02\n");

    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId      INT64 NOT NULL,\n"
            + "  FirstName     STRING(1024),\n"
            + "  LastName      STRING(1024),\n"
            + "  Active        BOOL,\n"
            + "  Rating        FLOAT32,\n"
            + "  Score         FLOAT64,\n"
            + "  BirthDate     DATE,\n"
            + "  LastModified  TIMESTAMP,\n"
            + ") PRIMARY KEY (SingerId)";
    googleSqlResourceManager.executeDdlStatement(statement);

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
            + "        {\"column_name\": \"Rating\", \"type_name\": \"FLOAT32\"},\n"
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
            .addParameter("instanceId", googleSqlResourceManager.getInstanceId())
            .addParameter("databaseId", googleSqlResourceManager.getDatabaseId())
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
        googleSqlResourceManager.readTableRecords(
            "Singers",
            List.of(
                "SingerId",
                "FirstName",
                "LastName",
                "Active",
                "Rating",
                "Score",
                "BirthDate",
                "LastModified"));
    assertThat(structs).hasSize(3);
    assertThatStructs(structs)
        .hasRecordsUnordered(
            List.of(
                createRecordMap(
                    "1", "John", "Doe", "true", "3.5", "1.5", "2023-02-01", "2023-01-01T17:22:00Z"),
                createRecordMap(
                    "2", "Jane", "Doe", "true", "4.1", "2.1", "2021-02-03", "2023-01-01T17:23:01Z"),
                createRecordMap(
                    "3",
                    "Elvis",
                    "Presley",
                    "false",
                    "5.0",
                    "3.99",
                    "2020-03-05",
                    "2023-01-01T17:24:02Z")));
  }

  @Test
  public void testImportCsvBadRows() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/singers1.csv",
        "1,John,Doe,TRUE,4.0,1.5,2023-02-01,2023-01-01T17:22:00\n" + "2,Jane,Doe,5,A\n");

    String statement =
        "CREATE TABLE Singers (\n"
            + "  SingerId      INT64 NOT NULL,\n"
            + "  FirstName     STRING(1024),\n"
            + "  LastName      STRING(1024),\n"
            + "  Active        BOOL,\n"
            + "  Rating        FLOAT32,\n"
            + "  Score         FLOAT64,\n"
            + "  BirthDate     DATE,\n"
            + "  LastModified  TIMESTAMP,\n"
            + ") PRIMARY KEY (SingerId)";
    googleSqlResourceManager.executeDdlStatement(statement);

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
            + "        {\"column_name\": \"Rating\", \"type_name\": \"FLOAT32\"},\n"
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
            .addParameter("instanceId", googleSqlResourceManager.getInstanceId())
            .addParameter("databaseId", googleSqlResourceManager.getDatabaseId())
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
        googleSqlResourceManager.readTableRecords(
            "Singers",
            List.of(
                "SingerId",
                "FirstName",
                "LastName",
                "Active",
                "Rating",
                "Score",
                "BirthDate",
                "LastModified"));
    assertThat(structs).hasSize(1);
    assertThatStructs(structs)
        .hasRecordsUnordered(
            List.of(
                createRecordMap(
                    "1",
                    "John",
                    "Doe",
                    "true",
                    "4.0",
                    "1.5",
                    "2023-02-01",
                    "2023-01-01T17:22:00Z")));

    List<Artifact> artifacts = gcsClient.listArtifacts("invalid/", Pattern.compile(".*bad.*"));
    assertThat(artifacts).hasSize(1);
    assertThatArtifacts(artifacts).hasContent("2,Jane,Doe,5,A");
  }

  @Test
  public void testImportCsvToPostgres() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/singers1.csv",
        "1,John,Doe,TRUE,3.5,1.5,2023-02-01,2023-01-01T17:22:00\n"
            + "2,Jane,Doe,TRUE,4.1,2.1,2021-02-03,2023-01-01T17:23:01\n");
    gcsClient.createArtifact(
        "input/singers2.csv", "3,Elvis,Presley,FALSE,5.0,3.99,2020-03-05,2023-01-01T17:24:02\n");

    String statement =
        "CREATE TABLE \"Singers\" (\n"
            + "  \"SingerId\"      bigint NOT NULL,\n"
            + "  \"FirstName\"     character varying(256),\n"
            + "  \"LastName\"      character varying(256),\n"
            + "  \"Active\"        boolean,\n"
            + "  \"Rating\"        real,\n"
            + "  \"Score\"         double precision,\n"
            + "  \"BirthDate\"     date,\n"
            + "  \"LastModified\"  timestamp with time zone,\n"
            + " PRIMARY KEY (\"SingerId\"))";
    postgresResourceManager.executeDdlStatement(statement);

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
            + "        {\"column_name\": \"SingerId\", \"type_name\": \"bigint\"},\n"
            + "        {\"column_name\": \"FirstName\", \"type_name\": \"character varying(256)\"},\n"
            + "        {\"column_name\": \"LastName\", \"type_name\": \"character varying(256)\"},\n"
            + "        {\"column_name\": \"Active\", \"type_name\": \"boolean\"},\n"
            + "        {\"column_name\": \"Rating\", \"type_name\": \"real\"},\n"
            + "        {\"column_name\": \"Score\", \"type_name\": \"double precision\"},\n"
            + "        {\"column_name\": \"BirthDate\", \"type_name\": \"date\"},\n"
            + "        {\"column_name\": \"LastModified\", \"type_name\": \"timestamp with time zone\"}\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"dialect\": \"POSTGRESQL\"\n"
            + "}";
    gcsClient.createArtifact("input/manifest.json", manifestJson.getBytes(StandardCharsets.UTF_8));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("instanceId", postgresResourceManager.getInstanceId())
            .addParameter("databaseId", postgresResourceManager.getDatabaseId())
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
        postgresResourceManager.readTableRecords(
            "Singers",
            List.of(
                "SingerId",
                "FirstName",
                "LastName",
                "Active",
                "Rating",
                "Score",
                "BirthDate",
                "LastModified"));
    assertThat(structs).hasSize(3);
    assertThatStructs(structs)
        .hasRecordsUnordered(
            List.of(
                createRecordMap(
                    "1", "John", "Doe", "true", "3.5", "1.5", "2023-02-01", "2023-01-01T17:22:00Z"),
                createRecordMap(
                    "2", "Jane", "Doe", "true", "4.1", "2.1", "2021-02-03", "2023-01-01T17:23:01Z"),
                createRecordMap(
                    "3",
                    "Elvis",
                    "Presley",
                    "false",
                    "5.0",
                    "3.99",
                    "2020-03-05",
                    "2023-01-01T17:24:02Z")));
  }

  private Map<String, Object> createRecordMap(
      String singerId,
      String firstName,
      String lastName,
      String active,
      String rating,
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
        "Rating",
        rating,
        "Score",
        score,
        "BirthDate",
        birthDate,
        "LastModified",
        lastModified);
  }
}
