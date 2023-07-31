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

import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.mutationsToRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
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
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link SpannerToText Spanner to GCS Text} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerToText.class)
@RunWith(JUnit4.class)
public class SpannerToTextIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 100;

  private SpannerResourceManager googleSqlResourceManager;
  private SpannerResourceManager postgresResourceManager;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    googleSqlResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    postgresResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(googleSqlResourceManager, postgresResourceManager);
  }

  @Test
  public void testSpannerToGCSText() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            testName);
    googleSqlResourceManager.executeDdlStatement(createTableStatement);
    List<Mutation> expectedData = generateTableRows(String.format("%s", testName));
    googleSqlResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", googleSqlResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", googleSqlResourceManager.getDatabaseId())
            .addParameter("spannerTable", testName)
            .addParameter("textWritePrefix", getGcsPath("output/" + testName));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> textArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*%s.*\\.csv.*", testName)));

    List<Map<String, Object>> records = new ArrayList<>();
    textArtifacts.forEach(
        artifact -> {
          List<String> lines =
              List.of(new String(artifact.contents()).replace("\"", "").split("\n"));
          lines.forEach(
              line -> {
                List<Object> values = List.of(line.split(","));
                records.add(
                    Map.of(
                        "Id",
                        values.get(0),
                        "FirstName",
                        values.get(1),
                        "LastName",
                        values.get(2)));
              });
        });

    List<Map<String, Object>> expectedRecords = mutationsToRecords(expectedData);

    assertThatRecords(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }

  @Test
  public void testPostgresSpannerToGCSText() throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE \"%s\" (\n"
                + "  \"Id\" bigint NOT NULL,\n"
                + "  \"FirstName\" character varying(256),\n"
                + "  \"LastName\" character varying(256),\n"
                + " PRIMARY KEY(\"Id\"))",
            testName);
    postgresResourceManager.executeDdlStatement(createTableStatement);
    List<Mutation> expectedData = generateTableRows(String.format("%s", testName));
    postgresResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", postgresResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", postgresResourceManager.getDatabaseId())
            .addParameter("spannerTable", testName)
            .addParameter("textWritePrefix", getGcsPath("output/" + testName));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> textArtifacts =
        gcsClient.listArtifacts(
            "output/", Pattern.compile(String.format(".*%s.*\\.csv.*", testName)));

    List<Map<String, Object>> records = new ArrayList<>();
    textArtifacts.forEach(
        artifact -> {
          List<String> lines =
              List.of(new String(artifact.contents()).replace("\"", "").split("\n"));
          lines.forEach(
              line -> {
                List<Object> values = List.of(line.split(","));
                records.add(
                    Map.of(
                        "Id",
                        values.get(0),
                        "FirstName",
                        values.get(1),
                        "LastName",
                        values.get(2)));
              });
        });

    List<Map<String, Object>> expectedRecords = mutationsToRecords(expectedData);

    assertThatRecords(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      mutation.set("FirstName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("LastName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutations.add(mutation.build());
    }

    return mutations;
  }
}
