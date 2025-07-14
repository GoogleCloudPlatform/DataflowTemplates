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
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link SpannerToText Spanner to GCS Text} template. */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
@TemplateIntegrationTest(SpannerToText.class)
@RunWith(Parameterized.class)
public class SpannerToTextIT extends SpannerTemplateITBase {

  private static final int MESSAGES_COUNT = 100;

  private SpannerResourceManager spannerResourceManager;

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testSpannerToGCSText() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testSpannerToGCSTextBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testSpannerToGCSTextBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP TABLE IF EXISTS `%s`", testName));
    statements.add(
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            testName));
    spannerResourceManager.executeDdlStatements(statements);
    List<Mutation> expectedData = generateTableRows(String.format("%s", testName));
    spannerResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerDatabaseId", spannerResourceManager.getDatabaseId())
                .addParameter("spannerTable", testName)
                .addParameter("textWritePrefix", getGcsPath("output/" + testName)));

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
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testPostgresSpannerToGCSTextBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testPostgresSpannerToGCSTextBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP TABLE IF EXISTS \"%s\"", testName));
    statements.add(
        String.format(
            "CREATE TABLE \"%s\" (\n"
                + "  \"Id\" bigint NOT NULL,\n"
                + "  \"FirstName\" character varying(256),\n"
                + "  \"LastName\" character varying(256),\n"
                + " PRIMARY KEY(\"Id\"))",
            testName));
    spannerResourceManager.executeDdlStatements(statements);
    List<Mutation> expectedData = generateTableRows(String.format("%s", testName));
    spannerResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerDatabaseId", spannerResourceManager.getDatabaseId())
                .addParameter("spannerTable", testName)
                .addParameter("textWritePrefix", getGcsPath("output/" + testName)));

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

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  public void testSpannerToGCSText_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    // Arrange
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP TABLE IF EXISTS `%s`", testName));
    statements.add(
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  UuidCol UUID\n"
                + ") PRIMARY KEY(Id)",
            testName));
    spannerResourceManager.executeDdlStatements(statements);
    List<Mutation> expectedData = generateTableRowsUUID(String.format("%s", testName));
    spannerResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTable", testName)
            .addParameter("textWritePrefix", getGcsPath("output/" + testName))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());

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
                List<Object> values = List.of(line.split(",", -1));
                Object uuidVal = values.get(1) == "" ? "null" : values.get(1);
                records.add(Map.of("Id", values.get(0), "UuidCol", uuidVal));
              });
        });

    List<Map<String, Object>> expectedRecords = mutationsToRecords(expectedData);

    assertThatRecords(records).hasRecordsUnordered(expectedRecords);
  }

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  public void testPostgresSpannerToGCSText_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    // Arrange
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP TABLE IF EXISTS \"%s\"", testName));
    statements.add(
        String.format(
            "CREATE TABLE \"%s\" (\n"
                + "  \"Id\" bigint NOT NULL,\n"
                + "  \"UuidCol\" uuid,\n"
                + " PRIMARY KEY(\"Id\"))",
            testName));
    spannerResourceManager.executeDdlStatements(statements);
    List<Mutation> expectedData = generateTableRowsUUID(String.format("%s", testName));
    spannerResourceManager.write(expectedData);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTable", testName)
            .addParameter("textWritePrefix", getGcsPath("output/" + testName))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());

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
                List<Object> values = List.of(line.split(",", -1));
                Object uuidVal = values.get(1) == "" ? "null" : values.get(1);
                records.add(Map.of("Id", values.get(0), "UuidCol", uuidVal));
              });
        });

    List<Map<String, Object>> expectedRecords = mutationsToRecords(expectedData);

    assertThatRecords(records).hasRecordsUnordered(expectedRecords);
  }

  private static List<Mutation> generateTableRowsUUID(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      // Randomly add null values as well
      String uuid = Math.random() < 0.5 ? UUID.randomUUID().toString() : null;
      mutation.set("UuidCol").to(uuid);
      mutations.add(mutation.build());
    }

    return mutations;
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
