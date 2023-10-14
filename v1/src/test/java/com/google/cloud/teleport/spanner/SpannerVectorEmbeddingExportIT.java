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
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.mutationsToRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.templates.SpannerVectorEmbeddingExport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.JsonTestUtil;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link SpannerVectorEmbeddingExportIT Spanner to GCS JSON} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerVectorEmbeddingExport.class)
@RunWith(JUnit4.class)
public class SpannerVectorEmbeddingExportIT extends TemplateTestBase {

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
  public void testSpannerToGCSJSON() throws IOException {
    String tableName = testName + "_Documents";
    String createDocumentsTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  id INT64 NOT NULL,\n"
                + "  text STRING(MAX),\n"
                + "  embedding ARRAY<FLOAT64>,\n"
                + "  restricts JSON,\n"
                + ") PRIMARY KEY(id)\n",
            tableName);

    googleSqlResourceManager.executeDdlStatement(createDocumentsTableStatement);
    List<Mutation> expectedData = generateTableRows(tableName);
    googleSqlResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", googleSqlResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", googleSqlResourceManager.getDatabaseId())
            .addParameter("spannerTable", tableName)
            .addParameter("spannerColumnsToExport", "id,embedding: embedding, restricts")
            .addParameter("gcsOutputFilePrefix", "vectors-")
            .addParameter("gcsOutputFolder", getGcsPath("output/"));

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> exportedArtifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*json"));
    assertThat(exportedArtifacts).isNotEmpty();

    List<Map<String, Object>> recordsFromGCS = extractArtifacts(exportedArtifacts);

    assertThatRecords(recordsFromGCS)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            mutationsToRecords(expectedData, List.of("id", "embedding", "restricts")));
  }

  @Test
  public void testPostgresSpannerToGCSJSON() throws IOException {
    // converting to lowercase for PG
    String tableName = StringUtils.lowerCase(testName + "_Documents");
    String createDocumentsTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  id int8 NOT NULL PRIMARY KEY,\n"
                + "  text text,\n"
                + "  embedding float8[],\n"
                + "  restricts JSONB\n"
                + ")\n",
            tableName);

    postgresResourceManager.executeDdlStatement(createDocumentsTableStatement);
    List<Mutation> expectedData = generateTableRows(tableName);
    postgresResourceManager.write(expectedData);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("spannerInstanceId", postgresResourceManager.getInstanceId())
            .addParameter("spannerDatabaseId", postgresResourceManager.getDatabaseId())
            .addParameter("spannerTable", tableName)
            .addParameter("spannerColumnsToExport", "id,embedding: embedding, restricts")
            .addParameter("gcsOutputFilePrefix", "vectors-")
            .addParameter("gcsOutputFolder", getGcsPath("output/"));

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> exportedArtifacts =
        gcsClient.listArtifacts("output/", Pattern.compile(".*json"));
    assertThat(exportedArtifacts).isNotEmpty();

    List<Map<String, Object>> recordsFromGCS = extractArtifacts(exportedArtifacts);

    recordsFromGCS =
        recordsFromGCS.stream().map(JsonTestUtil::sortJsonMap).collect(Collectors.toList());

    assertThatRecords(recordsFromGCS)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            mutationsToRecords(expectedData, List.of("id", "embedding", "restricts")));
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation
          .set("id")
          .to(i)
          .set("text")
          .to(RandomStringUtils.randomAlphanumeric(1, 200))
          .set("embedding")
          .to(Value.float64Array(ThreadLocalRandom.current().doubles(128, -10000, 10000).toArray()))
          .set("restricts")
          .to(Value.json("[{\"allow_list\": [\"even\"], \"namespace\": \"class\"}]"))
          .build();

      mutations.add(mutation.build());
    }
    return mutations;
  }

  private static List<Map<String, Object>> extractArtifacts(List<Artifact> artifacts) {
    List<Map<String, Object>> records = new ArrayList<>();
    artifacts.forEach(
        artifact -> {
          try {
            records.addAll(JsonTestUtil.readNDJSON(artifact.contents()));
          } catch (IOException e) {
            throw new RuntimeException("Error reading " + artifact.name() + " as JSON.", e);
          }
        });

    return records;
  }
}
