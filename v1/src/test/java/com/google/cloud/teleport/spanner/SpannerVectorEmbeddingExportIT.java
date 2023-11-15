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
import org.junit.runners.Parameterized;

/** Integration test for {@link SpannerVectorEmbeddingExportIT Spanner to GCS JSON} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerVectorEmbeddingExport.class)
@RunWith(Parameterized.class)
public class SpannerVectorEmbeddingExportIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 100;

  private SpannerResourceManager googleSqlResourceManager;
  private SpannerResourceManager postgresResourceManager;

  private String shortTestName;

  @Parameterized.Parameter public Boolean dataBoostEnabled;

  @Parameterized.Parameters(name = "data-boost-{0}")
  public static List<Boolean> testParameters() {
    return List.of(true, false);
  }

  @Before
  public void setup() throws IOException {
    // Make test names shorter to have distinctive database IDs
    shortTestName = testName.toLowerCase().replace("test", "").replace("to", "") + dataBoostEnabled;
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(googleSqlResourceManager, postgresResourceManager);
  }

  @Test
  public void testGsqlToGcs() throws IOException {
    googleSqlResourceManager =
        SpannerResourceManager.builder(
                (dataBoostEnabled ? "db_" : "") + shortTestName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String tableName = "gsql" + dataBoostEnabled + RandomStringUtils.randomNumeric(5);
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
            .addParameter("spannerDataBoostEnabled", Boolean.toString(dataBoostEnabled))
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
  public void testPgToGcs() throws IOException {
    postgresResourceManager =
        SpannerResourceManager.builder(
                (dataBoostEnabled ? "db_" : "") + shortTestName,
                PROJECT,
                REGION,
                Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .build();

    // converting to lowercase for PG
    String tableName =
        StringUtils.lowerCase("pg" + dataBoostEnabled + RandomStringUtils.randomNumeric(5));
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
            .addParameter("spannerDataBoostEnabled", Boolean.toString(dataBoostEnabled))
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
