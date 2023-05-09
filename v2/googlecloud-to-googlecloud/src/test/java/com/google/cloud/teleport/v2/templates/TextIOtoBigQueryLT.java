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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link TextIOToBigQuery TextIOToBigQuery} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(TextIOToBigQuery.class)
@RunWith(JUnit4.class)
public class TextIOtoBigQueryLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/GCS_Text_to_BigQuery_Flex");
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final Long NUM_MESSAGES = 35_000_000L;
  // schema should match schema supplied to generate fake records.
  private static final Schema SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.INT64),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL));
  private static final String INPUT_PCOLLECTION = "Read from source/Read.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Insert into Bigquery/PrepareWrite/ParDo(Anonymous).out0";
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      TextIOtoBigQueryLT.class.getSimpleName().toLowerCase();
  private static ArtifactClient artifactClient;
  private static DefaultBigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    Storage gcsClient = createStorageClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, project)
            .setCredentials(CREDENTIALS)
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(artifactClient, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(Function.identity());
  }

  @Ignore("Test fails, sickbay")
  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "use_runner_v2"));
  }

  @Ignore("Test fails, sickbay")
  @Test
  public void testBacklog10gbUsingPrime() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "enable_prime"));
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // upload schema files and save path
    String jsonPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            artifactClient
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource("TextIOtoBigQueryLT/schema.json").getPath())
                .name());
    String udfPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            artifactClient
                .uploadArtifact(
                    "input/udf.js", Resources.getResource("TextIOToBigQueryTest/udf.js").getPath())
                .name());
    TableId table = bigQueryResourceManager.createTable(testName, SCHEMA);
    // Generate fake data to table
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setNumShards("20")
            .setNumWorkers("20")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter("JSONPath", jsonPath)
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*")
                    .addParameter("outputTable", toTableSpec(project, table))
                    .addParameter("javascriptTextTransformGcsPath", udfPath)
                    .addParameter("javascriptTextTransformFunctionName", "identity")
                    .addParameter(
                        "bigQueryLoadingTemporaryDirectory", getTestMethodDirPath() + "/temp"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable())).isAtLeast(NUM_MESSAGES);

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName);
  }
}
