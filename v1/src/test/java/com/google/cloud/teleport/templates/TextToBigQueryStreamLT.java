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

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.ArtifactClient;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link TextToBigQueryStreamLT GCS Text to BigQuery} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(TextToBigQueryStreaming.class)
@RunWith(JUnit4.class)
public class TextToBigQueryStreamLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Stream_GCS_Text_to_BigQuery");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      TextToBigQueryStreamLT.class.getSimpleName().toLowerCase();
  private static final String INPUT_PCOLLECTION =
      "ReadFromSource/Via ReadFiles/Read all via FileBasedSource/Read ranges.out0";
  private static final String OUTPUT_PCOLLECTION =
      "InsertIntoBigQuery/StreamingInserts/StreamingWriteTables/StripShardId/Map.out0";
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

  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final int NUM_MESSAGES_FOR_10GB = 35000000;
  private static final long TIMEOUT_FOR_10_GB_TEST_MINUTES = 30;
  private static final long TIMEOUT_FOR_1_HOUR_TEST_MINUTES = 60;

  private static ArtifactClient gcsClient;
  private static BigQueryResourceManager bigQueryResourceManager;
  private String jsonPath;
  private String udfPath;

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();

    gcsClient = GcsResourceManager.builder(ARTIFACT_BUCKET, TEST_ROOT_DIR, CREDENTIALS).build();
    // upload schema files and save path
    jsonPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            gcsClient
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource("TextIOToBigQueryTest/schema.json").getPath())
                .name());

    udfPath =
        getFullGcsPath(
            ARTIFACT_BUCKET,
            gcsClient
                .uploadArtifact(
                    "input/udf.js", Resources.getResource("TextIOToBigQueryTest/udf.js").getPath())
                .name());
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager, gcsClient);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::disableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStreamingEngine()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableStreamingEngine);
  }

  @Ignore("RunnerV2 is disabled on streaming templates.")
  @Test
  public void testSteadyState1hrUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(this::enableRunnerV2);
  }

  @Test
  public void testSteadyState1hrUsingStorageApi()
      throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(
        config ->
            config
                .addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "60")
                .addParameter("storageWriteApiTriggeringFrequencySec", "60")
                .addParameter("experiments", "disable_runner_v2"));
  }

  private void testBacklog(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, ParseException, InterruptedException {

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES_FOR_10GB))
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setNumShards("20")
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    // Executes the data generator
    dataGenerator.execute(Duration.ofMinutes(TIMEOUT_FOR_10_GB_TEST_MINUTES));

    /*
     * This table will automatically expire 1 h after creation if not cleaned up manually or by
     * calling the {@link BigQueryResourceManager#cleanupAll()} method.
     */
    TableId table = bigQueryResourceManager.createTable(testName, SCHEMA);

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 5)
                    .addParameter("outputTable", toTableSpec(project, table))
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*")
                    .addParameter("JSONPath", jsonPath)
                    .addParameter(
                        "bigQueryLoadingTemporaryDirectory", getTestMethodDirPath() + "/temp")
                    .addParameter("javascriptTextTransformGcsPath", udfPath)
                    .addParameter("javascriptTextTransformFunctionName", "identity"))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    PipelineOperator.Result result =
        // The method waitForConditionAndCancel was used because the streaming pipeline template
        // includes a call to Splittable DoFn. Invoking a splittable DoFn causes the job to remain
        // in the Draining state indefinitely.
        // @see <a
        // href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#important_information_about_draining_a_job">
        // Important information about draining a job</a>
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(TIMEOUT_FOR_10_GB_TEST_MINUTES)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(NUM_MESSAGES_FOR_10GB)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private void testSteadyState1hr(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, ParseException, InterruptedException {

    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("100000")
            .setSinkType("GCS")
            .setOutputDirectory(getTestMethodDirPath())
            .setNumShards("20")
            .setNumWorkers("10")
            .setMaxNumWorkers("15")
            .build();

    /*
     * This table will automatically expire 2h after creation if not cleaned up manually or by
     * calling the {@link BigQueryResourceManager#cleanupAll()} method.
     */
    TableId table =
        bigQueryResourceManager.createTable(testName, SCHEMA, System.currentTimeMillis() + 7200000);

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment("maxWorkers", 10)
                    .addEnvironment("numWorkers", 5)
                    .addParameter("outputTable", toTableSpec(project, table))
                    .addParameter("inputFilePattern", getTestMethodDirPath() + "/*")
                    .addParameter("JSONPath", jsonPath)
                    .addParameter(
                        "bigQueryLoadingTemporaryDirectory", getTestMethodDirPath() + "/temp")
                    .addParameter("javascriptTextTransformGcsPath", udfPath)
                    .addParameter("javascriptTextTransformFunctionName", "identity"))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();

    // Executes the data generator and return approximate number of messages
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    int expectedMessages =
        (int) (dataGenerator.execute(Duration.ofMinutes(TIMEOUT_FOR_1_HOUR_TEST_MINUTES)) * 0.99);

    PipelineOperator.Result result =
        // The method waitForConditionAndCancel was used because the streaming pipeline template
        // includes a call to Splittable DoFn. Invoking a splittable DoFn causes the job to remain
        // in the Draining state indefinitely.
        // @see <a
        // href="https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline#important_information_about_draining_a_job">
        // Important information about draining a job</a>
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(TIMEOUT_FOR_1_HOUR_TEST_MINUTES)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(expectedMessages)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsClient.runId(), testName);
  }
}
