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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link TextIOToBigQuery} (GCS_Text_to_BigQuery_Flex).
 *
 * <p>Example Usage:
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class TextIOToBigQueryIT extends TemplateTestBase {

  private static final String SCHEMA_PATH = "TextIOToBigQueryTest/schema.json";
  private static final String INPUT_PATH = "TextIOToBigQueryTest/input.txt";
  private static final String UDF_PATH = "TextIOToBigQueryTest/udf.js";
  private static final String PYUDF_PATH = "TextIOToBigQueryTest/pyudf.py";
  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient);
  }

  @Test
  @TemplateIntegrationTest(value = TextIOToBigQuery.class, template = "GCS_Text_to_BigQuery_Flex")
  public void testTextIOToBigQueryWithJavascriptUdf() throws IOException {
    gcsClient.uploadArtifact("udf.js", Resources.getResource(UDF_PATH).getPath());
    testTextIOToBigQuery(
        b -> {
          b.addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"));
          b.addParameter("javascriptTextTransformFunctionName", "identity");
          return b;
        });
  }

  @Test
  @TemplateIntegrationTest(value = TextIOToBigQuery.class, template = "GCS_Text_to_BigQuery_Flex")
  @Category(DirectRunnerTest.class)
  public void testTextIOToBigQueryWithNoUdf() throws IOException {
    testTextIOToBigQuery(Function.identity());
  }

  @Test
  @TemplateIntegrationTest(value = TextIOToBigQuery.class, template = "GCS_Text_to_BigQuery_Flex")
  public void testTextIOToBigQueryWithStorageApi() throws IOException {
    gcsClient.uploadArtifact("udf.js", Resources.getResource(UDF_PATH).getPath());
    testTextIOToBigQuery(
        b -> {
          b.addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"));
          b.addParameter("javascriptTextTransformFunctionName", "identity");
          b.addParameter("useStorageWriteApi", "true");
          return b;
        });
  }

  @Test
  @TemplateIntegrationTest(value = TextIOToBigQuery.class, template = "GCS_Text_to_BigQuery_Xlang")
  public void testTextIOToBigQueryWithPythonUdf() throws IOException {
    gcsClient.uploadArtifact("pyudf.py", Resources.getResource(PYUDF_PATH).getPath());
    testTextIOToBigQuery(
        b -> {
          b.addParameter("pythonExternalTextTransformGcsPath", getGcsPath("pyudf.py"));
          b.addParameter("pythonExternalTextTransformFunctionName", "identity");
          return b;
        });
  }

  private void testTextIOToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    gcsClient.uploadArtifact("schema.json", Resources.getResource(SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact("input.txt", Resources.getResource(INPUT_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING),
                Field.newBuilder(
                        "details",
                        StandardSQLTypeName.STRUCT,
                        Field.of("year", StandardSQLTypeName.INT64),
                        Field.of("summary", StandardSQLTypeName.STRING))
                    .setMode(Mode.NULLABLE)
                    .build()));

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("JSONPath", getGcsPath("schema.json"))
                    .addParameter("inputFilePattern", getGcsPath("input.txt"))
                    .addParameter("outputTable", toTableSpecLegacy(table))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getGcsPath("bq-tmp"))));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(testName);
    assertThatBigQueryRecords(tableRows)
        .hasRecordUnordered(
            ImmutableMap.of(
                "book_id",
                1,
                "title",
                "ABC",
                "details",
                ImmutableMap.of("year", "2023", "summary", "LOREM IPSUM LOREM IPSUM")));
  }
}
