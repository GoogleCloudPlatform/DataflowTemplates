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

import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextToBigQueryStreaming} (GCS_Text_to_BigQuery_Flex). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextToBigQueryStreaming.class)
@RunWith(JUnit4.class)
public class TextToBigQueryStreamingIT extends TemplateTestBase {

  private static final String SCHEMA_PATH = "TextToBigQueryStreamingIT/schema.json";
  private static final String INPUT_PATH = "TextToBigQueryStreamingIT/input.txt";
  private static final String UDF_PATH = "TextToBigQueryStreamingIT/udf.js";
  private static final Map<String, Object> EXPECTED = ImmutableMap.of("BOOK_ID", 1, "TITLE", "ABC");

  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient);
  }

  @Test
  public void testTextToBigQuery() throws IOException {
    testTextToBigQuery(Function.identity()); // no extra parameters to set
  }

  @Test
  public void testTextToBigQueryWithStorageApi() throws IOException {
    testTextToBigQuery(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "5"));
  }

  private void testTextToBigQuery(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    String bqTable = testName;
    gcsClient.uploadArtifact("schema.json", Resources.getResource(SCHEMA_PATH).getPath());
    gcsClient.uploadArtifact("udf.js", Resources.getResource(UDF_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    TableId tableId =
        bigQueryClient.createTable(
            bqTable,
            Schema.of(
                Field.of("BOOK_ID", StandardSQLTypeName.INT64),
                Field.of("TITLE", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("JSONPath", getGcsPath("schema.json"))
                    .addParameter("inputFilePattern", getGcsPath("input.txt"))
                    .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                    .addParameter("javascriptTextTransformFunctionName", "identity")
                    .addParameter("outputTable", toTableSpecLegacy(tableId))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getGcsPath("bq-tmp"))));
    assertThatPipeline(info).isRunning();

    gcsClient.uploadArtifact("input.txt", Resources.getResource(INPUT_PATH).getPath());

    Result result =
        pipelineOperator()
            // drain doesn't seem to work with the TextIO GCS files watching that the template uses
            .waitForConditionAndCancel(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(1).build());

    // Assert
    assertThatResult(result).meetsConditions();
    assertThatBigQueryRecords(bigQueryClient.readTable(bqTable)).hasRecord(EXPECTED);
  }
}
