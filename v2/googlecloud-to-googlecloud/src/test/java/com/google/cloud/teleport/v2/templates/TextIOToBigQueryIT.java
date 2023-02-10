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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link TextIOToBigQuery} (GCS_Text_to_BigQuery_Flex).
 *
 * <p>Example Usage:
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextIOToBigQuery.class)
@RunWith(JUnit4.class)
public final class TextIOToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TextIOToBigQueryIT.class);

  private static final String SCHEMA_PATH = "TextIOToBigQueryTest/schema.json";
  private static final String INPUT_PATH = "TextIOToBigQueryTest/input.txt";
  private static final String UDF_PATH = "TextIOToBigQueryTest/udf.js";
  private static final Map<String, Object> EXPECTED = ImmutableMap.of("book_id", 1, "title", "ABC");

  private BigQueryResourceManager bigQueryClient;

  @Rule public final TestName testName = new TestName();

  @Before
  public void setup() throws IOException {
    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void tearDownClass() {
    try {
      bigQueryClient.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete BigQuery resources.", e);
      throw new IllegalStateException("Failed to delete resources. Check above for errors.");
    }
  }

  @Test
  public void testTextIOToBigQuery() throws IOException {
    testTextIOToBigQuery(Function.identity());
  }

  @Test
  public void testTextIOToBigQueryWithStorageApi() throws IOException {
    testTextIOToBigQuery(b -> b.addParameter("useStorageWriteApi", "true"));
  }

  private void testTextIOToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    String bqTable = testName.getMethodName();

    artifactClient.uploadArtifact("schema.json", Resources.getResource(SCHEMA_PATH).getPath());
    artifactClient.uploadArtifact("input.txt", Resources.getResource(INPUT_PATH).getPath());
    artifactClient.uploadArtifact("udf.js", Resources.getResource(UDF_PATH).getPath());

    bigQueryClient.createDataset(REGION);
    TableId table =
        bigQueryClient.createTable(
            bqTable,
            Schema.of(
                Field.of("book_id", StandardSQLTypeName.INT64),
                Field.of("title", StandardSQLTypeName.STRING)));

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("JSONPath", getGcsPath("schema.json"))
                    .addParameter("inputFilePattern", getGcsPath("input.txt"))
                    .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                    .addParameter("javascriptTextTransformFunctionName", "identity")
                    .addParameter("outputTable", toTableSpec(table))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getGcsPath("bq-tmp"))));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult tableRows = bigQueryClient.readTable(bqTable);
    assertThatRecords(tableRows).hasRecord(EXPECTED);
  }
}
