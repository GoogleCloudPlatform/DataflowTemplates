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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static com.google.cloud.teleport.it.common.TestProperties.getProperty;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.common.utils.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.bigQueryRowsToRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.elasticsearch.ElasticsearchResourceManager;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.utils.BigQueryTestUtil;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToElasticsearch}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigQueryToElasticsearch.class)
@RunWith(JUnit4.class)
public final class BigQueryToElasticsearchIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryClient;
  private ElasticsearchResourceManager elasticsearchResourceManager;

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "20", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  @Before
  public void setup() {
    bigQueryClient =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
    elasticsearchResourceManager =
        ElasticsearchResourceManager.builder(testId).setHost(HOST_IP).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, elasticsearchResourceManager);
  }

  @Test
  public void testBigQueryToElasticsearch() throws IOException {
    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(testName, bigQuerySchema);
    bigQueryClient.write(testName, bigQueryRows);
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputTableSpec", toTableSpecLegacy(table))
                .addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_dlq")
                .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
                .addParameter("index", indexName)
                .addParameter("apiKey", "elastic"));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(20);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(bigQueryRowsToRecords(bigQueryRows));
  }

  @Test
  public void testBigQueryToElasticsearchQuery() throws IOException {
    // Arrange
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(testName, bigQuerySchema);
    bigQueryClient.write(testName, bigQueryRows);
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputTableSpec", toTableSpecLegacy(table))
                .addParameter(
                    "query", "SELECT * FROM `" + toTableSpecLegacy(table).replace(':', '.') + "`")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_dlq")
                .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
                .addParameter("index", indexName)
                .addParameter("apiKey", "elastic"));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(20);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(bigQueryRowsToRecords(bigQueryRows));
  }

  @Test
  public void testBigQueryToElasticsearchUdf() throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.name = data.name.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");
    Schema bigQuerySchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("name", StandardSQLTypeName.STRING));
    List<RowToInsert> bigQueryRows =
        List.of(
            RowToInsert.of(Map.of("id", 1, "name", "Dataflow")),
            RowToInsert.of(Map.of("id", 2, "name", "Pub/Sub")));
    TableId table = bigQueryClient.createTable(testName, bigQuerySchema);
    bigQueryClient.write(testName, bigQueryRows);
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputTableSpec", toTableSpecLegacy(table))
                .addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_dlq")
                .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
                .addParameter("index", indexName)
                .addParameter("apiKey", "elastic")
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(2);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(
            List.of(Map.of("id", 1, "name", "DATAFLOW"), Map.of("id", 2, "name", "PUB/SUB")));
  }
}
