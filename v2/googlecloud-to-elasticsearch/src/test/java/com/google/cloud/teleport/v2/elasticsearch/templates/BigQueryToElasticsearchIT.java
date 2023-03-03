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

import static com.google.cloud.teleport.it.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.TestProperties.getProperty;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.BigQueryTestUtils;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.elasticsearch.DefaultElasticsearchResourceManager;
import com.google.cloud.teleport.it.elasticsearch.ElasticsearchResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
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
      Integer.parseInt(getProperty("numRows", "50", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "50", TestProperties.Type.PROPERTY)));

  @Before
  public void setup() {
    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    elasticsearchResourceManager =
        DefaultElasticsearchResourceManager.builder(testId).setHost(HOST_IP).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, elasticsearchResourceManager);
  }

  @Test
  public void testBigQueryToElasticsearch() throws IOException {
    // Arrange
    String tableName = testName;
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtils.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(tableName, bigQuerySchema);
    bigQueryClient.write(tableName, bigQueryRows);
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputTableSpec", toTableSpec(table))
            .addParameter("outputDeadletterTable", toTableSpec(table) + "_dlq")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(50);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(
            bigQueryRows.stream().map(RowToInsert::getContent).collect(Collectors.toList()));
  }

  @Test
  public void testBigQueryToElasticsearchQuery() throws IOException {
    // Arrange
    String tableName = testName;
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtils.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(tableName, bigQuerySchema);
    bigQueryClient.write(tableName, bigQueryRows);
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputTableSpec", toTableSpec(table))
            .addParameter("query", "SELECT * FROM `" + toTableSpec(table).replace(':', '.') + "`")
            .addParameter("outputDeadletterTable", toTableSpec(table) + "_dlq")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(50);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(
            bigQueryRows.stream().map(RowToInsert::getContent).collect(Collectors.toList()));
  }
}
