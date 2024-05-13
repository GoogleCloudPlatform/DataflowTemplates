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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.elasticsearch.ElasticsearchResourceManager;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link GCSToElasticsearch}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GCSToElasticsearch.class)
@RunWith(JUnit4.class)
public final class GCSToElasticsearchIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryClient;
  private ElasticsearchResourceManager elasticsearchResourceManager;

  @Before
  public void setup() {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    elasticsearchResourceManager = ElasticsearchResourceManager.builder(testId).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, elasticsearchResourceManager);
  }

  @Test
  @TemplateIntegrationTest(value = GCSToElasticsearch.class, template = "GCS_to_Elasticsearch")
  public void testElasticsearchCsvWithoutHeadersJS() throws IOException {
    testElasticsearchCsvWithoutHeaders(
        "no_header_10.csv",
        "elasticUdf.js",
        List.of(Map.of("id", "001", "state", "CA", "price", 3.65)));
  }

  @Test
  @TemplateIntegrationTest(
      value = GCSToElasticsearch.class,
      template = "GCS_to_Elasticsearch_Xlang")
  public void testElasticsearchCsvWithoutHeadersPython() throws IOException {
    testElasticsearchCsvWithoutHeadersAndPythonUdf(
        "no_header_10.csv",
        "elasticPyUdf.py",
        List.of(Map.of("id", "001", "state", "CA", "price", 3.65)));
  }

  @Test
  @TemplateIntegrationTest(value = GCSToElasticsearch.class, template = "GCS_to_Elasticsearch")
  public void testElasticsearchCsvWithoutHeadersES6() throws IOException {
    testElasticsearchCsvWithoutHeaders(
        "no_header_10.csv",
        "elasticUdfES6.js",
        List.of(Map.of("id", "001", "state", "CA", "price", 3.65)));
  }

  public void testElasticsearchCsvWithoutHeaders(
      String csvFileName, String udfFileName, List<Map<String, Object>> expectedRecords)
      throws IOException {
    // Arrange
    gcsClient.uploadArtifact(
        "input/" + csvFileName,
        Resources.getResource("GCSToElasticsearch/" + csvFileName).getPath());
    gcsClient.uploadArtifact(
        "input/" + udfFileName,
        Resources.getResource("GCSToElasticsearch/" + udfFileName).getPath());
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);
    bigQueryClient.createDataset(REGION);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileSpec", getGcsPath("input") + "/*.csv")
            .addParameter("inputFormat", "csv")
            .addParameter("containsHeaders", "false")
            .addParameter("deadletterTable", PROJECT + ":" + bigQueryClient.getDatasetId() + ".dlq")
            .addParameter("delimiter", ",")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("javascriptTextTransformGcsPath", getGcsPath("input/" + udfFileName))
            .addParameter("javascriptTextTransformFunctionName", "transform")
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(10);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(expectedRecords);
  }

  public void testElasticsearchCsvWithoutHeadersAndPythonUdf(
      String csvFileName, String udfFileName, List<Map<String, Object>> expectedRecords)
      throws IOException {
    // Arrange
    gcsClient.uploadArtifact(
        "input/" + csvFileName,
        Resources.getResource("GCSToElasticsearch/" + csvFileName).getPath());
    gcsClient.uploadArtifact(
        "input/" + udfFileName,
        Resources.getResource("GCSToElasticsearch/" + udfFileName).getPath());
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);
    bigQueryClient.createDataset(REGION);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileSpec", getGcsPath("input") + "/*.csv")
            .addParameter("inputFormat", "csv")
            .addParameter("containsHeaders", "false")
            .addParameter("deadletterTable", PROJECT + ":" + bigQueryClient.getDatasetId() + ".dlq")
            .addParameter("delimiter", ",")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("pythonExternalTextTransformGcsPath", getGcsPath("input/" + udfFileName))
            .addParameter("pythonExternalTextTransformFunctionName", "transform")
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(10);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(expectedRecords);
  }

  @Test
  @TemplateIntegrationTest(value = GCSToElasticsearch.class, template = "GCS_to_Elasticsearch")
  public void testElasticsearchCsvWithHeaders() throws IOException {
    // Arrange
    gcsClient.uploadArtifact(
        "input/with_headers_10.csv",
        Resources.getResource("GCSToElasticsearch/with_headers_10.csv").getPath());
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);
    bigQueryClient.createDataset(REGION);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileSpec", getGcsPath("input") + "/*.csv")
            .addParameter("inputFormat", "csv")
            .addParameter("containsHeaders", "true")
            .addParameter("deadletterTable", PROJECT + ":" + bigQueryClient.getDatasetId() + ".dlq")
            .addParameter("delimiter", ",")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(10);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(List.of(Map.of("id", "001", "state", "CA", "price", 3.65)));
  }

  @Test
  @TemplateIntegrationTest(value = GCSToElasticsearch.class, template = "GCS_to_Elasticsearch")
  public void testElasticsearchCsvWithoutHeadersWithJsonSchema() throws IOException {
    // Arrange
    gcsClient.uploadArtifact(
        "input/no_header_10.csv",
        Resources.getResource("GCSToElasticsearch/no_header_10.csv").getPath());
    gcsClient.createArtifact(
        "input/schema.json",
        "[{\n"
            + "    \"name\": \"id\",\n"
            + "    \"type\": \"STRING\"\n"
            + "}, {\n"
            + "    \"name\": \"state\",\n"
            + "    \"type\": \"STRING\"\n"
            + "}, {\n"
            + "    \"name\": \"price\",\n"
            + "    \"type\": \"DOUBLE\"\n"
            + "}]");
    String indexName = createJobName(testName);
    elasticsearchResourceManager.createIndex(indexName);
    bigQueryClient.createDataset(REGION);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFileSpec", getGcsPath("input") + "/*.csv")
            .addParameter("inputFormat", "csv")
            .addParameter("containsHeaders", "false")
            .addParameter("jsonSchemaPath", getGcsPath("input/schema.json"))
            .addParameter("deadletterTable", PROJECT + ":" + bigQueryClient.getDatasetId() + ".dlq")
            .addParameter("delimiter", ",")
            .addParameter("connectionUrl", elasticsearchResourceManager.getUri())
            .addParameter("index", indexName)
            .addParameter("apiKey", "elastic");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(elasticsearchResourceManager.count(indexName)).isEqualTo(10);
    assertThatRecords(elasticsearchResourceManager.fetchAll(indexName))
        .hasRecordsUnordered(List.of(Map.of("id", "001", "state", "CA", "price", 3.65)));
  }
}
