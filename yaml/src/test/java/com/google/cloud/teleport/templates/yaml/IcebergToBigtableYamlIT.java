/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link IcebergToBigtableYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToBigtableYaml.class)
@RunWith(JUnit4.class)
public final class IcebergToBigtableYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToBigtableYamlIT.class);

  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String ICEBERG_TABLE_NAME = "source_table";

  private final String namespace =
      "iceberg_bigtable_ns_" + UUID.randomUUID().toString().replace("-", "");
  private final String icebergTableIdentifier = namespace + "." + ICEBERG_TABLE_NAME;

  private IcebergResourceManager icebergResourceManager;
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setUp() throws IOException {
    gcsClient.registerTempDir(namespace);

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
    icebergResourceManager.createNamespace(namespace);

    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(icebergResourceManager, bigtableResourceManager);
  }

  @Test
  public void testIcebergToBigtable() throws IOException {
    // Arrange: Re-invoke createNamespace as a propagation barrier for BigLake REST catalog.
    icebergResourceManager.createNamespace(namespace);
    LOG.info("Namespace '{}' created/verified successfully", namespace);

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "timestamp_micros", Types.LongType.get()));

    icebergResourceManager.createTable(icebergTableIdentifier, icebergSchema);

    List<Map<String, Object>> icebergRecords =
        List.of(
            Map.of("id", "row1", "name", "Alice", "timestamp_micros", 1000L),
            Map.of("id", "row2", "name", "Bob", "timestamp_micros", 2000L),
            Map.of("id", "row3", "name", "Charlie", "timestamp_micros", 3000L),
            // Missing 'name' (null), which will fail MapToFields transformation and route to DLQ
            Map.of("id", "bad_row", "timestamp_micros", 4000L));

    icebergResourceManager.write(icebergTableIdentifier, icebergRecords);
    LOG.info("Iceberg source table populated with {} records", icebergRecords.size());

    String tableId = generateTableId("test_table");
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf1"));

    String errorPath = getGcsPath("errors/error.json");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", icebergTableIdentifier)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter("catalogProperties", new JSONObject(getCatalogProperties()).toString())
            .addParameter("language", "python")
            .addParameter(
                "fields",
                "{"
                    + "\"key\": {\"expression\": \"id.encode('utf-8')\", \"output_type\": \"bytes\"},"
                    + "\"type\": {\"expression\": \"'SetCell'\", \"output_type\": \"string\"},"
                    + "\"family_name\": {\"expression\": \"'cf1'\", \"output_type\": \"string\"},"
                    + "\"column_qualifier\": {\"expression\": \"b'cq1'\", \"output_type\": \"bytes\"},"
                    + "\"value\": {\"expression\": \"name.encode('utf-8')\", \"output_type\": \"bytes\"},"
                    + "\"timestamp_micros\": {\"expression\": \"timestamp_micros\", \"output_type\": \"integer\"}"
                    + "}")
            .addParameter("projectId", PROJECT)
            .addParameter("instanceId", bigtableResourceManager.getInstanceId())
            .addParameter("tableId", tableId)
            .addParameter("errorPath", errorPath);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> tableRows = bigtableResourceManager.readTable(tableId);
    assertThat(tableRows).hasSize(3);

    Map<String, Row> rowMap =
        tableRows.stream()
            .collect(Collectors.toMap(row -> row.getKey().toStringUtf8(), row -> row));

    assertCell(rowMap, "row1", "cf1", "cq1", "Alice", 1000L);
    assertCell(rowMap, "row2", "cf1", "cq1", "Bob", 2000L);
    assertCell(rowMap, "row3", "cf1", "cq1", "Charlie", 3000L);

    List<Artifact> errorArtifacts =
        gcsClient.listArtifacts("errors/", Pattern.compile(".*error\\.json.*"));
    assertThat(errorArtifacts).isNotEmpty();

    StringBuilder dlqContent = new StringBuilder();
    for (Artifact artifact : errorArtifacts) {
      dlqContent.append(new String(artifact.contents(), StandardCharsets.UTF_8));
    }
    assertThat(dlqContent.toString()).contains("bad_row");
  }

  private void assertCell(
      Map<String, Row> rowMap,
      String rowKey,
      String expectedFamily,
      String expectedQualifier,
      String expectedValue,
      long expectedTimestamp) {
    assertThat(rowMap).containsKey(rowKey);
    Row row = rowMap.get(rowKey);
    assertThat(row.getCells()).hasSize(1);
    assertThat(row.getCells().get(0).getFamily()).isEqualTo(expectedFamily);
    assertThat(row.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo(expectedQualifier);
    assertThat(row.getCells().get(0).getValue().toStringUtf8()).isEqualTo(expectedValue);
    assertThat(row.getCells().get(0).getTimestamp()).isEqualTo(expectedTimestamp);
  }

  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type", "rest",
        "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "warehouse", "gs://" + gcsClient.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
