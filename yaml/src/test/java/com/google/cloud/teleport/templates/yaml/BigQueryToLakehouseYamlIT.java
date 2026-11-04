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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToLakehouseYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(BigQueryToLakehouseYaml.class)
@RunWith(JUnit4.class)
public class BigQueryToLakehouseYamlIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryResourceManager;
  private IcebergResourceManager icebergResourceManager;

  private static final String BQ_SOURCE_TABLE_NAME = "source_table";
  private static final String CATALOG_NAME = "hadoop_catalog";
  private final String namespace =
      "bigquery_lakehouse_ns_" + UUID.randomUUID().toString().replace("-", "");
  private static final String LAKEHOUSE_TABLE_NAME = "lakehouse_table";
  private final String lakehouseTableIdentifier = namespace + "." + LAKEHOUSE_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    gcsClient.registerTempDir(namespace);

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager, icebergResourceManager);
  }

  @Test
  public void testBigQueryToLakehouse() throws Exception {
    // 1. Arrange: Create BigQuery source table and destination Lakehouse table
    Schema bqSchema =
        Schema.of(
            Field.newBuilder("id", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("state", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("price", StandardSQLTypeName.FLOAT64)
                .setMode(Field.Mode.REQUIRED)
                .build());
    bigQueryResourceManager.createDataset(REGION);
    TableId bqTable = bigQueryResourceManager.createTable(BQ_SOURCE_TABLE_NAME, bqSchema);
    bigQueryResourceManager.write(
        BQ_SOURCE_TABLE_NAME,
        List.of(RowToInsert.of(Map.of("id", "007", "state", "CA", "price", 26.23))));

    icebergResourceManager.createNamespace(namespace);
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "state", Types.StringType.get()),
            Types.NestedField.required(3, "price", Types.DoubleType.get()));
    icebergResourceManager.createTable(lakehouseTableIdentifier, icebergSchema);

    // 2. Act: Configure options and launch template
    String catalogPropertiesJson = new JSONObject(getCatalogProperties()).toString();
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", toTableSpecLegacy(bqTable))
            .addParameter("lakehouseTable", lakehouseTableIdentifier)
            .addParameter("lakehouseCatalogName", CATALOG_NAME)
            .addParameter("lakehouseCatalogProperties", catalogPropertiesJson);

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // 3. Assert
    assertThatResult(result).isLaunchFinished();

    List<Record> records = icebergResourceManager.read(lakehouseTableIdentifier);
    assertEquals(1, records.size());

    Record record = records.get(0);
    assertEquals("007", record.getField("id"));
    assertEquals("CA", record.getField("state"));
    assertEquals(26.23, record.getField("price"));
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
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
