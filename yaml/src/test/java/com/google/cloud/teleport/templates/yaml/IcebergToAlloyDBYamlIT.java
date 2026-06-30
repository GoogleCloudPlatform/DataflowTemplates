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
import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToAlloyDBYaml.class)
@RunWith(JUnit4.class)
public class IcebergToAlloyDBYamlIT extends TemplateTestBase {

  private PostgresResourceManager postgresResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergToAlloyDBYamlIT.class);

  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;
  private static final String ALLOYDB_TABLE_NAME = "alloydb_table";

  @Before
  public void setUp() throws IOException {
    postgresResourceManager = PostgresResourceManager.builder(testName).build();

    gcsClient.registerTempDir(NAMESPACE);

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(postgresResourceManager, icebergResourceManager);
  }

  @Test
  public void testIcebergToAlloyDB() throws IOException {
    // Iceberg setup
    icebergResourceManager.createNamespace(NAMESPACE);
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "active", Types.IntegerType.get()));
    icebergResourceManager.createTable(ICEBERG_TABLE_IDENTIFIER, icebergSchema);

    List<Map<String, Object>> icebergRecords =
        List.of(
            Map.of("id", 1, "name", "Alice", "active", 1),
            Map.of("id", 2, "name", "Bob", "active", 0),
            Map.of("id", 3, "name", "Charlie", "active", 1));
    icebergResourceManager.write(ICEBERG_TABLE_IDENTIFIER, icebergRecords);

    // AlloyDB (Postgres) setup
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INTEGER");
    columns.put("name", "VARCHAR(255)");
    columns.put("active", "INTEGER");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
    postgresResourceManager.createTable(ALLOYDB_TABLE_NAME, schema);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("jdbcUrl", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("alloydbTable", ALLOYDB_TABLE_NAME);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    List<Map<String, Object>> alloyDbRecords =
        postgresResourceManager.readTable(ALLOYDB_TABLE_NAME);
    assertNotNull(alloyDbRecords);
    assertEquals(3, alloyDbRecords.size());

    alloyDbRecords.sort(
        (a, b) -> ((Number) a.get("id")).intValue() - ((Number) b.get("id")).intValue());

    assertEquals(icebergRecords, alloyDbRecords);
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
        "uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
        "warehouse", "gs://" + gcsClient.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
