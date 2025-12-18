/*
 * Copyright (C) 2024 Google LLC
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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
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

/** Integration test for {@link IcebergToPostgresYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToPostgresYaml.class)
@RunWith(JUnit4.class)
public class IcebergToPostgresYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToPostgresYamlIT.class);

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "source_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  // Postgres Setup
  private static final String POSTGRES_TABLE_NAME = "target_table";
  private static final String WRITE_STATEMENT =
      "INSERT INTO " + POSTGRES_TABLE_NAME + " (id, name, active) VALUES (?, ?, ?)";

  private PostgresResourceManager postgresResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;

  @Before
  public void setUp() throws IOException {
    // Initialize Postgres resource manager
    postgresResourceManager = PostgresResourceManager.builder(testName).build();

    // Initialize GCS for Iceberg warehouse
    warehouseGcsResourceManager =
        GcsResourceManager.builder(getClass().getSimpleName(), credentials).build();
    warehouseGcsResourceManager.registerTempDir(NAMESPACE);
    LOG.info("Warehouse bucket created: {}", warehouseGcsResourceManager.getBucket());

    // Initialize Iceberg resource manager
    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        postgresResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testIcebergToPostgres() throws IOException {
    // Iceberg setup
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
    LOG.info("Iceberg source table populated with {} records", icebergRecords.size());

    // Postgres setup
    HashMap<String, String> postgresColumns = new HashMap<>();
    postgresColumns.put("id", "INTEGER");
    postgresColumns.put("name", "VARCHAR(255)");
    postgresColumns.put("active", "INTEGER");
    JDBCResourceManager.JDBCSchema postgresSchema =
        new JDBCResourceManager.JDBCSchema(postgresColumns, "id");

    postgresResourceManager.createTable(POSTGRES_TABLE_NAME, postgresSchema);

    // Pipeline execution
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("jdbcUrl", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("location", POSTGRES_TABLE_NAME);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Read records from Postgres table
    List<Map<String, Object>> postgresRecords =
        postgresResourceManager.readTable(POSTGRES_TABLE_NAME);
    LOG.info("Postgres target table contains {} records", postgresRecords.size());

    assertNotNull("Postgres records should not be null", postgresRecords);
    assertEquals(
        "Expected 3 records in Postgres table, got: " + postgresRecords.size(),
        3,
        postgresRecords.size());

    postgresRecords.sort(
        (a, b) -> ((Number) a.get("id")).intValue() - ((Number) b.get("id")).intValue());

    Map<String, Object> record1 = postgresRecords.get(0);
    assertEquals("Record 1 id should be 1", 1, ((Number) record1.get("id")).intValue());
    assertEquals("Record 1 name should be Alice", "Alice", record1.get("name"));
    assertEquals("Record 1 active should be 1", 1, ((Number) record1.get("active")).intValue());

    Map<String, Object> record2 = postgresRecords.get(1);
    assertEquals("Record 2 id should be 2", 2, ((Number) record2.get("id")).intValue());
    assertEquals("Record 2 name should be Bob", "Bob", record2.get("name"));
    assertEquals("Record 2 active should be 0", 0, ((Number) record2.get("active")).intValue());

    Map<String, Object> record3 = postgresRecords.get(2);
    assertEquals("Record 3 id should be 3", 3, ((Number) record3.get("id")).intValue());
    assertEquals("Record 3 name should be Charlie", "Charlie", record3.get("name"));
    assertEquals("Record 3 active should be 1", 1, ((Number) record3.get("active")).intValue());

    LOG.info("All assertions passed. Records successfully transferred from Iceberg to Postgres.");
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  /**
   * Build catalog properties for Iceberg warehouse backed by GCS.
   *
   * <p>Uses BigLake REST catalog for Iceberg metadata and GCS for warehouse storage.
   *
   * @return Map of catalog properties required by Iceberg
   */
  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type", "rest",
        "uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
        "warehouse", "gs://" + warehouseGcsResourceManager.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
