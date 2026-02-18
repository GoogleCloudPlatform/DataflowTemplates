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

import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
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
import org.apache.beam.it.common.utils.ClasspathResourceManager;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
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

/** Integration test for {@link IcebergToOracleYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToOracleYaml.class)
@RunWith(JUnit4.class)
public class IcebergToOracleYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToOracleYamlIT.class);

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "source_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  // Oracle Setup
  private static final String ORACLE_TABLE_NAME = "target_table";
  private static final String ORACLE_DRIVER_GCS_PATH = "libs/ojdbc17.jar";

  private GcsResourceManager gcsResourceManager; // For artifacts
  private OracleResourceManager oracleResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager; // For Iceberg warehouse

  @Before
  public void setUp() throws IOException {
    // Initialize GCS resource manager for artifacts
    // artifactBucket is inherited from TemplateTestBase
    gcsResourceManager =
        GcsResourceManager.builder(testName + "-artifacts", credentials, artifactBucket).build();

    // Initialize Oracle resource manager
    oracleResourceManager = OracleResourceManager.builder(testName).build();

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

    // Upload Oracle JDBC Driver to GCS
    try {
      ClasspathResourceManager.create()
          .uploadArtifact(artifactBucket, ORACLE_DRIVER_GCS_PATH, "ojdbc17.jar");
      LOG.info("Uploaded Oracle driver to gs://{}/{}", artifactBucket, ORACLE_DRIVER_GCS_PATH);
    } catch (Exception e) {
      throw new IOException("Failed to upload Oracle driver", e);
    }
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        oracleResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testIcebergToOracle() throws IOException {
    // Iceberg setup

    // Create namespace in the REST catalog
    icebergResourceManager.createNamespace(NAMESPACE);
    LOG.info("Namespace '{}' created successfully", NAMESPACE);

    // Define Iceberg table schema
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "active", Types.IntegerType.get()));

    // Create Iceberg table
    icebergResourceManager.createTable(ICEBERG_TABLE_IDENTIFIER, icebergSchema);

    List<Map<String, Object>> icebergRecords =
        List.of(
            Map.of("id", 1, "name", "Alice", "active", 1),
            Map.of("id", 2, "name", "Bob", "active", 0),
            Map.of("id", 3, "name", "Charlie", "active", 1));

    icebergResourceManager.write(ICEBERG_TABLE_IDENTIFIER, icebergRecords);
    LOG.info("Iceberg source table populated with {} records", icebergRecords.size());

    // Oracle setup
    HashMap<String, String> oracleColumns = new HashMap<>();
    oracleColumns.put("ID", "INTEGER");
    oracleColumns.put("NAME", "VARCHAR2(255)");
    oracleColumns.put("ACTIVE", "INTEGER");
    JDBCResourceManager.JDBCSchema oracleSchema =
        new JDBCResourceManager.JDBCSchema(oracleColumns, "ID");

    oracleResourceManager.createTable(ORACLE_TABLE_NAME, oracleSchema);

    // Pipeline execution
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("jdbcUrl", oracleResourceManager.getUri())
            .addParameter("username", oracleResourceManager.getUsername())
            .addParameter("password", oracleResourceManager.getPassword())
            .addParameter("driverClassName", "oracle.jdbc.driver.OracleDriver")
            .addParameter("driverJars", "gs://" + artifactBucket + "/" + ORACLE_DRIVER_GCS_PATH)
            .addParameter("jdbcType", "oracle")
            .addParameter("location", ORACLE_TABLE_NAME);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Read records from Oracle table
    List<Map<String, Object>> oracleRecords = oracleResourceManager.readTable(ORACLE_TABLE_NAME);
    LOG.info("Oracle target table contains {} records", oracleRecords.size());

    assertNotNull("Oracle records should not be null", oracleRecords);
    assertEquals(
        "Expected 3 records in Oracle table, got: " + oracleRecords.size(),
        3,
        oracleRecords.size());

    oracleRecords.sort(
        (a, b) -> ((Number) a.get("ID")).intValue() - ((Number) b.get("ID")).intValue());

    Map<String, Object> record1 = oracleRecords.get(0);
    assertEquals("Record 1 id should be 1", 1, ((Number) record1.get("ID")).intValue());
    assertEquals("Record 1 name should be Alice", "Alice", record1.get("NAME"));
    assertEquals("Record 1 active should be 1", 1, ((Number) record1.get("ACTIVE")).intValue());

    Map<String, Object> record2 = oracleRecords.get(1);
    assertEquals("Record 2 id should be 2", 2, ((Number) record2.get("ID")).intValue());
    assertEquals("Record 2 name should be Bob", "Bob", record2.get("NAME"));
    assertEquals("Record 2 active should be 0", 0, ((Number) record2.get("ACTIVE")).intValue());

    Map<String, Object> record3 = oracleRecords.get(2);
    assertEquals("Record 3 id should be 3", 3, ((Number) record3.get("ID")).intValue());
    assertEquals("Record 3 name should be Charlie", "Charlie", record3.get("NAME"));
    assertEquals("Record 3 active should be 1", 1, ((Number) record3.get("ACTIVE")).intValue());

    LOG.info("All assertions passed. Records successfully transferred from Iceberg to Oracle.");
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
        "warehouse", "gs://" + warehouseGcsResourceManager.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
