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
import java.util.Comparator;
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
import org.apache.beam.it.jdbc.MySQLResourceManager;
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

/** Integration test for {@link IcebergToMySqlYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToMySqlYaml.class)
@RunWith(JUnit4.class)
public class IcebergToMySqlYamlIT extends TemplateTestBase {

  private MySQLResourceManager mySQLResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;
  private static final Logger LOG = LoggerFactory.getLogger(MySqlToIcebergYamlIT.class);

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();

    warehouseGcsResourceManager =
        GcsResourceManager.builder(getClass().getSimpleName(), credentials).build();
    warehouseGcsResourceManager.registerTempDir(NAMESPACE);
    LOG.info("warehouse bucket created {}", warehouseGcsResourceManager.getBucket());

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testIcebergToMySql() throws IOException {

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


    // MySql setup
    String tableName = "source_table";
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INTEGER");
    columns.put("name", "VARCHAR(255)");
    columns.put("active", "INTEGER");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    mySQLResourceManager.createTable(tableName, schema);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("jdbcUrl", mySQLResourceManager.getUri())
            .addParameter("username", mySQLResourceManager.getUsername())
            .addParameter("password", mySQLResourceManager.getPassword())
            .addParameter("location", tableName);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    // Assert
    assertThatResult(result).isLaunchFinished();
    List<Map<String,Object>> mySqlRecords = mySQLResourceManager.readTable(tableName);
    LOG.info("MySql records: {}", mySqlRecords);
    assertNotNull(mySqlRecords);
    assertEquals(3, mySqlRecords.size());

    mySqlRecords.sort(Comparator.comparingInt(r -> (Integer) r.get("id")));
    icebergRecords.sort(Comparator.comparingInt(r -> (Integer) r.get("id")));
    // Verify records
    assertEquals(mySqlRecords, icebergRecords);
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
