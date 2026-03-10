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
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
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

/** Integration test for {@link IcebergToSqlYaml} template using existing Cloud SQL. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergToMySqlYaml.class)
@RunWith(JUnit4.class)
public class IcebergToMySqlYamlITLocal extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToMySqlYamlITLocal.class);

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "source_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  // 1. IP for your local machine "via Proxy". This allows the test to setup test data and tables,
  // and then cleanup.
  private static final String LOCAL_PROXY_IP = "127.0.0.1";

  // 2. IP for Dataflow Workers to connect to Cloud SQL when the job runs
  private static final String CLOUD_SQL_REAL_IP = "10.74.192.17";

  private static final String CLOUD_SQL_USERNAME = "root";
  private static final String CLOUD_SQL_PASSWORD = "test";
  private static final int CLOUD_SQL_PORT = 3306;

  private static final String TABLE_NAME = "target_table";

  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;
  private CloudMySQLResourceManager mySQLResourceManager;

  @Before
  public void setUp() throws IOException {
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

    mySQLResourceManager =
        (CloudMySQLResourceManager)
            CloudMySQLResourceManager.builder(testName)
                .setUsername(CLOUD_SQL_USERNAME)
                .setPassword(CLOUD_SQL_PASSWORD)
                .setHost(LOCAL_PROXY_IP)
                .setPort(CLOUD_SQL_PORT)
                .build();
  }

  @After
  public void tearDown() {
    // Clean up other resources
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testIcebergToMySql() throws Exception {
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

    // SQL setup, create table using LOCAL connection "Proxy"
    LOG.info("Creating SQL table: {}", TABLE_NAME);
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INTEGER");
    columns.put("name", "VARCHAR(255)");
    columns.put("active", "INTEGER");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
    try {
      mySQLResourceManager.createTable(TABLE_NAME, schema);
      LOG.info("Successfully created table: {}", TABLE_NAME);
    } catch (Exception e) {
      LOG.error("Failed to create SQL table: {}", TABLE_NAME, e);
      throw e;
    }

    // Pipeline execution
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter(
                "jdbcUrl",
                getJdbcUrl(
                    CLOUD_SQL_REAL_IP,
                    CLOUD_SQL_PORT,
                    mySQLResourceManager.getDatabaseName())) // <--- USE THE REAL IP FOR DATAFLOW
            .addParameter("username", CLOUD_SQL_USERNAME)
            .addParameter("password", CLOUD_SQL_PASSWORD)
            .addParameter(
                "location",
                String.format("%s.%s", mySQLResourceManager.getDatabaseName(), TABLE_NAME));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Read records using LOCAL connection "Proxy"
    List<Map<String, Object>> sqlRecords = readMySqlTable(TABLE_NAME);
    sqlRecords.sort(
        (a, b) -> ((Number) a.get("id")).intValue() - ((Number) b.get("id")).intValue());

    assertEquals(
        "Expected 3 records in SQL table, got: " + sqlRecords.size(), 3, sqlRecords.size());
    assertEquals(sqlRecords, icebergRecords);
    LOG.info("All assertions passed. Records successfully transferred from Iceberg to SQL.");
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

  // Reads all records from the SQL table using the LOCAL PROXY connection.
  private List<Map<String, Object>> readMySqlTable(String tableName) throws Exception {
    try {
      LOG.info("Reading records from SQL table: {}", tableName);
      List<Map<String, Object>> mySqlRecords = mySQLResourceManager.readTable(tableName);
      LOG.info("MySql records: {}", mySqlRecords);
      return mySqlRecords;
    } catch (Exception e) {
      LOG.error("Failed to read from SQL table: {}", tableName, e);
      throw e;
    }
  }

  private String getJdbcUrl(String ip, int port, String database) {
    return String.format("jdbc:mysql://%s:%d/%s", ip, port, database);
  }
}
