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
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.iceberg.data.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PostgresToIcebergYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PostgresToIcebergYaml.class)
@RunWith(JUnit4.class)
public class PostgresToIcebergYamlIT extends TemplateTestBase {

  private static final String READ_QUERY = "SELECT * FROM %s";

  private PostgresResourceManager postgresResourceManager;
  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;
  private static final Logger LOG = LoggerFactory.getLogger(PostgresToIcebergYamlIT.class);

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;
  private static final String WAREHOUSE_BUCKET = "cloud-teleport-testing-it-gitactions";
  private static final String WAREHOUSE = "gs://" + WAREHOUSE_BUCKET;

  @Before
  public void setUp() throws IOException {
    postgresResourceManager = PostgresResourceManager.builder(testName).build();

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
    warehouseGcsResourceManager =
        GcsResourceManager.builder(WAREHOUSE_BUCKET, getClass().getSimpleName(), credentials)
            .build();
    warehouseGcsResourceManager.registerTempDir(NAMESPACE);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        postgresResourceManager, icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testPostgresToIceberg() throws IOException {
    // Postgres setup
    String tableName = "source_table";
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INTEGER");
    columns.put("active", "INTEGER");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    postgresResourceManager.createTable(tableName, schema);

    List<Map<String, Object>> records =
        List.of(Map.of("id", 1, "active", 1), Map.of("id", 2, "active", 0));
    postgresResourceManager.write(tableName, records);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jdbcUrl", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("readQuery", String.format(READ_QUERY, tableName))
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString());

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    // Assert
    assertThatResult(result).isLaunchFinished();
    List<Record> icebergRecords = icebergResourceManager.read(ICEBERG_TABLE_IDENTIFIER);
    LOG.info("Iceberg records: {}", icebergRecords);
    assertNotNull(icebergRecords);
    assertEquals(2, icebergRecords.size());
    icebergRecords.sort(Comparator.comparingInt(r -> (Integer) r.getField("id")));
    // Verify records
    Record actualRecord1 = icebergRecords.get(0);
    Record actualRecord2 = icebergRecords.get(1);
    assertEquals(1, actualRecord1.getField("id"));
    assertEquals(1, actualRecord1.getField("active"));
    assertEquals(2, actualRecord2.getField("id"));
    assertEquals(0, actualRecord2.getField("active"));
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
        "warehouse", WAREHOUSE,
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
