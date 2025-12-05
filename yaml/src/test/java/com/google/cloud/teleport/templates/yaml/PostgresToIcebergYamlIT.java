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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
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
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@TemplateIntegrationTest(PostgresToIcebergYaml.class)
@RunWith(JUnit4.class)
public class PostgresToIcebergYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresToIcebergYamlIT.class);

  private static final String READ_QUERY = "SELECT * FROM %s";
  private static String warehouseLocation;

  private PostgresResourceManager postgresResourceManager;
  private IcebergResourceManager icebergResourceManager;

  // Iceberg Setup
  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String NAMESPACE = "iceberg_namespace";
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private static final String ICEBERG_TABLE_IDENTIFIER = NAMESPACE + "." + ICEBERG_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    warehouseLocation = getGcsBasePath();
    LOG.info("Warehouse Location: {}, {}", warehouseLocation, getGcsBasePath());
    Map<String, String> catalogHadoopConf =
        Map.of("fs.gs.project.id", PROJECT, "fs.gs.auth.type", "APPLICATION_DEFAULT");

    Map<String, String> catalogProperties =
        Map.of(
            "type",
            "hadoop",
            "warehouse",
            warehouseLocation,
            "io-impl",
            "org.apache.iceberg.gcp.gcs.GCSFileIO");
    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(catalogProperties)
            .setConfigProperties(catalogHadoopConf)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(postgresResourceManager, icebergResourceManager);
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

    String catalogProperties =
        String.format(
            "{\"type\": \"hadoop\", \"warehouse\": \"%s\", \"io-impl\": \"org.apache.iceberg.gcp.gcs.GCSFileIO\"}",
            warehouseLocation);

    String configProperties =
        String.format(
            "{\"fs.gs.project.id\": \"%s\", \"fs.gs.auth.type\": \"APPLICATION_DEFAULT\"}",
            PROJECT);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jdbcUrl", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("readQuery", String.format(READ_QUERY, tableName))
            .addParameter("table", ICEBERG_TABLE_IDENTIFIER)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter("catalogProperties", catalogProperties)
            .addParameter("configProperties", configProperties);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    LOG.info("Dataflow job finished successfully");
    List<Record> icebergRecords = icebergResourceManager.read(ICEBERG_TABLE_IDENTIFIER);
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    for (Record record : icebergRecords) {
      expectedRecords.add(
          ImmutableMap.of(
              "id",
              Objects.requireNonNull(record.get(0)),
              "active",
              Objects.requireNonNull(record.get(2))));
    }
    assertThat(expectedRecords).containsExactlyElementsIn(records);
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }
}
