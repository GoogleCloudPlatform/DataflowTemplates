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
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/** Integration test for {@link PostgresToIcebergYaml} template. */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(PostgresToIcebergYaml.class)
public class PostgresToIcebergYamlIT extends TemplateTestBase {

  private static final String READ_QUERY = "SELECT * FROM %s";
  private static String warehouseLocation;

  private PostgresResourceManager postgresResourceManager;
  private IcebergResourceManager icebergResourceManager;

  @Before
  public void setUp() throws IOException {
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    java.nio.file.Path warehouseDirectory = Files.createTempDirectory("test-warehouse");
    warehouseLocation = "file:" + warehouseDirectory.toString();

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName("hadoop")
            .setCatalogProperties(Map.of("type", "hadoop", "warehouse", warehouseLocation))
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
    columns.put("active", "BOOLEAN");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    postgresResourceManager.createTable(tableName, schema);

    List<Map<String, Object>> records =
        List.of(Map.of("id", 1, "active", true), Map.of("id", 2, "active", false));
    postgresResourceManager.write(tableName, records);

    // Iceberg Setup
    String catalogName = "hadoop_catalog";
    String catalogProperties =
        String.format("{\"type\": \"hadoop\", \"warehouse\": \"%s\"}", warehouseLocation);
    String icebergTableName = "iceberg_table";

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jdbc_url", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("read_query", String.format(READ_QUERY, tableName))
            .addParameter("table", icebergTableName)
            .addParameter("catalog_name", catalogName)
            .addParameter("catalog_properties", catalogProperties);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    List<Record> icebergRecords = icebergResourceManager.read(icebergTableName);
    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    for (Record record : icebergRecords) {
      expectedRecords.add(ImmutableMap.of("id", record.get(0), "active", record.get(2)));
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
