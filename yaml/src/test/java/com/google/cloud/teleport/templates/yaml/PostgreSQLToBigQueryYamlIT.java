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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PostgreSQLToBigQueryYaml.class)
@RunWith(JUnit4.class)
public class PostgreSQLToBigQueryYamlIT extends TemplateTestBase {

  private PostgresResourceManager postgresResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final String TABLE_NAME = "users";

  @Before
  public void setUp() {
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(postgresResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testPostgreSQLToBigQuery() throws IOException {
    // 1. Setup PostgreSQL database and insert some test data
    JDBCResourceManager.JDBCSchema jdbcSchema =
        new JDBCResourceManager.JDBCSchema(Map.of("id", "INTEGER", "name", "VARCHAR(100)"), "id");

    postgresResourceManager.createTable(TABLE_NAME, jdbcSchema);
    postgresResourceManager.write(
        TABLE_NAME,
        java.util.List.of(Map.of("id", 1, "name", "Alice"), Map.of("id", 2, "name", "Bob")));

    // 2. Setup BigQuery target dataset and table name
    bigQueryResourceManager.createDataset(REGION);
    TableId bqTable = TableId.of(PROJECT, bigQueryResourceManager.getDatasetId(), TABLE_NAME);

    // 3. Launch the Pipeline
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jdbcUrl", postgresResourceManager.getUri())
            .addParameter("username", postgresResourceManager.getUsername())
            .addParameter("password", postgresResourceManager.getPassword())
            .addParameter("postgresTable", TABLE_NAME)
            .addParameter(
                "table",
                bqTable.getProject() + ":" + bqTable.getDataset() + "." + bqTable.getTable());

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // 4. Wait for job to finish and assert results
    Result result = pipelineOperator().waitUntilDone(createConfig(info));
    assertThatResult(result).isLaunchFinished();

    // 5. Verify records appear in BigQuery
    TableResult records = bigQueryResourceManager.readTable(TABLE_NAME);
    org.junit.Assert.assertEquals(
        "Expected exactly 2 records in Target BigQuery Table", 2, records.getTotalRows());
  }
}
