/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.jdbc.DefaultPostgresResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager.JDBCSchema;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link JdbcToBigQuery} Flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryIT extends TemplateTestBase {

  private static final String QUERY = "select * from %s";
  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static JDBCResourceManager jdbcResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    jdbcResourceManager =
        DefaultPostgresResourceManager.builder(testName)
            // Have to manually set username and password since sometimes, the password characters
            // are not supported by the driver
            .setUsername("test")
            .setPassword("Password")
            .setHost(HOST_IP)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(jdbcResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testJdbcToBigQuery() throws IOException {
    JDBCSchema jdbcSchema =
        new JDBCSchema(
            Map.of(
                "id", "INTEGER",
                "name", "VARCHAR(100)"),
            "id");
    jdbcResourceManager.createTable(testName, jdbcSchema);
    jdbcResourceManager.write(
        testName,
        List.of(Map.of("id", 1, "name", "Jake Peralta"), Map.of("id", 2, "name", "Amy Santiago")));
    Schema bqSchema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("name", StandardSQLTypeName.STRING));
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            // TODO(pranavbhandari): Change this, find a way to not hardcode
            .addParameter("driverJars", "gs://cloud-teleport-testing-it/postgresql-42.2.27.jar")
            .addParameter("driverClassName", DRIVER_CLASS_NAME)
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword())
            .addParameter("connectionURL", jdbcResourceManager.getUri())
            .addParameter("query", String.format(QUERY, testName))
            .addParameter("outputTable", toTableSpecLegacy(table))
            .addParameter("bigQueryLoadingTemporaryDirectory", getGcsPath(testName));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable())).isAtLeast(2);
  }
}
