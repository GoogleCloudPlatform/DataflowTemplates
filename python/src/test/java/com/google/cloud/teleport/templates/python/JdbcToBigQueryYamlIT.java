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
package com.google.cloud.teleport.templates.python;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link YAMLTemplate} using a JDBC to BigQuery pipeline. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(YAMLTemplate.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryYamlIT extends JDBCBaseIT {

  private PostgresResourceManager postgresResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final String YAML_PIPELINE = "JdbcToBigQueryYamlIT.yaml";
  private static final String YAML_PIPELINE_GCS_PATH = "input/" + YAML_PIPELINE;
  private static final String JDBC_TABLE_NAME = "source_table";
  private static final int ROW_COUNT = 10;

  private static final String ROW_ID = "id";
  private static final String NAME = "name";
  private static final String AGE = "age";

  @Before
  public void setUp() throws IOException {
    // We need a real JDBC URL for the test, so we use the one from JDBCBaseIT constants
    // For a real test against a specific DB, ensure its Docker container is running or accessible.
    // For this example, we'll use Postgres.
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();

    gcsClient.createArtifact(YAML_PIPELINE_GCS_PATH, readYamlPipelineFile());
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(postgresResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testJdbcToBigQuery() throws IOException {
    // Arrange JDBC source
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR(100)");
    columns.put(AGE, "INTEGER");
    JDBCResourceManager.JDBCSchema jdbcSchema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
    postgresResourceManager.createTable(JDBC_TABLE_NAME, jdbcSchema);

    // Arrange BigQuery destination
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.INT64));
    Schema bqSchema = Schema.of(bqSchemaFields);
    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    // Prepare Jinja variables
    String jdbcUrl = postgresResourceManager.getUri();
    String username = postgresResourceManager.getUsername();
    String password = postgresResourceManager.getPassword();
    String query = String.format("SELECT %s, %s, %s FROM %s", ROW_ID, NAME, AGE, JDBC_TABLE_NAME);

    // TODO: when Beam 2.66.0 is released, JDBC_DRIVER_JARS and JDBC_DRIVER_CLASS_NAME can be
    // removed
    String jinjaVars =
        String.format(
            "{"
                + "\"JDBC_URL\": \"%s\", "
                + "\"JDBC_USERNAME\": \"%s\", "
                + "\"JDBC_PASSWORD\": \"%s\", "
                + "\"JDBC_QUERY\": \"%s\", "
                + "\"BQ_TABLE_SPEC\": \"%s\", "
                + "\"JDBC_DRIVER_JARS\": \"%s\", "
                + "\"JDBC_DRIVER_CLASS_NAME\": \"%s\""
                + "}",
            jdbcUrl,
            username,
            password,
            query,
            toTableSpecStandard(table),
            postgresDriverGCSPath(),
            POSTGRES_DRIVER);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("yaml_pipeline_file", getGcsPath(YAML_PIPELINE_GCS_PATH))
            .addParameter("jinja_variables", jinjaVars);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Write data to JDBC after pipeline starts (for batch)
    List<Map<String, Object>> expectedData = new ArrayList<>();
    Random random = new Random();
    for (int i = 1; i <= ROW_COUNT; i++) {
      Map<String, Object> row =
          ImmutableMap.of(
              ROW_ID, i, NAME, RandomStringUtils.randomAlphabetic(10), AGE, random.nextInt(100));
      expectedData.add(row);
    }
    postgresResourceManager.write(JDBC_TABLE_NAME, expectedData);

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();
    TableResult actualRecords = bigQueryResourceManager.readTable(table);
    assertThatBigQueryRecords(actualRecords).hasRecordsUnordered(expectedData);
  }

  private String readYamlPipelineFile() throws IOException {
    return Files.readString(Paths.get(Resources.getResource(YAML_PIPELINE).getPath()));
  }
}
