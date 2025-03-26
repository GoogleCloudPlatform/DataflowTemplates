/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;
import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.bigQueryRowsToRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.clickhouse.templates.BigQueryToClickHouse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.clickhouse.ClickHouseResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.utils.BigQueryTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToClickHouse}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigQueryToClickHouse.class)
@RunWith(JUnit4.class)
public class BigQueryToClickHouseIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryClient;
  private ClickHouseResourceManager clickHouseResourceManager;

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "20", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  @Before
  public void setup() {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    clickHouseResourceManager = ClickHouseResourceManager.builder(testId).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, clickHouseResourceManager);
  }

  @Test
  @TemplateIntegrationTest(value = BigQueryToClickHouse.class, template = "BigQuery_to_ClickHouse")
  public void testBigQueryToClickHouse() throws IOException {
    // Create the target table in ClickHouse

    // Arrange
    Tuple<Schema, List<InsertAllRequest.RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<InsertAllRequest.RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(testName, bigQuerySchema);
    bigQueryClient.write(testName, bigQueryRows);
    String tableName = createJobName(testName).replace("-", "_");

    // Create ClickHouse columns dynamically from BigQuery schema
    Map<String, String> columns = new HashMap<>();
    for (Field field : bigQuerySchema.getFields()) {
      columns.put(field.getName(), "String"); // Assuming all fields as String for ClickHouse
    }

    clickHouseResourceManager.createTable(tableName, columns, null, null);

    // Act
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("inputTableSpec", toTableSpecLegacy(table))
                .addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_dlq")
                .addParameter("jdbcUrl", clickHouseResourceManager.getJdbcConnectionString())
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHouseTable", tableName));
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(clickHouseResourceManager.count(tableName)).isEqualTo(BIGQUERY_NUM_ROWS);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(bigQueryRowsToRecords(bigQueryRows));
  }

  @Test
  @TemplateIntegrationTest(value = BigQueryToClickHouse.class, template = "BigQuery_to_ClickHouse")
  public void testBigQueryToClickHouseWithQuery() throws IOException {
    // Create the target table in ClickHouse

    // Arrange
    Tuple<Schema, List<InsertAllRequest.RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<InsertAllRequest.RowToInsert> bigQueryRows = generatedTable.y();
    TableId table = bigQueryClient.createTable(testName, bigQuerySchema);
    bigQueryClient.write(testName, bigQueryRows);
    String tableName = createJobName(testName).replace("-", "_");

    // Create ClickHouse columns dynamically from BigQuery schema
    Map<String, String> columns = new HashMap<>();
    for (Field field : bigQuerySchema.getFields()) {
      columns.put(field.getName(), "String"); // Assuming all fields as String for ClickHouse
    }

    clickHouseResourceManager.createTable(tableName, columns, null, null);

    // Act
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "query", "SELECT * FROM `" + toTableSpecLegacy(table).replace(':', '.') + "`")
                .addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_dlq")
                .addParameter("jdbcUrl", clickHouseResourceManager.getJdbcConnectionString())
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHouseTable", tableName));
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    assertThat(clickHouseResourceManager.count(tableName)).isEqualTo(BIGQUERY_NUM_ROWS);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(bigQueryRowsToRecords(bigQueryRows));
  }
}
