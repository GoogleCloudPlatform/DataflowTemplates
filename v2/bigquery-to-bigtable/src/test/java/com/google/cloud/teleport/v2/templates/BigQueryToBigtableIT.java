/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.it.TestProperties.getProperty;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.BigQueryTestUtils;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToBigtable} (BigQuery_To_Bigtable). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigQueryToBigtable.class)
@RunWith(JUnit4.class)
public class BigQueryToBigtableIT extends TemplateTestBase {
  @Rule public final TestName testName = new TestName();

  private static final String READ_QUERY = "readQuery";
  private static final String READ_ID_COL = "readIdColumn";
  private static final String WRITE_PROJECT_ID = "bigtableWriteProjectId";
  private static final String WRITE_INSTANCE_ID = "bigtableWriteInstanceId";
  private static final String WRITE_TABLE_ID = "bigtableWriteTableId";
  private static final String WRITE_COL_FAMILY = "bigtableWriteColumnFamily";
  private static final String WRITE_APP_PROFILE = "bigtableWriteAppProfile";

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "20", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENTH =
      Integer.max(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  private BigQueryResourceManager bigQueryClient;
  private DefaultBigtableResourceManager bigtableClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
    bigtableClient =
        DefaultBigtableResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDownClass() {
    if (bigtableClient != null) {
      bigtableClient.cleanupAll();
    }
    if (bigQueryClient != null) {
      bigQueryClient.cleanupAll();
    }
  }

  @Test
  public void testBigQueryToBigtable() throws IOException {
    // Arrange
    String tableName = "test_table";
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtils.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    bigQueryClient.createTable(tableName, bigQuerySchema);
    bigQueryClient.write(tableName, bigQueryRows);

    String colFamily = "names";
    bigtableClient.createTable(tableName, ImmutableList.of(colFamily));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter(
                READ_QUERY,
                "SELECT * FROM `"
                    + String.join(".", PROJECT, bigQueryClient.getDatasetId(), tableName)
                    + "`")
            .addParameter(READ_ID_COL, BIGQUERY_ID_COL)
            .addParameter(WRITE_PROJECT_ID, PROJECT)
            .addParameter(WRITE_INSTANCE_ID, bigtableClient.getInstanceId())
            .addParameter(WRITE_APP_PROFILE, "default")
            .addParameter(WRITE_TABLE_ID, tableName)
            .addParameter(WRITE_COL_FAMILY, colFamily);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> rows = bigtableClient.readTable(tableName);
    rows.forEach(
        row -> {
          row.getCells(colFamily)
              .forEach(
                  rowCell -> {
                    RowToInsert bqRow =
                        bigQueryRows.get(Integer.parseInt(row.getKey().toStringUtf8()));
                    String col = rowCell.getQualifier().toStringUtf8();
                    String val = rowCell.getValue().toStringUtf8();

                    assertEquals(rowCell.getFamily(), colFamily);
                    assertEquals(bqRow.getContent().get(col), val);
                  });
        });
  }
}
