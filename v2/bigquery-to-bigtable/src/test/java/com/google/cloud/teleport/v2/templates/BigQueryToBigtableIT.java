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
import static com.google.cloud.teleport.it.matchers.RecordsSubject.bigQueryRowsToRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatBigtableRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.common.BigQueryTestUtil;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigQueryToBigtable} (BigQuery_To_Bigtable). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigQueryToBigtable.class)
@RunWith(JUnit4.class)
public class BigQueryToBigtableIT extends TemplateTestBase {

  private static final String READ_QUERY = "query";
  private static final String READ_TABLE_SPEC = "inputTableSpec";
  private static final String USE_LEGACY_SQL = "useLegacySql";
  private static final String READ_ID_COL = "readIdColumn";
  private static final String WRITE_INSTANCE_ID = "bigtableWriteInstanceId";
  private static final String WRITE_TABLE_ID = "bigtableWriteTableId";
  private static final String WRITE_COL_FAMILY = "bigtableWriteColumnFamily";

  // Define a set of parameters used to allow configuration of the test size being run.
  private static final String BIGQUERY_ID_COL = "test_id";
  private static final int BIGQUERY_NUM_ROWS =
      Integer.parseInt(getProperty("numRows", "20", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_NUM_FIELDS =
      Integer.parseInt(getProperty("numFields", "100", TestProperties.Type.PROPERTY));
  private static final int BIGQUERY_MAX_ENTRY_LENGTH =
      Integer.min(
          300, Integer.parseInt(getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

  private BigQueryResourceManager bigQueryClient;
  private DefaultBigtableResourceManager bigtableClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    bigtableClient =
        DefaultBigtableResourceManager.builder(testName, PROJECT)
            .setCredentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableClient, bigQueryClient);
  }

  private void simpleBigQueryToBigtableTest(PipelineLauncher.LaunchConfig.Builder options)
      throws IOException {
    // Arrange
    String tableName = options.getParameter(WRITE_TABLE_ID);
    Tuple<Schema, List<RowToInsert>> generatedTable =
        BigQueryTestUtil.generateBigQueryTable(
            BIGQUERY_ID_COL, BIGQUERY_NUM_ROWS, BIGQUERY_NUM_FIELDS, BIGQUERY_MAX_ENTRY_LENGTH);
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    bigQueryClient.createTable(tableName, bigQuerySchema);
    bigQueryClient.write(tableName, bigQueryRows);

    String colFamily = options.getParameter(WRITE_COL_FAMILY);
    Truth.assertThat(colFamily).isNotNull();
    bigtableClient.createTable(tableName, ImmutableList.of(colFamily));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> rows = bigtableClient.readTable(tableName);
    assertThatBigtableRecords(rows, colFamily)
        .hasRecordsUnordered(
            bigQueryRowsToRecords(bigQueryRows, ImmutableList.of(BIGQUERY_ID_COL)));
  }

  @Test
  public void testBigQueryToBigtableWithQuery() throws IOException {
    String tableName = "test_table";
    String colFamily = "names";

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter(
                READ_QUERY,
                "SELECT * FROM `"
                    + toTableSpecStandard(
                        TableId.of(PROJECT, bigQueryClient.getDatasetId(), tableName))
                    + "`")
            .addParameter(READ_ID_COL, BIGQUERY_ID_COL)
            .addParameter(WRITE_INSTANCE_ID, bigtableClient.getInstanceId())
            .addParameter(WRITE_TABLE_ID, tableName)
            .addParameter(WRITE_COL_FAMILY, colFamily);

    simpleBigQueryToBigtableTest(options);
  }

  @Test
  public void testBigQueryToBigtableWithLegacyQuery() throws IOException {
    String tableName = "test_table";
    String colFamily = "names";

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter(
                READ_QUERY,
                "SELECT * FROM ["
                    + toTableSpecLegacy(
                        TableId.of(PROJECT, bigQueryClient.getDatasetId(), tableName))
                    + "]")
            .addParameter(USE_LEGACY_SQL, "true")
            .addParameter(READ_ID_COL, BIGQUERY_ID_COL)
            .addParameter(WRITE_INSTANCE_ID, bigtableClient.getInstanceId())
            .addParameter(WRITE_TABLE_ID, tableName)
            .addParameter(WRITE_COL_FAMILY, colFamily);
    toTableSpecLegacy(TableId.of(PROJECT, bigQueryClient.getDatasetId(), tableName));

    simpleBigQueryToBigtableTest(options);
  }

  @Test
  public void testBigQueryToBigtableWithTableSpec() throws IOException {
    String tableName = "test_table";
    String colFamily = "names";

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter(
                READ_TABLE_SPEC,
                toTableSpecLegacy(TableId.of(PROJECT, bigQueryClient.getDatasetId(), tableName)))
            .addParameter(READ_ID_COL, BIGQUERY_ID_COL)
            .addParameter(WRITE_INSTANCE_ID, bigtableClient.getInstanceId())
            .addParameter(WRITE_TABLE_ID, tableName)
            .addParameter(WRITE_COL_FAMILY, colFamily);

    simpleBigQueryToBigtableTest(options);
  }
}
