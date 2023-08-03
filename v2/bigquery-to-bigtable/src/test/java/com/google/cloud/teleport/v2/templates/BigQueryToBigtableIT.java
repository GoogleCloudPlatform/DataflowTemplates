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

import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.bigQueryRowsToRecords;
import static org.apache.beam.it.gcp.bigtable.matchers.BigtableAsserts.assertThatBigtableRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import java.io.IOException;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.utils.BigQueryTestUtil;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
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
  private BigtableResourceManager bigtableClient;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    bigtableClient =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
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
    String tableName = "test_table_with_query_" + RandomStringUtils.randomAlphanumeric(8);
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
    String tableName = "test_table_legacy_" + RandomStringUtils.randomAlphanumeric(8);
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
    String tableName = "test_table_table_spec_" + RandomStringUtils.randomAlphanumeric(8);
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
