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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowClient;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

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

  private static final String BIGQUERY_ID_COL = "test_id";

  private static BigQueryResourceManager bigQueryClient;
  private static DefaultBigtableResourceManager bigtableClient;

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
    String jobName = createJobName(testName.getMethodName());
    String tableName = "test_table";

    Tuple<Schema, List<RowToInsert>> generatedTable = generateBigQueryTable();
    Schema bigQuerySchema = generatedTable.x();
    List<RowToInsert> bigQueryRows = generatedTable.y();
    bigQueryClient.createTable(tableName, bigQuerySchema);
    bigQueryClient.write(tableName, bigQueryRows);

    String colFamily = "names";
    bigtableClient.createTable(tableName, ImmutableList.of(colFamily));

    DataflowClient.LaunchConfig.Builder options =
        DataflowClient.LaunchConfig.builder(jobName, specPath)
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
    DataflowClient.JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(DataflowClient.JobState.ACTIVE_STATES);

    DataflowOperator.Result result =
        new DataflowOperator(getDataflowClient()).waitUntilDone(createConfig(info));

    // Assert
    assertThat(result).isEqualTo(DataflowOperator.Result.JOB_FINISHED);

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

  private static Tuple<Schema, List<RowToInsert>> generateBigQueryTable() {
    // Grab user parameters for table dimensions, or use defaults
    // *note* maxEntryLength cannot exceed 300 characters
    int numRows =
        Integer.parseInt(TestProperties.getProperty("numRows", "20", TestProperties.Type.PROPERTY));
    int numFields =
        Integer.parseInt(
            TestProperties.getProperty("numFields", "100", TestProperties.Type.PROPERTY));
    int maxEntryLength =
        Math.max(
            300,
            Integer.parseInt(
                TestProperties.getProperty("maxEntryLength", "20", TestProperties.Type.PROPERTY)));

    // List to store BigQuery schema fields
    List<Field> bqSchemaFields = new ArrayList<>();

    // Add unique identifier field
    bqSchemaFields.add(Field.of(BIGQUERY_ID_COL, StandardSQLTypeName.INT64));

    // Generate random fields
    for (int i = 1; i < numFields; i++) {
      StringBuilder randomField = new StringBuilder();

      // Field must start with letter
      String prependLetter = RandomStringUtils.randomAlphabetic(1);
      // Field uses unique number at end to keep name unique
      String appendNum = String.valueOf(i);
      // Remaining field name is generated randomly within bounds of maxEntryLength
      String randomString =
          RandomStringUtils.randomAlphanumeric(0, maxEntryLength - appendNum.length());

      randomField.append(prependLetter).append(randomString).append(appendNum);
      bqSchemaFields.add(Field.of(randomField.toString(), StandardSQLTypeName.STRING));
    }
    // Create schema and BigQuery table
    Schema schema = Schema.of(bqSchemaFields);

    // Generate random data
    List<RowToInsert> bigQueryRows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      Map<String, Object> content = new HashMap<>();

      // Iterate unique identifier column
      content.put(BIGQUERY_ID_COL, i);

      // Generate remaining cells in row
      for (int j = 1; j < numFields; j++) {
        content.put(
            bqSchemaFields.get(j).getName(),
            RandomStringUtils.randomAlphanumeric(1, maxEntryLength));
      }
      bigQueryRows.add(RowToInsert.of(content));
    }

    // Return tuple containing the randomly generated schema and table data
    return Tuple.of(schema, bigQueryRows);
  }
}
