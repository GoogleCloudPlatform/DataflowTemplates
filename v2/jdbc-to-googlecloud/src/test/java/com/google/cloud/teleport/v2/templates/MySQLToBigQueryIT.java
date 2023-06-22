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

import static com.google.cloud.teleport.it.common.utils.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.MySQLResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link JdbcToBigQuery} Flex template MySQL_to_BigQuery. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(value = JdbcToBigQuery.class, template = "MySQL_to_BigQuery")
@RunWith(JUnit4.class)
public class MySQLToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLToBigQueryIT.class);

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String FULL_NAME = "full_name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String IS_MEMBER = "is_member";
  private static final String ENTRY_ADDED = "entry_added";

  private MySQLResourceManager mySQLResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(mySQLResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testMySqlToBigQueryDedicated() throws IOException {
    // Create MySQL Resource manager
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Arrange
    List<Map<String, Object>> jdbcData =
        getJdbcData(List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED));
    mySQLResourceManager.createTable(testName, schema);
    mySQLResourceManager.write(testName, jdbcData);

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(FULL_NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.FLOAT64),
            Field.of(IS_MEMBER, StandardSQLTypeName.STRING),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(testName, bqSchema);
    String tableSpec = PROJECT + ":" + bigQueryResourceManager.getDatasetId() + "." + testName;

    String jobName = createJobName(testName);
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("connectionURL", mySQLResourceManager.getUri())
            .addParameter("outputTable", tableSpec)
            .addParameter(
                "query",
                "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                    + testName)
            .addParameter("bigQueryLoadingTemporaryDirectory", getGcsBasePath() + "/temp")
            .addParameter("username", mySQLResourceManager.getUsername())
            .addParameter("password", mySQLResourceManager.getPassword())
            .addParameter("useColumnAlias", "true")
            .addParameter("connectionProperties", "characterEncoding=UTF-8")
            .addParameter("disabledAlgorithms", "SSLv3, GCM");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(PipelineLauncher.JobState.ACTIVE_STATES);

    PipelineOperator.Result result = pipelineOperator().waitUntilDoneAndFinish(createConfig(info));

    // Assert
    assertThat(result).isEqualTo(PipelineOperator.Result.LAUNCH_FINISHED);

    jdbcData.forEach(
        row -> {
          row.put("full_name", row.remove("name"));
          row.put("is_member", row.remove("member"));
        });
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(testName))
        .hasRecordsUnorderedCaseInsensitiveColumns(jdbcData);
  }

  /**
   * Helper function for generating data according to the common schema for the IT's in this class.
   *
   * @param columns List of column names.
   * @return A map containing the rows of data to be stored in each JDBC table.
   */
  private List<Map<String, Object>> getJdbcData(List<String> columns) {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(columns.get(0), i);
      values.put(columns.get(1), RandomStringUtils.randomAlphabetic(10));
      values.put(columns.get(2), new Random().nextInt(100));
      values.put(columns.get(3), i % 2 == 0 ? "Y" : "N");
      values.put(columns.get(4), Instant.now().toString());
      data.add(values);
    }

    return data;
  }
}
