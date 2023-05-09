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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.common.utils.PipelineUtils.createJobName;
import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.JDBCBaseIT;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.jdbc.MSSQLResourceManager;
import com.google.cloud.teleport.it.jdbc.MySQLResourceManager;
import com.google.cloud.teleport.it.jdbc.OracleResourceManager;
import com.google.cloud.teleport.it.jdbc.PostgresResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
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

/** Integration test for {@link JdbcToBigQuery} (JdbcToBigQuery). */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryIT extends JDBCBaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryIT.class);

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private MySQLResourceManager mySQLResourceManager;
  private PostgresResourceManager postgresResourceManager;
  private OracleResourceManager oracleResourceManager;
  private MSSQLResourceManager msSQLResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        msSQLResourceManager,
        postgresResourceManager,
        oracleResourceManager,
        bigQueryResourceManager);
  }

  @Test
  public void testMySqlToBigQueryClassic() throws IOException {
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

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        MYSQL_DRIVER,
        mySqlDriverGCSPath(),
        mySQLResourceManager,
        "SELECT * FROM " + testName);
  }

  @Test
  public void testPostgresToBigQueryClassic() throws IOException {
    // Create postgres Resource manager
    postgresResourceManager = PostgresResourceManager.builder(testName).build();

    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "INTEGER");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        POSTGRES_DRIVER,
        postgresDriverGCSPath(),
        postgresResourceManager,
        "SELECT * FROM " + testName);
  }

  @Test
  public void testOracleToBigQueryClassic() throws IOException {
    // Oracle image does not work on M1
    if (System.getProperty("testOnM1") != null) {
      LOG.info("M1 is being used, Oracle tests are not being executed.");
      return;
    }

    // Create oracle Resource manager
    oracleResourceManager = OracleResourceManager.builder(testName).build();

    // Arrange oracle-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        ORACLE_DRIVER,
        oracleDriverGCSPath(),
        oracleResourceManager,
        "SELECT * FROM " + testName);
  }

  @Test
  public void testMsSqlToBigQueryClassic() throws IOException {
    // Create msSql Resource manager
    msSQLResourceManager = MSSQLResourceManager.builder(testName).build();

    // Arrange msSql-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        MSSQL_DRIVER,
        msSqlDriverGCSPath(),
        msSQLResourceManager,
        "SELECT * FROM " + testName);
  }

  @Test
  public void testPostgresToBigQueryGcsQuery() throws IOException {
    // Create postgres Resource manager
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    gcsClient.createArtifact("input/query.sql", "SELECT * FROM " + testName);

    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "INTEGER");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        POSTGRES_DRIVER,
        postgresDriverGCSPath(),
        postgresResourceManager,
        getGcsPath("input/query.sql"));
  }

  private void simpleJdbcToBigQueryTest(
      String testName,
      JDBCResourceManager.JDBCSchema schema,
      String driverClassName,
      String driverJars,
      JDBCResourceManager jdbcResourceManager,
      String query)
      throws IOException {

    // Arrange
    jdbcResourceManager.createTable(testName, schema);
    jdbcResourceManager.write(
        testName, getJdbcData(List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED)));

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.FLOAT64),
            Field.of(MEMBER, StandardSQLTypeName.STRING),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(testName, bqSchema);
    String tableSpec = PROJECT + ":" + bigQueryResourceManager.getDatasetId() + "." + testName;

    String jobName = createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("connectionURL", jdbcResourceManager.getUri())
            .addParameter("driverClassName", driverClassName)
            .addParameter("query", query)
            .addParameter("outputTable", tableSpec)
            .addParameter("driverJars", driverJars)
            .addParameter("bigQueryLoadingTemporaryDirectory", getGcsBasePath() + "/temp")
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword());

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(PipelineLauncher.JobState.ACTIVE_STATES);

    PipelineOperator.Result result =
        new PipelineOperator(launcher()).waitUntilDoneAndFinish(createConfig(info));

    // Assert
    assertThat(result).isEqualTo(PipelineOperator.Result.LAUNCH_FINISHED);

    assertThatBigQueryRecords(bigQueryResourceManager.readTable(testName))
        .hasRecordsUnorderedCaseInsensitiveColumns(jdbcResourceManager.readTable(testName));
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
