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

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.kms.KMSResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link JdbcToBigQuery} Flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryIT extends JDBCBaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryIT.class);

  private static final Integer NUM_ROWS = 100;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String FULL_NAME = "full_name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String IS_MEMBER = "is_member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final String FAKE = "FAKE";

  private static final String KMS_REGION = "global";
  private static final String KEYRING_ID = "JDBCToBigQuery";
  private static final String CRYPTO_KEY_NAME = "key1";

  private MySQLResourceManager mySQLResourceManager;
  private PostgresResourceManager postgresResourceManager;
  private OracleResourceManager oracleResourceManager;
  private MSSQLResourceManager msSQLResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;
  private KMSResourceManager kmsResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    kmsResourceManager =
        KMSResourceManager.builder(PROJECT, credentialsProvider).setRegion(KMS_REGION).build();
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
  public void testMySqlToBigQueryFlex() throws IOException {
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
        true,
        false,
        config ->
            config.addParameter(
                "query",
                "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                    + testName));
  }

  @Test
  public void testMySqlToBigQueryFlexWithDlq() throws IOException {
    // Create MySQL Resource manager
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    columns.put(FAKE, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        MYSQL_DRIVER,
        mySqlDriverGCSPath(),
        mySQLResourceManager,
        true,
        true,
        config ->
            config
                .addParameter("query", "select * from " + testName)
                .addParameter("useStorageWriteApi", "true"));
  }

  @Test
  public void testMySqlToBigQueryWithStorageWriteApi() throws IOException {
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

    String tableName = "mySqlToBigQueryStorageWrite";

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        tableName,
        schema,
        MYSQL_DRIVER,
        mySqlDriverGCSPath(),
        mySQLResourceManager,
        true,
        false,
        config ->
            config
                .addParameter(
                    "query",
                    "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                        + tableName)
                .addParameter("useStorageWriteApi", "true"));
  }

  @Test
  public void testPostgresToBigQueryFlex() throws IOException {
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
        true,
        false,
        config ->
            config.addParameter(
                "query",
                "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                    + testName));
  }

  @Test
  public void testPostgresWithUnicodeCharactersInQuery() throws IOException {
    String tableName = "unicóde_table";

    postgresResourceManager = PostgresResourceManager.builder(testName).build();
    gcsClient.createArtifact(
        "input/query.sql",
        "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
            + tableName);

    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "INTEGER NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "INTEGER");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    simpleJdbcToBigQueryTest(
        testName,
        tableName,
        testName,
        schema,
        POSTGRES_DRIVER,
        postgresDriverGCSPath(),
        postgresResourceManager,
        true,
        false,
        config -> config.addParameter("query", getGcsPath("input/query.sql")));
  }

  @Test
  public void testOracleToBigQueryFlex() throws IOException {
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
        true,
        false,
        config ->
            config.addParameter(
                "query",
                "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                    + testName));
  }

  @Test
  public void testMsSqlToBigQueryFlex() throws IOException {
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
        true,
        false,
        config ->
            config.addParameter(
                "query",
                "SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
                    + testName));
  }

  @Test
  public void testReadWithPartitions() throws IOException {
    postgresResourceManager = PostgresResourceManager.builder(testId).build();

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
        false,
        false,
        config -> config.addParameter("table", testName).addParameter("partitionColumn", ROW_ID));
  }

  private void simpleJdbcToBigQueryTest(
      String testName,
      JDBCResourceManager.JDBCSchema schema,
      String driverClassName,
      String driverJars,
      JDBCResourceManager jdbcResourceManager,
      boolean useColumnAlias,
      boolean useDlq,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    simpleJdbcToBigQueryTest(
        testName,
        testName,
        testName,
        schema,
        driverClassName,
        driverJars,
        jdbcResourceManager,
        useColumnAlias,
        useDlq,
        paramsAdder);
  }

  private void simpleJdbcToBigQueryTest(
      String testName,
      String sourceTableName,
      String targetTableName,
      JDBCResourceManager.JDBCSchema schema,
      String driverClassName,
      String driverJars,
      JDBCResourceManager jdbcResourceManager,
      boolean useColumnAlias,
      boolean useDlq,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Arrange
    List<String> columns = new ArrayList<>(List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED));
    if (useDlq) {
      columns.add(FAKE);
    }
    List<Map<String, Object>> jdbcData = getJdbcData(columns, useDlq);
    jdbcResourceManager.createTable(sourceTableName, schema);
    jdbcResourceManager.write(sourceTableName, jdbcData);

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(useColumnAlias ? FULL_NAME : NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.FLOAT64),
            Field.of(useColumnAlias ? IS_MEMBER : MEMBER, StandardSQLTypeName.STRING),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(targetTableName, bqSchema);

    Function<String, String> encrypt =
        message -> kmsResourceManager.encrypt(KEYRING_ID, CRYPTO_KEY_NAME, message);
    CryptoKey cryptoKey = kmsResourceManager.getOrCreateCryptoKey(KEYRING_ID, CRYPTO_KEY_NAME);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("connectionURL", encrypt.apply(jdbcResourceManager.getUri()))
                .addParameter("driverClassName", driverClassName)
                .addParameter("outputTable", toTableSpecLegacy(table))
                .addParameter("driverJars", driverJars)
                .addParameter("bigQueryLoadingTemporaryDirectory", getGcsBasePath() + "/temp")
                .addParameter("username", encrypt.apply(jdbcResourceManager.getUsername()))
                .addParameter("password", encrypt.apply(jdbcResourceManager.getPassword()))
                .addParameter("KMSEncryptionKey", cryptoKey.getName())
                .addParameter("useColumnAlias", "true")
                .addParameter("fetchSize", "100000")
                .addParameter("connectionProperties", "characterEncoding=UTF-8")
                .addParameter("disabledAlgorithms", "SSLv3, GCM"));
    if (useDlq) {
      options.addParameter("outputDeadletterTable", toTableSpecLegacy(table) + "_error_records");
    }

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    if (useColumnAlias) {
      jdbcData.forEach(
          row -> {
            row.put("full_name", row.remove("name"));
            row.put("is_member", row.remove("member"));
          });
    }
    if (useDlq) {
      assertThatBigQueryRecords(bigQueryResourceManager.readTable(targetTableName)).hasRows(0);
      assertThatBigQueryRecords(
              bigQueryResourceManager.readTable(targetTableName + "_error_records"))
          .hasRows(NUM_ROWS);
    } else {
      assertThatBigQueryRecords(bigQueryResourceManager.readTable(targetTableName))
          .hasRecordsUnorderedCaseInsensitiveColumns(jdbcData);
    }
  }

  /**
   * Helper function for generating data according to the common schema for the IT's in this class.
   *
   * @param columns List of column names.
   * @return A map containing the rows of data to be stored in each JDBC table.
   */
  private List<Map<String, Object>> getJdbcData(List<String> columns, boolean useDlq) {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(columns.get(0), i);
      values.put(columns.get(1), RandomStringUtils.randomAlphabetic(10));
      values.put(columns.get(2), new Random().nextInt(100));
      values.put(columns.get(3), i % 2 == 0 ? "Y" : "N");
      values.put(columns.get(4), Instant.now().toString());
      if (useDlq) {
        values.put(columns.get(5), RandomStringUtils.randomAlphabetic(10));
      }
      data.add(values);
    }

    return data;
  }

  // Since maybeParseSecret is private, we need to use reflection to access it
  private static String invokeMaybeParseSecret(String secret) throws Exception {
    java.lang.reflect.Method method =
        JdbcToBigQuery.class.getDeclaredMethod("maybeParseSecret", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, secret);
  }

  @Test
  public void testMaybeParseSecret_invalidSecretPath_returnsOriginalString() throws Exception {
    String secretPath = "not-a-secret";
    String expectedSecret = "not-a-secret";
    String actualSecret = invokeMaybeParseSecret(secretPath);
    assertEquals(expectedSecret, actualSecret);
  }

  @Test
  public void testMaybeParseSecret_nullInput_returnsNull() throws Exception {
    String secretPath = null;
    String actualSecret = invokeMaybeParseSecret(secretPath);
    assertNull(actualSecret);
  }
}
