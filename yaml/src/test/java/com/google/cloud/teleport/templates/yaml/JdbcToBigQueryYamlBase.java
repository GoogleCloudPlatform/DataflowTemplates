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
package com.google.cloud.teleport.templates.yaml;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MSSQLResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;

public class JdbcToBigQueryYamlBase extends YAMLTemplateTestBase {

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String FULL_NAME = "full_name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String IS_MEMBER = "is_member";
  private static final String ENTRY_ADDED = "entry_added";

  protected MySQLResourceManager mySQLResourceManager;
  protected PostgresResourceManager postgresResourceManager;
  protected OracleResourceManager oracleResourceManager;
  protected MSSQLResourceManager sqlServerResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        sqlServerResourceManager,
        postgresResourceManager,
        oracleResourceManager,
        bigQueryResourceManager);
  }

  protected void simpleJdbcToBigQueryTest(
      JDBCResourceManager.JDBCSchema schema,
      JDBCResourceManager jdbcResourceManager,
      boolean useColumnAlias,
      Map<String, String> paramsAdder)
      throws IOException {

    // Arrange
    List<Map<String, Object>> jdbcData =
        getJdbcData(List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED));
    jdbcResourceManager.createTable(testName, schema);
    jdbcResourceManager.write(testName, jdbcData);

    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.NUMERIC),
            Field.of(useColumnAlias ? FULL_NAME : NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.NUMERIC),
            Field.of(useColumnAlias ? IS_MEMBER : MEMBER, StandardSQLTypeName.STRING),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    Map<String, String> parameters = new HashMap<>(paramsAdder);
    parameters.put("connectionURL", "\"" + jdbcResourceManager.getUri() + "\"");
    parameters.put("username", "\"" + jdbcResourceManager.getUsername() + "\"");
    parameters.put("password", "\"" + jdbcResourceManager.getPassword() + "\"");
    parameters.put("connectionProperties", "\"characterEncoding=UTF-8\"");
    parameters.put("fetchSize", "30000");
    parameters.put("outputTable", "\"" + toTableSpecLegacy(table) + "\"");
    parameters.put("disabledAlgorithms", "\"SSLv3, GCM\"");

    // Act
    PipelineLauncher.LaunchInfo info = launchYamlTemplate(parameters);
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
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(testName))
        .hasRecordsUnorderedCaseInsensitiveColumns(jdbcData);
  }

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

  protected String getQueryString() {
    return "\"SELECT ROW_ID, NAME AS FULL_NAME, AGE, MEMBER AS IS_MEMBER, ENTRY_ADDED FROM "
        + testName
        + "\"";
  }

  protected JDBCResourceManager.JDBCSchema getMySqlSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");

    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  protected JDBCResourceManager.JDBCSchema getPostgresSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");

    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  protected JDBCResourceManager.JDBCSchema getOracleSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");

    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  protected JDBCResourceManager.JDBCSchema getSqlServerSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");

    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }
}
