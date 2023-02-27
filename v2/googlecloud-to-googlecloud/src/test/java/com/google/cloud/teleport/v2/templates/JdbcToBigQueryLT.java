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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.jdbc.DefaultMSSQLResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager.JDBCSchema;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link JdbcToBigQuery Jdbc To BigQuery} Flex template. */
@TemplateLoadTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryLT extends TemplateLoadTestBase {
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = JdbcToBigQueryLT.class.getSimpleName().toLowerCase();
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Jdbc_to_BigQuery_Flex");
  private static final int NUM_MESSAGES = 35000000;
  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.INT64),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL));
  private static final JDBCSchema JDBC_SCHEMA =
      new JDBCSchema(
          Map.of(
              "eventId", "VARCHAR(100)",
              "eventTimestamp", "DATETIME",
              "ipv4", "VARCHAR(100)",
              "ipv6", "VARCHAR(100)",
              "country", "VARCHAR(100)",
              "username", "VARCHAR(100)",
              "quest", "VARCHAR(100)",
              "score", "INTEGER",
              "completed", "BIT"),
          "eventId");
  private static final String STATEMENT =
      "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,DATEADD(SECOND,?/1000,'1970-1-1'),?,?,?,?,?,?,?)";
  private static final String QUERY = "select * from %s.%s";
  private static final String DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  private static final String INPUT_PCOLLECTION = "";
  private static final String OUTPUT_PCOLLECTION = "";
  private static JDBCResourceManager jdbcResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    jdbcResourceManager = DefaultMSSQLResourceManager.builder(testName).build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(CREDENTIALS)
            .build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(jdbcResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(Function.identity());
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "use_runner_v2"));
  }

  @Test
  public void testBacklog10gbUsingPrime() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(config -> config.addParameter("experiments", "enable_prime"));
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    jdbcResourceManager.createTable(testName, JDBC_SCHEMA);
    TableId table = bigQueryResourceManager.createTable(testName, BQ_SCHEMA);
    // Generate fake data to table
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setSinkType("JDBC")
            .setDriverClassName(DRIVER_CLASS_NAME)
            .setConnectionUrl(jdbcResourceManager.getUri())
            .setStatement(String.format(STATEMENT, testName))
            .setUsername(jdbcResourceManager.getUsername())
            .setPassword(jdbcResourceManager.getPassword())
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    // TODO(pranavbhandari): Change this, don't hardcode
                    .addParameter(
                        "driverJars", "gs://apache-beam-pranavbhandari/mssql-jdbc-12.2.0.jre11.jar")
                    .addParameter("driverClassName", DRIVER_CLASS_NAME)
                    .addParameter("username", jdbcResourceManager.getUsername())
                    .addParameter("password", jdbcResourceManager.getPassword())
                    .addParameter("connectionURL", jdbcResourceManager.getUri())
                    .addParameter(
                        "query",
                        String.format(QUERY, jdbcResourceManager.getDatabaseName(), testName))
                    .addParameter("outputTable", toTableSpec(PROJECT, table))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getTempDirectory()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(40)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                .setMinRows(NUM_MESSAGES)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTempDirectory() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, testName, "temp");
  }
}
