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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager.JDBCSchema;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link JdbcToBigQuery Jdbc To BigQuery} Flex template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryLT extends TemplateLoadTestBase {
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = JdbcToBigQueryLT.class.getSimpleName().toLowerCase();
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final long NUM_MESSAGES = 35000000L;
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Jdbc_to_BigQuery_Flex");
  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.STRING),
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
              "eventTimestamp", "TIMESTAMP",
              "ipv4", "VARCHAR(100)",
              "ipv6", "VARCHAR(100)",
              "country", "VARCHAR(100)",
              "username", "VARCHAR(100)",
              "quest", "VARCHAR(100)",
              "score", "INTEGER",
              "completed", "BOOLEAN"),
          "eventId");
  private static final String STATEMENT =
      "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,to_timestamp(?/1000),?,?,?,?,?,?,?)";
  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String INPUT_PCOLLECTION = "Read from JdbcIO/ParDo(DynamicRead).out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to BigQuery/PrepareWrite/ParDo(Anonymous).out0";
  private static JDBCResourceManager jdbcResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    jdbcResourceManager = PostgresResourceManager.builder(testName).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(jdbcResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog(this::enableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingStorageApi()
      throws IOException, ParseException, InterruptedException {
    testBacklog(
        config ->
            config
                .addParameter("useStorageWriteApi", "true")
                .addParameter("experiments", "disable_runner_v2"));
  }

  public void testBacklog(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    jdbcResourceManager.createTable(testName, JDBC_SCHEMA);
    TableId table = bigQueryResourceManager.createTable(testName, BQ_SCHEMA);
    // Generate fake data to table
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setSinkType("JDBC")
            .setQPS("200000")
            .setMessagesLimit(String.valueOf(NUM_MESSAGES))
            .setDriverClassName(DRIVER_CLASS_NAME)
            .setConnectionUrl(jdbcResourceManager.getUri())
            .setStatement(String.format(STATEMENT, testName))
            .setUsername(jdbcResourceManager.getUsername())
            .setPassword(jdbcResourceManager.getPassword())
            .setNumWorkers("10")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter(
                        "driverJars", "gs://apache-beam-pranavbhandari/postgresql-42.2.27.jar")
                    .addParameter("table", testName)
                    .addParameter("partitionColumn", "score")
                    .addParameter("driverClassName", DRIVER_CLASS_NAME)
                    .addParameter("username", jdbcResourceManager.getUsername())
                    .addParameter("password", jdbcResourceManager.getPassword())
                    .addParameter("connectionURL", jdbcResourceManager.getUri())
                    .addParameter("outputTable", toTableSpec(project, table))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getTempDirectory()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable())).isAtLeast(NUM_MESSAGES);

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTempDirectory() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, testName, "temp");
  }
}
