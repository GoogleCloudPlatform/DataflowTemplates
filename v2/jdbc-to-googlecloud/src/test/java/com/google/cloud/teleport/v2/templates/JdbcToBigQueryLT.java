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

import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager.JDBCSchema;
import com.google.cloud.teleport.it.jdbc.PostgresResourceManager;
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
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Jdbc_to_BigQuery_Flex");
  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.of("eventid", StandardSQLTypeName.STRING),
          Field.of("eventtimestamp", StandardSQLTypeName.INT64),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL),
          // add a insert timestamp column to query latency values
          Field.newBuilder("_metadata_insert_timestamp", StandardSQLTypeName.TIMESTAMP)
              .setDefaultValueExpression("CURRENT_TIMESTAMP()")
              .build());

  // need to provide json schema to BigQueryIO without `_metadata_insert_timestamp` so it populates
  // the default value.
  private static final String bqJsonSchema =
      "{\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"eventid\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"eventtimestamp\",\n"
          + "      \"type\": \"INTEGER\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"ipv4\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"ipv6\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"country\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"username\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"quest\",\n"
          + "      \"type\": \"STRING\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"score\",\n"
          + "      \"type\": \"INTEGER\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"completed\",\n"
          + "      \"type\": \"BOOL\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  private static final JDBCSchema JDBC_SCHEMA =
      new JDBCSchema(
          Map.of(
              "eventid", "VARCHAR(100)",
              "eventtimestamp", "BIGINT",
              "ipv4", "VARCHAR(100)",
              "ipv6", "VARCHAR(100)",
              "country", "VARCHAR(100)",
              "username", "VARCHAR(100)",
              "quest", "VARCHAR(100)",
              "score", "INTEGER",
              "completed", "BOOLEAN"),
          "eventid");
  private static final String STATEMENT =
      "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,?,?,?,?,?,?,?,?)";
  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String INPUT_PCOLLECTION = "Read from JdbcIO/ParDo(DynamicRead).out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to BigQuery/PrepareWrite/ParDo(Anonymous).out0";
  private static JDBCResourceManager jdbcResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;
  private final TestConfiguration config10gb = new TestConfiguration(35_000_000L, 30, 30);
  private final TestConfiguration config100gb = new TestConfiguration(350_000_000L, 300, 300);
  private final TestConfiguration config1tb = new TestConfiguration(3_500_000_000L, 3000, 3000);

  @Before
  public void setup() {
    jdbcResourceManager = PostgresResourceManager.builder(testName).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project).setCredentials(CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(jdbcResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(config10gb, this::disableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog(config10gb, this::enableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingStorageApi()
      throws IOException, ParseException, InterruptedException {
    testBacklog(
        config10gb,
        config ->
            config
                .addParameter("useStorageWriteApi", "true")
                .addParameter("experiments", "disable_runner_v2"));
  }

  @Test
  public void testBacklog100gb() throws IOException, ParseException, InterruptedException {
    testBacklog(config100gb, this::disableRunnerV2);
  }

  @Test
  public void testBacklog1tb() throws IOException, ParseException, InterruptedException {
    testBacklog(config1tb, this::disableRunnerV2);
  }

  public void testBacklog(
      TestConfiguration config, Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    jdbcResourceManager.createTable(testName, JDBC_SCHEMA);
    // Set BigQuery table expiration to pipelineTimeout + dataGeneratorTimeout + 1 hr (buffer)
    long expirationTimeMillis =
        60000L * (config.pipelineTimeout + config.dataGeneratorTimeout + 60);
    TableId table =
        bigQueryResourceManager.createTable(
            testName, BQ_SCHEMA, System.currentTimeMillis() + expirationTimeMillis);
    // Generate fake data to table
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setSinkType("JDBC")
            .setQPS("200000")
            .setMessagesLimit(String.valueOf(config.numMessages))
            .setDriverClassName(DRIVER_CLASS_NAME)
            .setConnectionUrl(jdbcResourceManager.getUri())
            .setStatement(String.format(STATEMENT, testName))
            .setUsername(jdbcResourceManager.getUsername())
            .setPassword(jdbcResourceManager.getPassword())
            .setNumWorkers("10")
            .build();
    dataGenerator.execute(Duration.ofMinutes(config.dataGeneratorTimeout));
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
                    .addParameter("bigQueryLoadingTemporaryDirectory", getTempDirectory())
                    .addParameter("jsonSchema", bqJsonSchema))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitUntilDone(
            createConfig(info, Duration.ofMinutes(config.pipelineTimeout)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable())).isAtLeast(config.numMessages);

    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION);

    // Storage Write API does not populate default values on BigQuery, skip latency calculations
    if (!info.parameters().containsKey("useStorageWriteApi")) {
      // Query end to end latency metrics from BigQuery
      TableResult latencyResult =
          bigQueryResourceManager.runQuery(
              String.format(
                  "WITH difference AS (SELECT\n"
                      + "    TIMESTAMP_DIFF(_metadata_insert_timestamp,\n"
                      + "    TIMESTAMP_MILLIS(eventTimestamp), SECOND) AS latency,\n"
                      + "    FROM %s.%s)\n"
                      + "    SELECT\n"
                      + "      PERCENTILE_CONT(difference.latency, 0.5) OVER () as median,\n"
                      + "      PERCENTILE_CONT(difference.latency, 0.9) OVER () as percentile_90,\n"
                      + "      PERCENTILE_CONT(difference.latency, 0.95) OVER () as percentile_95,\n"
                      + "      PERCENTILE_CONT(difference.latency, 0.99) OVER () as percentile_99\n"
                      + "    FROM difference LIMIT 1",
                  table.getDataset(), table.getTable()));

      FieldValueList latencyValues = latencyResult.getValues().iterator().next();
      metrics.put("median_latency", latencyValues.get(0).getDoubleValue());
      metrics.put("percentile_90_latency", latencyValues.get(1).getDoubleValue());
      metrics.put("percentile_95_latency", latencyValues.get(2).getDoubleValue());
      metrics.put("percentile_99_latency", latencyValues.get(3).getDoubleValue());
    }

    // export results
    exportMetricsToBigQuery(info, metrics);
  }

  private String getTempDirectory() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, testName, "temp");
  }
}
