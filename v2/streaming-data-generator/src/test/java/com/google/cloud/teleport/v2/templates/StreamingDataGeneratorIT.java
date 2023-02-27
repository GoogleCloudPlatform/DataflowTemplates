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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
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
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SchemaTemplate;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link StreamingDataGenerator}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(StreamingDataGenerator.class)
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataGeneratorIT.class);

  private static final String SCHEMA_FILE = "gameevent.json";
  private static final String LOCAL_SCHEMA_PATH = Resources.getResource(SCHEMA_FILE).getPath();

  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String QPS_KEY = "qps";
  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String SCHEMA_TEMPLATE_KEY = "schemaTemplate";
  private static final String SINK_TYPE_KEY = "sinkType";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String TOPIC_KEY = "topic";
  private static final String OUTPUT_TABLE_SPEC = "outputTableSpec";
  private static final String OUTPUT_DEADLETTER_TABLE = "outputDeadletterTable";

  private static final String DEFAULT_QPS = "15";
  private static final String DEFAULT_WINDOW_DURATION = "60s";
  private static final String HIGH_QPS = "10000";

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private JDBCResourceManager jdbcResourceManager;

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager,
        artifactClient,
        bigQueryResourceManager,
        spannerResourceManager,
        jdbcResourceManager);
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    // Arrange
    artifactClient.uploadArtifact(SCHEMA_FILE, LOCAL_SCHEMA_PATH);
    String name = testName;

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            // TODO(zhoufek): See if it is possible to use the properties interface and generate
            // the map from the set values.
            .addParameter(SCHEMA_LOCATION_KEY, getGcsPath(SCHEMA_FILE))
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(testName))
            .addParameter(NUM_SHARDS_KEY, "1");
    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  List<Artifact> outputFiles =
                      artifactClient.listArtifacts(name, Pattern.compile(".*output-.*"));
                  return !outputFiles.isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToGcsWithSchemaTemplate() throws IOException {
    // Arrange
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(testName))
            .addParameter(NUM_SHARDS_KEY, "1");
    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () ->
                    !artifactClient
                        .listArtifacts(testName, Pattern.compile(".*output-.*"))
                        .isEmpty());

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToPubSub() throws IOException {
    // Set up resource manager
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    TopicName backlogTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(backlogTopic, "output-subscription");
    // Arrange
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.PUBSUB.name())
            .addParameter(TOPIC_KEY, backlogTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> pubsubResourceManager.pull(subscription, 5).getReceivedMessagesCount() > 0);
    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToBigQuery() throws IOException {
    // Set up resource manager
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    // schema should match schema supplied to generate fake records.
    Schema schema =
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
    // Arrange
    TableId table = bigQueryResourceManager.createTable(testName, schema);
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, String.valueOf(SchemaTemplate.GAME_EVENT))
            .addParameter(QPS_KEY, HIGH_QPS)
            .addParameter(SINK_TYPE_KEY, "BIGQUERY")
            .addParameter(OUTPUT_TABLE_SPEC, toTableSpec(table));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, table).setMinRows(1).build());
    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToBigQueryWithErrors() throws IOException {
    // Set up resource manager
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    // removes fields intentionally to reproduce DLQ errors
    Schema schema =
        Schema.of(
            Field.of("eventId", StandardSQLTypeName.STRING),
            Field.of("eventTimestamp", StandardSQLTypeName.INT64),
            Field.of("ipv4", StandardSQLTypeName.STRING),
            Field.of("ipv6", StandardSQLTypeName.STRING),
            Field.of("country", StandardSQLTypeName.STRING),
            Field.of("username", StandardSQLTypeName.STRING));
    // Arrange
    TableId table = bigQueryResourceManager.createTable(testName, schema);
    TableId dlq = TableId.of(table.getDataset(), table.getTable() + "_dlq");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, String.valueOf(SchemaTemplate.GAME_EVENT))
            .addParameter(QPS_KEY, HIGH_QPS)
            .addParameter(SINK_TYPE_KEY, "BIGQUERY")
            .addParameter(OUTPUT_TABLE_SPEC, toTableSpec(table))
            .addParameter(OUTPUT_DEADLETTER_TABLE, toTableSpec(dlq));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, dlq).setMinRows(1).build());

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToSpanner() throws IOException {
    // Arrange
    spannerResourceManager =
        DefaultSpannerResourceManager.builder(testName, PROJECT, REGION).build();
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  eventId STRING(1024) NOT NULL,\n"
                + "  eventTimestamp INT64,\n"
                + "  ipv4 STRING(1024),\n"
                + "  ipv6 STRING(1024),\n"
                + "  country STRING(1024),\n"
                + "  username STRING(1024),\n"
                + "  quest STRING(1024),\n"
                + "  score INT64,\n"
                + "  completed BOOL,\n"
                + ") PRIMARY KEY(eventId)",
            testName);
    ImmutableList<String> columnNames =
        ImmutableList.of(
            "eventId",
            "eventTimestamp",
            "ipv4",
            "ipv6",
            "country",
            "username",
            "quest",
            "score",
            "completed");
    spannerResourceManager.createTable(createTableStatement);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.SPANNER.name())
            .addParameter("projectId", PROJECT)
            .addParameter("spannerInstanceName", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseName", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTableName", testName);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> !spannerResourceManager.readTableRecords(testName, columnNames).isEmpty());

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToJdbc() throws IOException {
    jdbcResourceManager = DefaultMSSQLResourceManager.builder(testName).build();
    JDBCSchema jdbcSchema =
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
    jdbcResourceManager.createTable(testName, jdbcSchema);
    String statement =
        String.format(
            "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,DATEADD(SECOND,?/1000,'1970-1-1'),?,?,?,?,?,?,?)",
            testName);
    String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.JDBC.name())
            .addParameter("driverClassName", driverClassName)
            .addParameter("connectionUrl", jdbcResourceManager.getUri())
            .addParameter("statement", statement)
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info), () -> !jdbcResourceManager.readTable(testName).isEmpty());

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
