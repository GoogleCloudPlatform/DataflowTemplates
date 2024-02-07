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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SchemaTemplate;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager.JDBCSchema;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link StreamingDataGenerator}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(StreamingDataGenerator.class)
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT extends TemplateTestBase {

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
  private KafkaResourceManager kafkaResourceManager;

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager,
        gcsClient,
        bigQueryResourceManager,
        spannerResourceManager,
        jdbcResourceManager,
        kafkaResourceManager);
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    // Arrange
    gcsClient.uploadArtifact(SCHEMA_FILE, LOCAL_SCHEMA_PATH);
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
                      gcsClient.listArtifacts(name, Pattern.compile(".*output-.*"));
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
                () -> !gcsClient.listArtifacts(testName, Pattern.compile(".*output-.*")).isEmpty());

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToPubSub() throws IOException {
    // Set up resource manager
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
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

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, subscription).setMinMessages(1).build();

    Result result = pipelineOperator().waitForConditionAndFinish(createConfig(info), pubsubCheck);

    // Assert
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testFakeMessagesToBigQuery() throws IOException {
    // Set up resource manager
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
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
            .addParameter(OUTPUT_TABLE_SPEC, toTableSpecLegacy(table));

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
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
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
            .addParameter(OUTPUT_TABLE_SPEC, toTableSpecLegacy(table))
            .addParameter(OUTPUT_DEADLETTER_TABLE, toTableSpecLegacy(dlq));

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
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
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
    spannerResourceManager.executeDdlStatement(createTableStatement);

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
    jdbcResourceManager = PostgresResourceManager.builder(testName).build();
    JDBCSchema jdbcSchema =
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
    jdbcResourceManager.createTable(testName, jdbcSchema);
    String statement =
        String.format(
            "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,to_timestamp(?/1000),?,?,?,?,?,?,?)",
            testName);
    String driverClassName = "org.postgresql.Driver";

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

  @Test
  public void testFakeMessagesToKafka() throws IOException {
    // Set up resource manager
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    String outTopicName = kafkaResourceManager.createTopic(testName, 5);

    KafkaConsumer<String, String> consumer =
        kafkaResourceManager.buildConsumer(new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(Collections.singletonList(outTopicName));

    // Arrange
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter(SCHEMA_TEMPLATE_KEY, SchemaTemplate.GAME_EVENT.name())
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.KAFKA.name())
            .addParameter(
                "bootstrapServer",
                kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", ""))
            .addParameter("kafkaTopic", outTopicName);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Set<String> outMessages = new HashSet<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  ConsumerRecords<String, String> outMessage =
                      consumer.poll(Duration.ofMillis(100));
                  for (ConsumerRecord<String, String> message : outMessage) {
                    outMessages.add(message.value());
                  }
                  return outMessages.size() >= 1;
                });

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
