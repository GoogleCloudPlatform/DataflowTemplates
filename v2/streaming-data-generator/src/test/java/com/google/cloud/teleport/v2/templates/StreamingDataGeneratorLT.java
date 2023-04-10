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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.gcp.spanner.SpannerResourceManager;
import com.google.cloud.teleport.it.jdbc.DefaultPostgresResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager.JDBCSchema;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SchemaTemplate;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link StreamingDataGenerator Streaming Data generator} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(StreamingDataGenerator.class)
@RunWith(JUnit4.class)
public class StreamingDataGeneratorLT extends TemplateLoadTestBase {
  private static final String TEST_ROOT_DIR = StreamingDataGeneratorLT.class.getSimpleName();
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Streaming_Data_Generator");
  private static final String FAKE_DATA_PCOLLECTION = "Generate Fake Messages.out0";
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final String NUM_MESSAGES = "35000000";
  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient gcsClient;
  private static BigQueryResourceManager bigQueryResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static JDBCResourceManager jdbcResourceManager;

  @After
  public void cleanup() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager,
        gcsClient,
        bigQueryResourceManager,
        spannerResourceManager,
        jdbcResourceManager);
  }

  @Test
  public void testGeneratePubsub10gb() throws IOException, ParseException, InterruptedException {
    // Set up resource manager
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, project)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    TopicName backlogTopic = pubsubResourceManager.createTopic("output");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(backlogTopic, "output-subscription");
    // Arrange
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("schemaTemplate", SchemaTemplate.GAME_EVENT.name())
            .addParameter("qps", "1000000")
            .addParameter("messagesLimit", NUM_MESSAGES)
            .addParameter("topic", backlogTopic.toString())
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));
    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(pubsubResourceManager.pull(subscription, 5).getReceivedMessagesCount())
        .isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }

  @Test
  public void testGenerateGcs10gb() throws IOException, ParseException, InterruptedException {
    String artifactBucket = TestProperties.artifactBucket();
    // Set up resource manager
    Storage storageClient = createStorageClient(CREDENTIALS);
    gcsClient = GcsArtifactClient.builder(storageClient, artifactBucket, TEST_ROOT_DIR).build();
    String outputDirectory =
        getFullGcsPath(artifactBucket, TEST_ROOT_DIR, gcsClient.runId(), testName);
    // Arrange
    Pattern expectedPattern = Pattern.compile(".*output-.*");
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("schemaTemplate", SchemaTemplate.GAME_EVENT.name())
            .addParameter("qps", "1000000")
            .addParameter("messagesLimit", NUM_MESSAGES)
            .addParameter("sinkType", "GCS")
            .addParameter("outputDirectory", outputDirectory)
            .addParameter("numShards", "50")
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));
    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(gcsClient.listArtifacts(testName, expectedPattern)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }

  @Test
  public void testGenerateBigQuery10gb() throws IOException, ParseException, InterruptedException {
    // Set up resource manager
    String name = testName;
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(name, project).setCredentials(CREDENTIALS).build();
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
    TableId table = bigQueryResourceManager.createTable(name, schema);
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("schemaTemplate", SchemaTemplate.GAME_EVENT.name())
            .addParameter("qps", "1000000")
            .addParameter("messagesLimit", NUM_MESSAGES)
            .addParameter("sinkType", "BIGQUERY")
            .addParameter("outputTableSpec", toTableSpec(project, table))
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));
    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable())).isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }

  @Test
  public void testGenerateSpanner10gb() throws IOException, ParseException, InterruptedException {
    // Set up resource manager
    String name = testName;
    spannerResourceManager = DefaultSpannerResourceManager.builder(name, project, region).build();
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
            name);
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
    // Arrange
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("schemaTemplate", SchemaTemplate.GAME_EVENT.name())
            .addParameter("qps", "1000000")
            .addParameter("messagesLimit", NUM_MESSAGES)
            .addParameter("sinkType", "SPANNER")
            .addParameter("projectId", project)
            .addParameter("spannerInstanceName", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseName", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTableName", name)
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(spannerResourceManager.readTableRecords(name, columnNames)).isNotEmpty();
    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }

  @Test
  public void testGenerateJdbc10gb()
      throws IOException, ParseException, InterruptedException, SQLException {
    jdbcResourceManager = DefaultPostgresResourceManager.builder(testName).setHost(hostIp).build();
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
    // Arrange
    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("schemaTemplate", SchemaTemplate.GAME_EVENT.name())
            .addParameter("qps", "1000000")
            .addParameter("messagesLimit", NUM_MESSAGES)
            .addParameter("sinkType", "JDBC")
            .addParameter("driverClassName", driverClassName)
            .addParameter("connectionUrl", jdbcResourceManager.getUri())
            .addParameter("statement", statement)
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword())
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    ResultSet resultSet =
        jdbcResourceManager.runSQLQuery(String.format("SELECT COUNT(*) FROM %s", testName));
    resultSet.next();
    int totalRows = resultSet.getInt(1);
    assertThat(totalRows).isAtLeast(Integer.valueOf(NUM_MESSAGES));
    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }
}
