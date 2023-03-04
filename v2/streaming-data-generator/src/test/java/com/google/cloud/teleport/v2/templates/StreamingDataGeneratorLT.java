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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SchemaTemplate;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link StreamingDataGenerator Streaming Data generator} template. */
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
  private static ArtifactClient artifactClient;
  private static BigQueryResourceManager bigQueryResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  @After
  public void cleanup() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager, artifactClient, bigQueryResourceManager, spannerResourceManager);
  }

  @Test
  public void testGeneratePubsub10gb() throws IOException, ParseException, InterruptedException {
    // Set up resource manager
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
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
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
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
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, artifactBucket, TEST_ROOT_DIR).build();
    String outputDirectory =
        getFullGcsPath(artifactBucket, TEST_ROOT_DIR, artifactClient.runId(), testName);
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
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));
    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(artifactClient.listArtifacts(testName, expectedPattern)).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }

  @Test
  public void testGenerateBigQuery10gb() throws IOException, ParseException, InterruptedException {
    // Set up resource manager
    String name = testName;
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(name, PROJECT).setCredentials(CREDENTIALS).build();
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
            .addParameter("outputTableSpec", toTableSpec(PROJECT, table))
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
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
    spannerResourceManager = DefaultSpannerResourceManager.builder(name, PROJECT, REGION).build();
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
            .addParameter("projectId", PROJECT)
            .addParameter("spannerInstanceName", spannerResourceManager.getInstanceId())
            .addParameter("spannerDatabaseName", spannerResourceManager.getDatabaseId())
            .addParameter("spannerTableName", name)
            .addParameter("numWorkers", "50")
            .addParameter("maxNumWorkers", "100")
            .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(30)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(spannerResourceManager.readTableRecords(name, columnNames)).isNotEmpty();
    // export results
    exportMetricsToBigQuery(info, getMetrics(info, FAKE_DATA_PCOLLECTION));
  }
}
