/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataStreamToSpannerMixedIT extends DataStreamToSpannerITBase {

  private String gcsPrefix;
  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;
  private SpannerOracleResourceManager oracleResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
            .build();
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    gcsPrefix =
        getGcsPath(testName + "/cdc/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void mixedMigrationTest() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(
        spannerResourceManager,
        "oracle/OracleDataStreamToSpannerMixedIT/oracle-google_standard_sql-spanner-schema.sql");

    oracleResourceManager = SharedOracleLiveITInstance.getInstance();
    oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();

    executeOracleSqlFileScript(
        oracleResourceManager,
        "oracle/OracleDataStreamToSpannerMixedIT/oracle-schema.sql",
        oracleUser);

    OracleSource jdbcSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleUser,
                SharedOracleLiveITInstance.ORACLE_PASSWORD,
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(oracleUser.toUpperCase(), List.of("Authors", "Books", "Genre")))
            .build();

    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile",
            gcsResourceManager.getBucket(),
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);

    Stream stream =
        datastreamResourceManager.createStream(
            "stream" + RandomStringUtils.randomAlphanumeric(5).toLowerCase(),
            sourceConfig,
            destinationConfig);
    datastreamResourceManager.startStream(stream);

    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    TopicName topic = pubsubResourceManager.createTopic("it-topic");
    subscription = pubsubResourceManager.createSubscription(topic, "it-sub");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefix.substring(1));

    TopicName dlqTopic = pubsubResourceManager.createTopic("it-dlq");
    dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "it-dlq-sub");

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("inputFileFormat", "avro");
    jobParams.put("gcsPubSubSubscription", subscription.toString());
    jobParams.put("dlqGcsPubSubSubscription", dlqSubscription.toString());
    jobParams.put("streamName", stream.getName());
    jobParams.put("instanceId", spannerResourceManager.getInstanceId());
    jobParams.put("databaseId", spannerResourceManager.getDatabaseId());
    jobParams.put("projectId", PROJECT);
    jobParams.put("deadLetterQueueDirectory", getGcsPath(testName, gcsResourceManager) + "/dlq/");
    jobParams.put("workerMachineType", "n1-standard-4");
    jobParams.put("datastreamSourceType", "oracle");

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(PipelineUtils.createJobName(testName), specPath)
            .setParameters(jobParams)
            .addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE");

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            testName,
            "oracle/OracleDataStreamToSpannerMixedIT/oracle-session.json",
            null,
            testName,
            spannerResourceManager,
            pubsubResourceManager,
            jobParams,
            null,
            null,
            gcsResourceManager);

    assertThatPipeline(jobInfo).isRunning();

    // Condition checks
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeInitialData(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Authors")
                        .setMinRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Books")
                        .setMinRows(3)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES + 10)),
                conditionCheck);

    assertThatResult(result).meetsConditions();

    assertAuthorsTableContents();
    assertBooksTableContents();
  }

  private ConditionCheck writeInitialData() {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Write JDBC data";
      }

      @Override
      protected CheckResult check() {
        List<Map<String, Object>> authorRows = new ArrayList<>();
        authorRows.add(Map.of("author_id", 4, "full_name", "Stephen King"));
        authorRows.add(Map.of("author_id", 1, "full_name", "Jane Austen"));
        authorRows.add(Map.of("author_id", 2, "full_name", "Charles Dickens"));
        authorRows.add(Map.of("author_id", 3, "full_name", "Leo Tolstoy"));

        List<Map<String, Object>> bookRows = new ArrayList<>();
        bookRows.add(Map.of("id", 1, "title", "Pride and Prejudice", "author_id", 1));
        bookRows.add(Map.of("id", 2, "title", "Oliver Twist", "author_id", 2));
        bookRows.add(Map.of("id", 3, "title", "War and Peace", "author_id", 3));

        List<Map<String, Object>> genreRows = new ArrayList<>();
        genreRows.add(Map.of("genre_id", 1, "name", "Fiction"));

        for (Map<String, Object> r : authorRows) {
          try {
            executeOracleSql(
                oracleResourceManager,
                String.format(
                    "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (%d, '%s')",
                    r.get("author_id"), r.get("full_name")),
                oracleUser);
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        for (Map<String, Object> r : bookRows) {
          try {
            executeOracleSql(
                oracleResourceManager,
                String.format(
                    "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (%d, '%s', %d)",
                    r.get("id"), r.get("title"), r.get("author_id")),
                oracleUser);
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        for (Map<String, Object> r : genreRows) {
          try {
            executeOracleSql(
                oracleResourceManager,
                String.format(
                    "INSERT INTO \"Genre\"(\"genre_id\",\"name\") VALUES (%d, '%s')",
                    r.get("genre_id"), r.get("name")),
                oracleUser);
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        SharedOracleLiveITInstance.flushRedoLogs();
        return new CheckResult(true, "Sent to Oracle.");
      }
    };
  }

  private void assertAuthorsTableContents() {
    List<Map<String, Object>> authorEvents = new ArrayList<>();
    authorEvents.add(Map.of("author_id", 4, "full_name", "Stephen King"));
    authorEvents.add(Map.of("author_id", 1, "full_name", "Jane Austen"));
    authorEvents.add(Map.of("author_id", 2, "full_name", "Charles Dickens"));
    authorEvents.add(Map.of("author_id", 3, "full_name", "Leo Tolstoy"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(authorEvents);
  }

  private void assertBooksTableContents() {
    List<Map<String, Object>> bookEvents = new ArrayList<>();
    bookEvents.add(Map.of("id", 1, "title", "Pride and Prejudice"));
    bookEvents.add(Map.of("id", 2, "title", "Oliver Twist"));
    bookEvents.add(Map.of("id", 3, "title", "War and Peace"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select id, title from Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(bookEvents);
  }
}
