/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider.BOOKS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils.MySQLSrcDataProvider;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.logging.LoggingClient;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataStreamToSpannerMySQLSrcPubsubFT. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerMySQLSrcPubsubFT extends DataStreamToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataStreamToSpannerMySQLSrcPubsubFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";

  private PipelineLauncher.LaunchInfo jobInfo;
  public SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private CloudSqlResourceManager sourceDBResourceManager;
  private JDBCSource sourceConnectionProfile;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    pubsubResourceManager = setUpPubSubResourceManager();
  }

  /**
   * Cleanup all the resources and resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  @Test
  public void pubsubInvalidSubscriptionFITest() throws IOException, InterruptedException {
    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName);

    String subscriptionName = "projects/project/subscriptions/IncorrectFormatPubSubSubscriptionID";
    try {
      jobInfo =
          launchFwdDataflowJob(
              spannerResourceManager,
              "gcsPrefix",
              "testStreamName",
              "dlqGcsPrefix",
              subscriptionName,
              subscriptionName,
              flexTemplateBuilder);
      fail("Expected launch job to fail but it succeeded");
    } catch (RuntimeException e) {
      String jobId = extractJobIdFromError(e.getMessage());
      assertNotNull(jobId);
      LoggingClient loggingClient =
          LoggingClient.builder(credentials).setProjectId(PROJECT).build();
      String filter =
          String.format(" \"NOT_FOUND: Unable to find subscription %s\" ", subscriptionName);
      Instant startTime = Instant.now();
      Instant endTime = startTime.plus(Duration.ofMinutes(15L));
      Boolean errorLogFound = false;
      while (!errorLogFound && Instant.now().isBefore(endTime)) {
        List<Payload> logs = loggingClient.readJobLogs(jobId, filter, Severity.ERROR, 2);
        if (logs.size() > 0) {
          errorLogFound = true;
        }
        Thread.sleep(15 * 1000);
      }
      assertTrue(
          "Could not find Expected dataflow job log: NOT_FOUND pubsub subscription", errorLogFound);
    }
  }

  private String extractJobIdFromError(String message) {
    Pattern pattern =
        Pattern.compile(
            "https://console.cloud.google.com/dataflow/jobs/([^/]+)/([^/?]+)(\\?project=([^&]+))?");
    Matcher matcher = pattern.matcher(message);
    if (matcher.find()) {
      // Group 2 contains the jobId
      return matcher.group(2);
    } else {
      return null;
    }
  }

  @Test
  public void pubsubDuplicateMessageDeliveryFITest() throws IOException, InterruptedException {

    // create Source Resources
    sourceDBResourceManager = MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);
    sourceConnectionProfile =
        createMySQLSourceConnectionProfile(
            sourceDBResourceManager, Arrays.asList(AUTHORS_TABLE, BOOKS_TABLE));

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName);

    String testRootDir = getClass().getSimpleName();

    // create subscriptions
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    TopicName topic = createPubsubTopic(testRootDir + testName, pubsubResourceManager);
    SubscriptionName subscription =
        createPubsubSubscription(testRootDir + testName, pubsubResourceManager, topic);
    createGcsPubsubNotification(topic, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    TopicName dlqTopic = createPubsubTopic(testRootDir + testName + "dlq", pubsubResourceManager);
    SubscriptionName dlqSubscription =
        createPubsubSubscription(testRootDir + testName + "dlq", pubsubResourceManager, dlqTopic);
    createGcsPubsubNotification(dlqTopic, dlqGcsPrefix, gcsResourceManager);
    String artifactBucket = TestProperties.artifactBucket();

    // launch datastream
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
    Stream stream =
        createDataStreamResources(
            artifactBucket, gcsPrefix, sourceConnectionProfile, datastreamResourceManager);

    // launch dataflow template
    LaunchInfo jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsPrefix,
            stream.getName(),
            dlqGcsPrefix,
            subscription.toString(),
            dlqSubscription.toString(),
            flexTemplateBuilder);
    assertThatPipeline(jobInfo).isRunning();

    MySQLSrcDataProvider.writeRowsInSourceDB(1, 100, sourceDBResourceManager);

    // Wait for messages in pubsub for 5 minutes and consume them
    PullResponse pullResponse = null;
    for (int i = 0; i < 30; ++i) {
      pullResponse = pubsubResourceManager.pull(subscription, 2);
      if (pullResponse.getReceivedMessagesCount() > 0) {
        break;
      }
      Thread.sleep(1000);
    }
    pubsubResourceManager.publish(
        topic, ImmutableMap.of(), pullResponse.getReceivedMessages(0).getMessage().getData());
    pubsubResourceManager.publish(
        topic, ImmutableMap.of(), pullResponse.getReceivedMessages(0).getMessage().getData());

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(100)
                        .setMaxRows(100)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(100)
                        .setMaxRows(100)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }
}
