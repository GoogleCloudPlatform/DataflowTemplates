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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.SpannerDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.SpannerDataProvider.BOOKS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.SpannerDataProvider;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.conditions.CloudSQLRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A failure injection test for the Spanner to SourceDb template. This test simulates a Dataflow
 * worker failure scenario to ensure the pipeline remains resilient and processes all data without
 * loss.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSrcDBMySQLDlqPubsubFT extends SpannerToSourceDbFTBase {
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema-without-interleave.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerFailureInjectionTesting/session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static TopicName topic;
  private static SubscriptionName subscription;

  private static CloudSqlResourceManager cloudSqlResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    // create MySql Resources
    cloudSqlResourceManager = MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);
    MySQLSrcDataProvider.createForeignKeyConstraint(cloudSqlResourceManager);

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();
    createAndUploadReverseShardConfigToGcs(
        gcsResourceManager, cloudSqlResourceManager, cloudSqlResourceManager.getHost());
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

    // create pubsub manager
    pubsubResourceManager = setUpPubSubResourceManager();

    String topicNameSuffix = "rr-ft" + getClass().getSimpleName();
    String subscriptionNameSuffix = "rr-ft-sub" + getClass().getSimpleName();
    topic = pubsubResourceManager.createTopic(topicNameSuffix);
    subscription = pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, "");
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    prefix += "/retry/";
    gcsResourceManager.createNotification(topic.toString(), prefix);

    // launch reverse migration template
    jobInfo =
        launchRRDataflowJob(
            PipelineUtils.createJobName("rr" + getClass().getSimpleName()),
            null,
            null,
            subscription,
            spannerResourceManager,
            gcsResourceManager,
            spannerMetadataResourceManager,
            pubsubResourceManager,
            MYSQL_SOURCE_TYPE);
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cloudSqlResourceManager);
  }

  @Test
  public void dataflowWorkerFailureTest() throws IOException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    // Wave of inserts
    SpannerDataProvider.writeBookRowsInSpanner(1, 100, null, spannerResourceManager);

    // Wait for messages in pubsub for 5 minutes and consume them
    PullResponse pullResponse = null;
    for (int i = 0; i < 30; ++i) {
      pullResponse = pubsubResourceManager.pull(subscription, 2);
      if (pullResponse.getReceivedMessagesCount() > 0) {
        break;
      }
      Thread.sleep(1000);
    }
    SpannerDataProvider.writeAuthorRowsInSpanner(1, 100, spannerResourceManager);

    pubsubResourceManager.publish(
        topic, ImmutableMap.of(), pullResponse.getReceivedMessages(0).getMessage().getData());
    pubsubResourceManager.publish(
        topic, ImmutableMap.of(), pullResponse.getReceivedMessages(0).getMessage().getData());

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    CloudSQLRowsCheck.builder(cloudSqlResourceManager, AUTHORS_TABLE)
                        .setMinRows(100)
                        .setMaxRows(100)
                        .build(),
                    CloudSQLRowsCheck.builder(cloudSqlResourceManager, BOOKS_TABLE)
                        .setMinRows(100)
                        .setMaxRows(100)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }
}
