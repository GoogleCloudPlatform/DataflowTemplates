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
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.NetworkFailureInjector;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.SpannerDataProvider;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.conditions.CloudSQLRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure injection test for the Spanner to SourceDb template. This test simulates a Source Db
 * temporary failure scenario to ensure the pipeline remains resilient and processes all data
 * without loss.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSrcDBMySQLSourceFT extends SpannerToSourceDbFTBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSrcDBMySQLSourceFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerFailureInjectionTesting/session.json";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static NetworkFailureInjector networkFailureInjector;

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

    // create network failure injector
    networkFailureInjector =
        NetworkFailureInjector.builder(PROJECT, "nokill-failure-testing-mysql-source-vpc").build();

    // launch reverse migration template
    jobInfo =
        launchRRDataflowJob(
            PipelineUtils.createJobName("rr" + getClass().getSimpleName()),
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
        cloudSqlResourceManager,
        networkFailureInjector);
  }

  @Test
  public void sourceDbFailureTest() throws IOException, ExecutionException, InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    // Wave of inserts
    SpannerDataProvider.writeRowsInSpanner(1, 2, spannerResourceManager);

    // Wait for at least one row to appear in source - indicates job is in running state
    ConditionCheck conditionCheck =
        CloudSQLRowsCheck.builder(cloudSqlResourceManager, AUTHORS_TABLE).setMinRows(1).build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Inject Source Db failure
    networkFailureInjector.blockNetwork(
        "failure-testing-mysql-host", cloudSqlResourceManager.getPort());

    // Verify that the network is blocked
    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .until(
            () ->
                !isHostReachable(
                    cloudSqlResourceManager.getHost(), cloudSqlResourceManager.getPort(), 5000));
    LOG.info("Successfully verified that the source host is unreachable.");

    // Insert more data while network is blocked
    SpannerDataProvider.writeRowsInSpanner(1001, 2000, spannerResourceManager);

    // Wait for 3 minutes before unblocking the network
    Thread.sleep(180000);

    // Unblock network
    networkFailureInjector.cleanupAll();
    LOG.info("Network connectivity restored.");

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    CloudSQLRowsCheck.builder(cloudSqlResourceManager, AUTHORS_TABLE)
                        .setMinRows(1002)
                        .setMaxRows(1002)
                        .build(),
                    CloudSQLRowsCheck.builder(cloudSqlResourceManager, BOOKS_TABLE)
                        .setMinRows(1002)
                        .setMaxRows(1002)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }

  private static boolean isHostReachable(String host, int port, int timeoutMillis) {
    try (Socket socket = new Socket()) {
      socket.connect(new java.net.InetSocketAddress(host, port), timeoutMillis);
      return true;
    } catch (IOException e) {
      return false; // Either timeout or unreachable or failed DNS lookup.
    }
  }
}
