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

import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.BOOKS_TABLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.NetworkFailureInjector;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure injection test for the SourceDB to Spanner template. This test simulates a Source Db
 * temporary failure scenario to ensure the pipeline remains resilient and processes all data
 * without loss.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSrcDbToSpannerSourceFT extends SourceDbToSpannerFTBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLSrcDbToSpannerSourceFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static NetworkFailureInjector networkFailureInjector;

  private static CloudSqlResourceManager sourceDBResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create Source Resources
    sourceDBResourceManager = MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // Insert data before launching the job
    MySQLSrcDataProvider.writeRowsInSourceDB(1, 1000, sourceDBResourceManager);

    // create network failure injector
    networkFailureInjector =
        NetworkFailureInjector.builder(PROJECT, "nokill-failure-testing-mysql-source-vpc").build();

    // launch forward migration template
    jobInfo =
        launchBulkDataflowJob(
            getClass().getSimpleName(),
            spannerResourceManager,
            gcsResourceManager,
            sourceDBResourceManager);
  }

  /**
   * Cleanup all the resources and resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        gcsResourceManager,
        sourceDBResourceManager,
        networkFailureInjector);
  }

  @Test
  public void sourceDbFailureTest() throws ExecutionException, InterruptedException {

    // Wait for Bulk migration job to be in running state
    assertThatPipeline(jobInfo).isRunning();

    // Inject Source Db failure
    networkFailureInjector.blockNetwork(
        "failure-testing-mysql-host", sourceDBResourceManager.getPort());

    // Verify that the network is blocked
    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .until(
            () ->
                !isHostReachable(
                    sourceDBResourceManager.getHost(), sourceDBResourceManager.getPort(), 5000));
    LOG.info("Successfully verified that the source host is unreachable.");

    // Wait for 3 minutes before unblocking the network
    Thread.sleep(180000);

    // Unblock network
    networkFailureInjector.cleanupAll();
    LOG.info("Network connectivity restored.");

    ConditionCheck conditionCheck =
        new TotalEventsProcessedCheck(
            spannerResourceManager,
            List.of(AUTHORS_TABLE, BOOKS_TABLE),
            gcsResourceManager,
            "output/dlq/severe",
            2000);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
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
