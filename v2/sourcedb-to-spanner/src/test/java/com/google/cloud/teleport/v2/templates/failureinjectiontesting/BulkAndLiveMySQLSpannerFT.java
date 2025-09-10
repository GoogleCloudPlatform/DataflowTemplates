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
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
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

/**
 * A failure injection test for Bulk + retry DLQ Live migration i.e., SourceDbToSpanner and
 * DataStreamToSpanner templates. The bulk migration template does not retry transient failures.
 * Live migration template is used to retry the failures which happened during the Bulk migration.
 * This test injects Spanner errors to simulate transient errors during Bulk migration and checks
 * the template behaviour.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class BulkAndLiveMySQLSpannerFT extends SourceDbToSpannerFTBase {

  private static final Logger LOG = LoggerFactory.getLogger(BulkAndLiveMySQLSpannerFT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerFailureInjectionTesting/spanner-schema-small-author-name.sql";

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static CloudSqlResourceManager sourceDBResourceManager;
  private static PipelineLauncher.LaunchInfo retryLiveJobInfo;
  private static PubsubResourceManager pubsubResourceManager;
  private static String bulkErrorFolderFullPath;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    super.skipBaseCleanup = true;
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create Source Resources
    sourceDBResourceManager = MySQLSrcDataProvider.createSourceResourceManagerWithSchema(testName);

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // Insert data for bulk before launching the job
    MySQLSrcDataProvider.writeRowsInSourceDB(1, 200, sourceDBResourceManager);

    // create pubsub manager
    pubsubResourceManager = setUpPubSubResourceManager();

    bulkErrorFolderFullPath = getGcsPath("output", gcsResourceManager);

    // launch bulk migration
    bulkJobInfo =
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
        spannerResourceManager, sourceDBResourceManager, pubsubResourceManager);
  }

  @Test
  public void bulkAndRetryDlqFailureTest() throws IOException {

    // Wait for Bulk migration job to be in running state
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo, Duration.ofMinutes(20)));
    assertThatResult(result).meetsConditions();

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    // Total events = successfully processed events + errors in output folder
                    new TotalEventsProcessedCheck(
                        spannerResourceManager,
                        List.of(AUTHORS_TABLE, BOOKS_TABLE),
                        gcsResourceManager,
                        "output/dlq/severe/",
                        400),
                    // There should be at least 1 error
                    DlqEventsCountCheck.builder(gcsResourceManager, "output/dlq/severe/")
                        .setMinEvents(1)
                        .build()))
            .build();

    assertTrue(conditionCheck.get());

    // Correct spanner schema
    spannerResourceManager.executeDdlStatement(
        "ALTER TABLE\n" + "  `Authors` ALTER COLUMN `name` STRING(200);");

    String dlqGcsPrefix = bulkErrorFolderFullPath.replace("gs://" + artifactBucketName, "");
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testName + "dlq", pubsubResourceManager, dlqGcsPrefix, gcsResourceManager);

    // launch forward migration template in retryDLQ mode
    retryLiveJobInfo =
        launchFwdDataflowJobInRetryDlqMode(
            spannerResourceManager,
            bulkErrorFolderFullPath,
            bulkErrorFolderFullPath + "/dlq",
            dlqSubscription);

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(200)
                        .setMaxRows(200)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(200)
                        .setMaxRows(200)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryLiveJobInfo, Duration.ofMinutes(8)), conditionCheck);
    assertThatResult(result).meetsConditions();
  }
}
