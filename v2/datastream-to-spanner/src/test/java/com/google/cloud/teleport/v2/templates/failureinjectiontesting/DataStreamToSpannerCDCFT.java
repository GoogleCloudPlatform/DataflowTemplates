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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.CDCCorrectnessTestUtil;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.DataflowFailureInjector;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for verifying correctness of CDC processing in {@link DataStreamToSpanner} DataStream to
 * Spanner template.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerCDCFT extends DataStreamToSpannerFTBase {

  private static final String SPANNER_DDL_RESOURCE = "DataStreamToSpannerCDCFT/spanner-schema.sql";
  public static final String USERS_TABLE = "Users";
  public static final HashMap<String, String> USERS_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT NOT NULL");
          put("first_name", "VARCHAR(200)");
          put("last_name", "VARCHAR(200)");
          put("age", "INT");
          put("status", "TINYINT(1)");
          put("col1", "BIGINT");
          put("col2", "BIGINT");
        }
      };
  private static final int NUM_WORKERS = 10;
  private static final int MAX_WORKERS = 20;

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager shadowTableSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager sourceDBResourceManager;
  private JDBCSource sourceConnectionProfile;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    shadowTableSpannerResourceManager =
        SpannerResourceManager.builder("shadow-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    shadowTableSpannerResourceManager.ensureUsableAndCreateResources();
    // create Source Resources
    sourceDBResourceManager = CloudMySQLResourceManager.builder(testName).build();
    sourceDBResourceManager.createTable(
        USERS_TABLE, new JDBCResourceManager.JDBCSchema(USERS_TABLE_COLUMNS, "id"));
    sourceConnectionProfile =
        createMySQLSourceConnectionProfile(sourceDBResourceManager, List.of(USERS_TABLE));

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // create pubsub manager
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
        spannerResourceManager,
        sourceDBResourceManager,
        datastreamResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void liveMigrationCrossDbTxnCdcTest()
      throws IOException, InterruptedException, SQLException, ExecutionException {
    String testRootDir = getClass().getSimpleName();
    // create subscriptions
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            testRootDir + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testRootDir + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager);
    String artifactBucket = TestProperties.artifactBucket();

    // launch datastream
    DatastreamResourceManager.Builder datastreamResourceManagerBuilder =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider);
    if (System.getProperty("privateConnectivity") != null) {
      datastreamResourceManagerBuilder.setPrivateConnectivity(
          System.getProperty("privateConnectivity"));
    }
    datastreamResourceManager = datastreamResourceManagerBuilder.build();
    Stream stream =
        createDataStreamResources(
            artifactBucket, gcsPrefix, sourceConnectionProfile, datastreamResourceManager);

    int numRows = 100;
    int burstIterations = 10000;

    // generate Load
    CDCCorrectnessTestUtil testUtil = new CDCCorrectnessTestUtil();
    testUtil.generateLoad(numRows, burstIterations, sourceDBResourceManager);

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName)
            .addEnvironmentVariable("numWorkers", NUM_WORKERS)
            .addEnvironmentVariable("maxWorkers", MAX_WORKERS);

    // launch dataflow template
    PipelineLauncher.LaunchInfo jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsPrefix,
            stream.getName(),
            dlqGcsPrefix,
            subscription.toString(),
            dlqSubscription.toString(),
            flexTemplateBuilder,
            shadowTableSpannerResourceManager);

    // Wait for Forward migration job to be in running state
    assertThatPipeline(jobInfo).isRunning();

    // Wait for at least 1 row to appear in Spanner
    ConditionCheck conditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, USERS_TABLE).setMinRows(1).build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Kill the dataflow workers multiple times to induce work item assignment rebalancing.
    DataflowFailureInjector.abruptlyKillWorkers(jobInfo.projectId(), jobInfo.jobId());
    Thread.sleep(20000); // wait for 20 seconds
    DataflowFailureInjector.abruptlyKillWorkers(jobInfo.projectId(), jobInfo.jobId());
    Thread.sleep(20000);
    DataflowFailureInjector.abruptlyKillWorkers(jobInfo.projectId(), jobInfo.jobId());
    Thread.sleep(20000);
    DataflowFailureInjector.abruptlyKillWorkers(jobInfo.projectId(), jobInfo.jobId());
    Thread.sleep(20000);

    Thread.sleep(600000);

    long expectedRows = sourceDBResourceManager.getRowCount(USERS_TABLE);
    // Wait for exact number of rows as source to appear in Spanner
    conditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, USERS_TABLE)
            .setMinRows((int) expectedRows)
            .setMaxRows((int) expectedRows)
            .build();
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Usually the dataflow finishes processing the events within 10 minutes. Giving 10 more minutes
    // buffer for the dataflow job to process the events before asserting the results.
    Thread.sleep(600000);

    // Read data from Spanner and assert that it exactly matches with SourceDb
    testUtil.assertRows(spannerResourceManager, sourceDBResourceManager);
  }
}
