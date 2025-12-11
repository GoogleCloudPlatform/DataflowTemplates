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
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.User.USERS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.User.USERS_TABLE_MYSQL_DDL;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.DataflowFailureInjector;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.FuzzyCDCLoadGenerator;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.conditions.CloudSQLRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for verifying correctness of CDC processing in {@link SpannerToSourceDb} Spanner to SourceDb
 * template.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSrcDBMySQLCDCFT extends SpannerToSourceDbFTBase {
  private static final String SPANNER_DDL_RESOURCE = "SpannerToSrcDBMySQLCDCFT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerToSrcDBMySQLCDCFT/session.json";
  private static final String NUM_WORKERS = "13";
  private static final String MAX_WORKERS = "16";

  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager cloudSqlResourceManager;
  private FuzzyCDCLoadGenerator cdcLoadGenerator;

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
    cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();
    cloudSqlResourceManager.createTable(
        USERS_TABLE, new JDBCResourceManager.JDBCSchema(USERS_TABLE_MYSQL_DDL, "id"));

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

    // record startTimeStamp
    Timestamp startTimeStamp = Timestamp.now();

    int numRows = 200;
    int burstIterations = 5000;

    // generate Load
    cdcLoadGenerator = new FuzzyCDCLoadGenerator();
    cdcLoadGenerator.generateLoad(numRows, burstIterations, 0.5, spannerResourceManager);

    // launch reverse migration template
    jobInfo =
        launchRRDataflowJob(
            PipelineUtils.createJobName("rr" + getClass().getSimpleName()),
            "failureInjectionTest",
            Map.of(
                "startTimestamp",
                startTimeStamp.toString(),
                "numWorkers",
                NUM_WORKERS,
                "maxNumWorkers",
                MAX_WORKERS,
                "maxShardConnections",
                MAX_WORKERS,
                "failureInjectionParameter",
                "{\"policyType\":\"TransactionTimeoutInjectionPolicy\", \"policyInput\": { \"injectionWindowDuration\": \"PT30M\", \"delayDuration\": \"PT80S\" }}"),
            null,
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
  public void reverseReplicationCrossDbTxnCdcTest() throws IOException, InterruptedException {

    assertThatPipeline(jobInfo).isRunning();

    long expectedRows = spannerResourceManager.getRowCount(USERS_TABLE);
    // Wait for exact number of rows as Spanner to appear in source
    ConditionCheck sourceDbRowCountCondition =
        CloudSQLRowsCheck.builder(cloudSqlResourceManager, USERS_TABLE)
            .setMinRows((int) expectedRows)
            .setMaxRows((int) expectedRows)
            .build();

      PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(jobInfo, Duration.ofHours(1)), sourceDbRowCountCondition);
    assertThatResult(result).meetsConditions();

    // Usually the dataflow finishes processing the events within 10 minutes. Giving 10 more minutes
    // buffer for the dataflow job to process the events before asserting the results.
    Thread.sleep(600000);

    // Read data from Spanner and assert that it exactly matches with SourceDb
    cdcLoadGenerator.assertRows(spannerResourceManager, cloudSqlResourceManager);
  }
}
