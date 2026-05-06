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
 * Fault Tolerance (FT) test for verifying the robustness and correctness of Change Data Capture
 * (CDC) processing in the {@link SpannerToSourceDb} Dataflow template.
 *
 * <p>Objective: Verify that the Reverse Replication (RR) pipeline can successfully process CDC
 * events from a Spanner change stream and write them to a target MySQL database, even when facing
 * persistent transaction timeouts at the sink.
 *
 * <p>Edge cases covered in this test include:
 *
 * <ul>
 *   <li>Handling simulated MySQL transaction timeouts using the {@code
 *       TransactionTimeoutInjectionPolicy}.
 *   <li>Processing large bursts of CDC events (inserts/updates/deletes) from Spanner simultaneously
 *       while undergoing the injected failure condition.
 *   <li>Ensuring the pipeline successfully retries aborted transactions and guarantees consistency
 *       in CDC events processing without duplicating records.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSrcDBMySQLCDCFT extends SpannerToSourceDbFTBase {
  private static final String SPANNER_DDL_RESOURCE = "SpannerToSrcDBMySQLCDCFT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "SpannerToSrcDBMySQLCDCFT/session.json";
  private static final String NUM_WORKERS = "10";
  private static final String MAX_WORKERS = "20";

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
    // 1. Create Source Spanner Databases
    // The main database holds the data and the change stream.
    // The metadata database is used by the pipeline to store internal state.
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    // 2. Create Target MySQL Database
    // Set up the downstream Cloud SQL MySQL database and create the target Users table.
    cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();
    cloudSqlResourceManager.createTable(
        USERS_TABLE, new JDBCResourceManager.JDBCSchema(USERS_TABLE_MYSQL_DDL, "id"));

    // 3. Create and upload GCS Resources
    // Set up the GCS bucket and upload the reverse replication shard configuration
    // and Spanner session files required by the pipeline.
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();
    createAndUploadReverseShardConfigToGcs(
        gcsResourceManager, cloudSqlResourceManager, cloudSqlResourceManager.getHost());
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

    // 4. Create Pub/Sub Resources
    // Set up Pub/Sub resources for DLQ processing and error notifications.
    pubsubResourceManager = setUpPubSubResourceManager();

    // 5. Record Start Timestamp
    // Capture the current time so the pipeline only streams events that occur from this point
    // forward.
    Timestamp startTimeStamp = Timestamp.now();

    int numRows = 200;
    int burstIterations = 5000;

    // 6. Generate CDC Load
    // Simulate high traffic by loading 200 initial rows and generating 5,000 random burst mutations
    // directly into the Spanner source database.
    cdcLoadGenerator = new FuzzyCDCLoadGenerator();
    cdcLoadGenerator.generateLoad(numRows, burstIterations, 0.5, spannerResourceManager);

    // 7. Launch Dataflow Template with Failure Injection
    // This configures the TransactionTimeoutInjectionPolicy, simulating 80-second stalls
    // for JDBC transactions over a 30-minute window.
    //
    // NOTE: This test is estimated to take around 45-50 minutes to complete (30-minute injection
    // window + setup + post-recovery processing buffer).
    //
    // The `startTimestamp` passed here was captured before the CDC load generation. Since the
    // Dataflow job is launched after all CDC events are loaded into Spanner, it will immediately
    // face all the CDC events as a burst. During the 30-minute window, the simulated
    // 80-second delay exceeds standard Spanner transaction timeout, forcing transaction
    // rollbacks. The pipeline must recover and ensure consistency in CDC events processing.
    //
    // After the 30-minute window expires, failures are no longer injected. The pipeline then
    // rapidly processes the accumulated burst backlog and successfully retries any previously
    // failed events.
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

    // 1. Wait for the Reverse Replication job to be in a running state
    assertThatPipeline(jobInfo).isRunning();

    long expectedRows = spannerResourceManager.getRowCount(USERS_TABLE);

    // 2. Wait for the exact number of rows from Spanner to successfully appear in the target MySQL
    // database. This verifies that despite the induced transaction rollbacks at the JDBC sink, the
    // pipeline successfully retried the failed events until all data was synced.
    ConditionCheck sourceDbRowCountCondition =
        CloudSQLRowsCheck.builder(cloudSqlResourceManager, USERS_TABLE)
            .setMinRows((int) expectedRows)
            .setMaxRows((int) expectedRows)
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofHours(1)), sourceDbRowCountCondition);
    assertThatResult(result).meetsConditions();

    // 3. Usually the dataflow finishes processing the events within 10 minutes. Giving 10 more
    // minutes buffer for the dataflow job to process the events and ensure no late-arriving
    // events alter the state.
    Thread.sleep(600000);

    // 4. Read data from both Spanner and MySQL and assert that the exact row states match,
    // validating the content data integrity and consistency between Spanner and the MySQL target.
    cdcLoadGenerator.assertRows(spannerResourceManager, cloudSqlResourceManager);
  }
}
