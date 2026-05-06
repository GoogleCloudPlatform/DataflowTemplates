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

import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.User.USERS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.User.USERS_TABLE_MYSQL_DDL;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.FuzzyCDCLoadGenerator;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
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
 * Fault Tolerance (FT) test for verifying the robustness and correctness of Change Data Capture
 * (CDC) processing in the {@link DataStreamToSpanner} Dataflow template.
 *
 * <p>Objective: Verify that the pipeline can successfully process and replicate CDC events from a
 * MySQL source database to a Spanner target database, even when facing persistent network timeouts
 * and Spanner transaction aborts during peak load.
 *
 * <p>Edge cases covered in this test include:
 *
 * <ul>
 *   <li>Handling simulated Spanner transaction timeouts using the {@code
 *       TransactionTimeoutInjectionPolicy}.
 *   <li>Replicating large bursts of CDC events (inserts/updates/deletes) simultaneously while
 *       undergoing the injected failure condition.
 *   <li>Ensuring the template successfully retries aborted commits and guarantees consistency in
 *       CDC event processing without data loss.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerCDCFT extends DataStreamToSpannerFTBase {

  private static final String SPANNER_DDL_RESOURCE = "DataStreamToSpannerCDCFT/spanner-schema.sql";

  private static final int NUM_WORKERS = 10;
  private static final int MAX_WORKERS = 20;

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager shadowTableSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  private static CloudSqlResourceManager sourceDBResourceManager;
  private JDBCSource sourceConnectionProfile;
  private FuzzyCDCLoadGenerator cdcLoadGenerator;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // 1. Create Target Spanner Databases
    // The target database stores the replicated data. A separate Spanner database is explicitly
    // created and passed to the Dataflow job as the shadow database (used for internal state
    // management). Passing a distinct shadow database triggers the
    // "Separate Shadow Table" distributed transaction algorithm for processing CDC events.
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    shadowTableSpannerResourceManager =
        SpannerResourceManager.builder("shadow-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    shadowTableSpannerResourceManager.ensureUsableAndCreateResources();

    // 2. Create Source MySQL Database
    // Set up the upstream MySQL database and create the Users table.
    sourceDBResourceManager = CloudMySQLResourceManager.builder(testName).build();
    sourceDBResourceManager.createTable(
        USERS_TABLE, new JDBCResourceManager.JDBCSchema(USERS_TABLE_MYSQL_DDL, "id"));
    sourceConnectionProfile =
        createMySQLSourceConnectionProfile(sourceDBResourceManager, List.of(USERS_TABLE));

    // 3. Create and upload GCS Resources
    // Set up the GCS bucket where Datastream will write CDC files.
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // 4. Create Pub/Sub Resources
    // Set up Pub/Sub topics and subscriptions to notify Dataflow of new files in GCS.
    // Separate topics are created for the main data stream and Dead Letter Queue (DLQ).
    pubsubResourceManager = setUpPubSubResourceManager();

    String testRootDir = getClass().getSimpleName();
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

    // 5. Launch Datastream Stream
    // Create and start the Datastream stream to replicate data from MySQL to the GCS bucket.
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

    // 6. Generate CDC Load
    // Initialize the source with 100 base rows, then simulate 10,000 random burst mutations
    // (inserts, updates, deletes) with a probability of 0.5 to generate CDC stream traffic.
    cdcLoadGenerator = new FuzzyCDCLoadGenerator();
    cdcLoadGenerator.generateLoad(numRows, burstIterations, 0.5, sourceDBResourceManager);

    // 7. Configure Dataflow Failure Injection
    // This sets the TransactionTimeoutInjectionPolicy, which simulates 260-second long stalls
    // in Spanner transactions over a 30-minute window, causing standard Spanner RPCs to timeout and
    // abort.
    //
    // NOTE: This test is estimated to take around 45-50 minutes to complete (30-minute injection
    // window + setup + post-recovery processing buffer).
    //
    // Since the Datastream stream is started first and then the CDC load is generated before the
    // Dataflow job is launched, the pipeline will immediately see all the generated CDC events as a
    // massive burst upon startup.
    //
    // During the 30-minute window, the simulated 260-second delay exceeds standard Spanner Java
    // Client timeout settings (typically 60 seconds for commit RPCs). This results in
    // DEADLINE_EXCEEDED errors. This policy specifically tests the "Separate Shadow Table"
    // transaction algorithm, where the shadow database (tracking stream offsets) and the
    // main database are distinct. The algorithm utilizes nested transactions to ensure
    // consistency.
    //
    // After the 30-minute window expires, failures are no longer injected. The pipeline then
    // rapidly
    // processes the remaining backlog and successfully retries any previously failed events.
    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName)
            .withAdditionalMavenProfile("failureInjectionTest")
            .addParameter(
                "failureInjectionParameter",
                "{\"policyType\":\"TransactionTimeoutInjectionPolicy\", \"policyInput\": { \"injectionWindowDuration\": \"PT30M\", \"delayDuration\": \"PT260S\" }}")
            .addEnvironmentVariable("numWorkers", NUM_WORKERS)
            .addEnvironmentVariable("maxWorkers", MAX_WORKERS);

    // 8. Launch the Dataflow CDC template
    jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsPrefix,
            stream.getName(),
            dlqGcsPrefix,
            subscription.toString(),
            dlqSubscription.toString(),
            flexTemplateBuilder,
            shadowTableSpannerResourceManager);
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

    // 1. Wait for the Forward migration job to be in a running state
    assertThatPipeline(jobInfo).isRunning();

    long expectedRows = sourceDBResourceManager.getRowCount(USERS_TABLE);

    // 2. Wait for the exact number of rows from the source to successfully appear in Spanner.
    // This checks that despite the induced transaction timeouts, the pipeline eventually
    // retries and converges to the correct row count.
    ConditionCheck spannerRowCountConditionCheck =
        SpannerRowsCheck.builder(spannerResourceManager, USERS_TABLE)
            .setMinRows((int) expectedRows)
            .setMaxRows((int) expectedRows)
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofHours(1)), spannerRowCountConditionCheck);
    assertThatResult(result).meetsConditions();

    // 3. Usually the dataflow finishes processing the events within 10 minutes. Giving 10 more
    // minutes buffer for the dataflow job to process the events and ensure no late-arriving
    // events alter the state.
    Thread.sleep(600000);

    // 4. Read data from both Spanner and MySQL and assert that the exact row states match,
    // validating the content data integrity and consistency between SourceDb and Spanner.
    cdcLoadGenerator.assertRows(spannerResourceManager, sourceDBResourceManager);
  }
}
