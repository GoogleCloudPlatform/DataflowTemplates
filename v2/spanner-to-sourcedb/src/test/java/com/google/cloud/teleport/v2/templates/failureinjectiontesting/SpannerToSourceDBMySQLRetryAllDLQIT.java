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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for reverse replication from Spanner to MySQL using the retryAllDLQ mode.
 *
 * <p>Objective: Verify that the retryAllDLQ batch job correctly processes and retries ALL Dead
 * Letter Queue (DLQ) events when the main pipeline is stopped.
 *
 * <p>Edge cases covered in this test include: - Handling retriable errors such as check constraint
 * and foreign key violations via the retryAllDLQ pipeline. - Processing severe errors introduced by
 * custom transformation failures via the retryAllDLQ pipeline. - Retrying fixed items successfully
 * in both retry/ and severe/ buckets: e.g. fixing a foreign key violation by inserting a missing
 * parent row, and using a corrected transformation file. - Ensuring non-fixable items remain
 * correctly logged under their respective error buckets. - Validating schema complexities between
 * Source and Spanner, including mismatched primary keys, added, deleted, and renamed columns, as
 * well as all datatypes. - Utilizing the schema overrides file to reconcile schema differences. -
 * Utilizing the static DLQ file-based consumer (instead of the Pub/Sub consumer flow).
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBMySQLRetryAllDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBMySQLRetryAllDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDBMySQLRetryAllDLQIT/spanner-schema.sql";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDBMySQLRetryAllDLQIT/mysql-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "SpannerToSourceDBMySQLRetryAllDLQIT/overrides.json";

  private static final HashSet<SpannerToSourceDBMySQLRetryAllDLQIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static MySQLResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBMySQLRetryAllDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDBMySQLRetryAllDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = MySQLResourceManager.builder(testName).build();

        createMySQLSchema(
            jdbcResourceManager, SpannerToSourceDBMySQLRetryAllDLQIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

        // Upload overrides file
        gcsResourceManager.uploadArtifact(
            "input/overrides.json", Resources.getResource(OVERRIDES_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", // Use relative path!
                    "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters("mode=bad")
                .build();

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put(
                    "schemaOverridesFilePath",
                    getGcsPath("input/overrides.json", gcsResourceManager));
                put("dlqMaxRetryCount", "20");
                put(
                    "dlqRetryMinutes",
                    "60"); // keeping these high so that the test can comfortably read the static
                // retry/ bucket
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                null, // Passing null disables Pub/Sub consumer, leaving retry DLQ items statically
                // in the bucket
                null,
                null,
                null,
                null,
                customTransformation,
                MYSQL_SOURCE_TYPE,
                jobParameters);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBMySQLRetryAllDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testSpannerToSrcDBRetryAllDLQ() throws Exception {
    LOG.info("Starting testSpannerToSrcDBRetryAllDLQ");
    assertThatPipeline(jobInfo).isRunning();

    // 1. Insert parent rows directly into MySQL. This prevents out-of-order Dataflow failures
    //    since Dataflow processes asynchronously and might process child rows before parent rows.
    LOG.info("Inserting parent rows directly into MySQL");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (2, 'Customer 2', 1500, 'Silver')");

    // 2. Insert all test data into the source Spanner database.
    LOG.info("Inserting test data into Spanner");
    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

    // 3. Wait for DLQ events to appear in corresponding buckets.
    // Total events expected:
    // - Customers: 1
    // - Orders: 1
    // - AllDataTypes: 2
    // Total: 4
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                ChainedConditionCheck.builder(
                        List.of(
                            DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
                                .setMinEvents(2)
                                .build(),
                            DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                                .setMinEvents(2)
                                .build()))
                    .build());
    assertThatResult(dlqWaitResult).meetsConditions();
    LOG.info("DLQ events successfully generated in corresponding buckets");

    // 4. Stop the regular pipeline. The retry pipeline must run independently.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());

    // Wait for the pipeline job to be cancelled (up to 5 minutes)
    boolean cancelled = false;
    for (int i = 0; i < 30; i++) {
      PipelineLauncher.JobState status =
          pipelineLauncher.getJobStatus(PROJECT, REGION, jobInfo.jobId());
      if (status == PipelineLauncher.JobState.CANCELLED
          || status == PipelineLauncher.JobState.DRAINED) {
        cancelled = true;
        break;
      }
      Thread.sleep(10000);
    }
    assertTrue("Job did not cancel in time", cancelled);
    LOG.info("Regular pipeline stopped successfully");

    // 5. Apply partial fixes to simulate user intervention correcting data before DLQ retry.
    // Insert parent for Orders to fix the foreign key violation.
    LOG.info("Applying partial fixes in MySQL (inserting missing parent row for Orders)");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO Customers (CustomerId, CustomerName, CreditLimit, LegacyRegion) VALUES (3, 'Parent Customer', 2000, 'Gold')");

    // 6. Launch a new Dataflow job in retryAllDLQ mode to process the DLQ items.
    LOG.info("Launching retryAllDLQ job with schema overrides to process DLQ");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryAllDLQ");
    retryParams.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));
    retryParams.put("dlqMaxRetryCount", "20");
    retryParams.put("dlqRetryMinutes", "60");

    PipelineLauncher.LaunchInfo retryJobInfo =
        launchDataflowJob(
            gcsResourceManager,
            spannerResourceManager,
            spannerMetadataResourceManager,
            null,
            null,
            null,
            null,
            null,
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.SpannerToSourceDbRetryTransformation")
                .setCustomParameters(
                    "mode=semi-fixed") // This fixes one of our severe errors simulated in the
                // transformer
                .build(),
            MYSQL_SOURCE_TYPE,
            retryParams);
    LOG.info("RetryAllDLQ job launched: {}", retryJobInfo.jobId());

    assertThatPipeline(retryJobInfo).isRunning();

    // 7. Wait for the retry job to process events and ensure they reach the DLQ correctly BEFORE
    // cancelling.
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets after retry");
    ConditionCheck dlqConditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
                        .setMinEvents(1)
                        .setMaxEvents(1)
                        .build(),
                    DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                        .setMinEvents(1)
                        .setMaxEvents(1)
                        .build()))
            .build();

    PipelineOperator.Result retryResult =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryJobInfo, Duration.ofMinutes(15)), dlqConditionCheck);

    assertThatResult(retryResult).meetsConditions();
    LOG.info("Retry job completed processing successfully");

    // 8. Verify target MySQL database has the correct updated state.
    LOG.info("Verifying target MySQL database contents");

    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManager, "AllDataTypes")
            .setMinRows(2)
            .setMaxRows(2)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManager, "Customers")
            .setMinRows(2)
            .setMaxRows(2)
            .build()
            .get());
    assertTrue(
        JDBCRowsCheck.builder(jdbcResourceManager, "Orders")
            .setMinRows(2)
            .setMaxRows(2)
            .build()
            .get());

    // AllDataTypes:
    // id=1 should exist
    // id=999 should exist (fixed)
    // id=888 should NOT exist (written back since the transformation error wasn't fixed)
    List<Map<String, Object>> allDataTypesRows =
        jdbcResourceManager.runSQLQuery("SELECT id FROM AllDataTypes");
    List<Integer> allDataTypesIds =
        allDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist", allDataTypesIds.contains(1));
    assertTrue("id=999 should exist", allDataTypesIds.contains(999));
    assertTrue("id=888 should NOT exist", !allDataTypesIds.contains(888));

    // Customers:
    // id=2 should exist (inserted directly)
    // id=3 should exist (inserted as a partial fix)
    // id=1 should NOT exist (check constraint violation wasn't fixed)
    List<Map<String, Object>> customersRows =
        jdbcResourceManager.runSQLQuery("SELECT CustomerId FROM Customers");
    List<Integer> customersIds =
        customersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=1 should NOT exist", !customersIds.contains(1));
    assertTrue("id=2 should exist", customersIds.contains(2));
    assertTrue("id=3 should exist", customersIds.contains(3));

    // Orders:
    // id=101 should exist (FK issue fixed by inserting parent)
    // id=102 should exist (parent was seeded originally)
    List<Map<String, Object>> ordersRows =
        jdbcResourceManager.runSQLQuery("SELECT OrderId FROM Orders");
    List<Integer> ordersIds =
        ordersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should exist", ordersIds.contains(101));
    assertTrue("id=102 should exist", ordersIds.contains(102));

    LOG.info("Verified target MySQL database contents successfully");
  }

  private Integer getIntValueCaseInsensitive(Map<String, Object> map, String key) {
    for (String k : map.keySet()) {
      if (k.equalsIgnoreCase(key)) {
        Object val = map.get(k);
        if (val instanceof Number) {
          return ((Number) val).intValue();
        }
      }
    }
    return null;
  }

  private void insertDataInSpanner() {
    com.google.cloud.spanner.Mutation customer1 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Customers")
            .set("CustomerId")
            .to(1)
            .set("CustomerName")
            .to("Customer 1")
            .set("CreditLimit")
            .to(500) // this will fail due to check constraint at source
            .set("LoyaltyTier")
            .to("Bronze")
            .build();

    com.google.cloud.spanner.Mutation order101 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Orders")
            .set("OrderId")
            .to(101)
            .set("CustomerId")
            .to(3) // fails due to no parent row in Customers
            .set("OrderValue")
            .to(1000)
            .set("OrderSource")
            .to("Website")
            .build();
    com.google.cloud.spanner.Mutation order102 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("Orders")
            .set("OrderId")
            .to(102)
            .set("CustomerId")
            .to(2)
            .set("OrderValue")
            .to(1000)
            .set("OrderSource")
            .to("AppStore")
            .build();

    com.google.cloud.spanner.Mutation allTypes1 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(1)
            .set("boolean_col")
            .to(true)
            .set("varchar_col")
            .to("test1")
            .set("bit8_col")
            .to(11)
            .set("bit1_col")
            .to(true)
            .build();
    com.google.cloud.spanner.Mutation allTypes999 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(999) // Bad transformer fails on purpose, good transformer doesnt
            .set("boolean_col")
            .to(false)
            .set("varchar_col")
            .to("test999")
            .set("bit8_col")
            .to(22)
            .set("bit1_col")
            .to(false)
            .build();
    com.google.cloud.spanner.Mutation allTypes888 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(888) // Bad and good transformer fail on purpose
            .set("boolean_col")
            .to(true)
            .set("varchar_col")
            .to("test888")
            .set("bit8_col")
            .to(33)
            .set("bit1_col")
            .to(true)
            .build();

    spannerResourceManager.write(
        List.of(customer1, order101, order102, allTypes1, allTypes999, allTypes888));
  }

  private String getCustomShardJarPath() {
    String userDir = System.getProperty("user.dir");
    if (userDir.endsWith("v2/spanner-to-sourcedb")) {
      return "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
    }
    return "v2/spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  }
}
