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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_ORACLE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for reverse replication from Spanner to Oracle using the retryDLQ mode. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDBOracleRetryDLQIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToSourceDBOracleRetryDLQIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToSourceDBOracleRetryDLQIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToSourceDBOracleRetryDLQIT/oracle-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "oracle/SpannerToSourceDBOracleRetryDLQIT/overrides.json";

  private static final HashSet<SpannerToSourceDBOracleRetryDLQIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (SpannerToSourceDBOracleRetryDLQIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToSourceDBOracleRetryDLQIT.SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
        testUsername = setupOracleIsolatedUser(jdbcResourceManager);

        createOracleTableWithNColumns(jdbcResourceManager, "test", 1, "25");

        createOracleSchema(
            jdbcResourceManager,
            SpannerToSourceDBOracleRetryDLQIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsername);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);

        // Upload overrides file
        gcsResourceManager.uploadArtifact(
            "input/overrides.json", Resources.getResource(OVERRIDES_FILE_RESOURCE).getPath());

        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters("mode=bad")
                .build();

        gcsResourceManager.uploadArtifact("input/customShard.jar", getCustomShardJarPath());

        pubsubResourceManager = setUpPubSubResourceManager();
        SubscriptionName subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);

        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put(
                    "schemaOverridesFilePath",
                    getGcsPath("input/overrides.json", gcsResourceManager));
                put("dlqRetryMinutes", "1");
                put("dlqMaxRetryCount", "1000");
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                customTransformation,
                SOURCE_ORACLE,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSourceDBOracleRetryDLQIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToSrcDBRetryDLQ() throws Exception {
    assertThatPipeline(jobInfo).isRunning();

    // Insert parent rows directly into Oracle to prevent out-of-order Dataflow failures.
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \""
            + testUsername
            + "\".\"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\", \"LegacyRegion\") VALUES (2, 'Customer 2', 1500, 'Silver')");

    jdbcResourceManager.runSQLUpdate("COMMIT");
    insertDataInSpanner();

    LOG.info("Waiting for DLQ events to appear in severe bucket");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManager, "\"" + testUsername + "\".\"Orders\"")
                            .setMinRows(1) // id = 102
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManager, "\"" + testUsername + "\".\"AllDataTypes\"")
                            .setMinRows(1) // id = 1
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManager, "\"" + testUsername + "\".\"Customers\"")
                            .setMinRows(1) // id = 2
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();

    LOG.info("Verifying Oracle state before retry job runs");
    List<Map<String, Object>> customersRows =
        runIsolatedSQLQuery(
            jdbcResourceManager, testUsername, "SELECT \"CustomerId\" FROM \"Customers\"");
    List<Integer> customersIds =
        customersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=1 should NOT exist yet", !customersIds.contains(1));

    List<Map<String, Object>> ordersRows =
        runIsolatedSQLQuery(
            jdbcResourceManager, testUsername, "SELECT \"OrderId\" FROM \"Orders\"");
    List<Integer> ordersIds =
        ordersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should NOT exist yet", !ordersIds.contains(101));
    assertTrue("id=102 should exist", ordersIds.contains(102));

    List<Map<String, Object>> allDataTypesRows =
        runIsolatedSQLQuery(
            jdbcResourceManager, testUsername, "SELECT \"id\" FROM \"AllDataTypes\"");
    List<Integer> allDataTypesIds =
        allDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist", allDataTypesIds.contains(1));

    LOG.info("Launching retryDLQ job with schema overrides to process DLQ");
    Map<String, String> retryParams = new HashMap<>();
    retryParams.put("runMode", "retryDLQ");
    retryParams.put(
        "schemaOverridesFilePath", getGcsPath("input/overrides.json", gcsResourceManager));

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
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters("mode=semi-fixed")
                .build(),
            SOURCE_ORACLE,
            retryParams);

    assertThatPipeline(retryJobInfo).isRunning();

    LOG.info("Applying partial fixes in Oracle (inserting missing parent row for Orders)");
    jdbcResourceManager.runSQLUpdate(
        "INSERT INTO \""
            + testUsername
            + "\".\"Customers\" (\"CustomerId\", \"CustomerName\", \"CreditLimit\", \"LegacyRegion\") VALUES (3, 'Parent Customer', 2000, 'Gold')");

    jdbcResourceManager.runSQLUpdate("COMMIT");
    LOG.info("Waiting for the retryDLQ job to complete automatically");
    PipelineOperator.Result retryJobResult =
        pipelineOperator().waitUntilDone(createConfig(retryJobInfo, Duration.ofMinutes(15)));
    assertThatResult(retryJobResult).isLaunchFinished();

    LOG.info("Verifying that severe bucket has exactly 1 entry after retryDLQ job completes");
    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .get());

    LOG.info("Waiting for fixed rows to appear in Oracle");
    PipelineOperator.Result finalWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                JDBCRowsCheck.builder(jdbcResourceManager, "\"" + testUsername + "\".\"Orders\"")
                    .setMinRows(2)
                    .build()
                    .and(
                        JDBCRowsCheck.builder(
                                jdbcResourceManager, "\"" + testUsername + "\".\"AllDataTypes\"")
                            .setMinRows(2)
                            .build()));
    assertThatResult(finalWaitResult).meetsConditions();

    LOG.info("Verifying final target Oracle database contents");

    customersRows =
        runIsolatedSQLQuery(
            jdbcResourceManager, testUsername, "SELECT \"CustomerId\" FROM \"Customers\"");
    customersIds =
        customersRows.stream().map(r -> getIntValueCaseInsensitive(r, "CustomerId")).toList();
    assertTrue("id=1 should NOT exist", !customersIds.contains(1));
    assertTrue("id=2 should exist", customersIds.contains(2));
    assertTrue("id=3 should exist", customersIds.contains(3));

    ordersRows =
        runIsolatedSQLQuery(
            jdbcResourceManager, testUsername, "SELECT \"OrderId\" FROM \"Orders\"");
    ordersIds = ordersRows.stream().map(r -> getIntValueCaseInsensitive(r, "OrderId")).toList();
    assertTrue("id=101 should exist", ordersIds.contains(101));
    assertTrue("id=102 should exist", ordersIds.contains(102));

    allDataTypesRows =
        runIsolatedSQLQuery(jdbcResourceManager, testUsername, "SELECT * FROM \"AllDataTypes\"");
    allDataTypesIds =
        allDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist", allDataTypesIds.contains(1));
    assertTrue("id=999 should exist", allDataTypesIds.contains(999));
    assertTrue("id=888 should NOT exist", !allDataTypesIds.contains(888));

    Map<String, Object> row999 =
        allDataTypesRows.stream()
            .filter(r -> getIntValueCaseInsensitive(r, "id") == 999)
            .findFirst()
            .orElse(null);
    assertTrue("Row with id=999 should be found", row999 != null);

    Map<String, Object> expectedRow999 = createExpectedRow999();
    assertRowMatchesExpected(row999, expectedRow999);

    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineLauncher.cancelJob(PROJECT, REGION, jobInfo.jobId());
  }

  private Integer getIntValueCaseInsensitive(Map<String, Object> map, String key) {
    for (String k : map.keySet()) {
      if (k.equalsIgnoreCase(key) || k.equalsIgnoreCase("\"" + key + "\"")) {
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
            .set("varchar_col")
            .to("test1")
            .build();
    com.google.cloud.spanner.Mutation allTypes999 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(999)
            .set("varchar_col")
            .to("test999")
            .build();
    com.google.cloud.spanner.Mutation allTypes888 =
        com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder("AllDataTypes")
            .set("id")
            .to(888) /* Bad and semi-fixed transformer fail on purpose */
            .set("varchar_col")
            .to("test888")
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

  private Map<String, Object> createExpectedRow999() {
    Map<String, Object> row = new java.util.HashMap<>();
    row.put("id", 999);
    row.put("varchar_col", "test999");
    return row;
  }

  private void assertRowMatchesExpected(
      Map<String, Object> actualRow, Map<String, Object> expectedRow) {
    expectedRow.forEach(
        (key, expectedValue) -> {
          Object actualValue = actualRow.get(key);
          if (actualValue == null && actualRow.containsKey(key.toUpperCase())) {
            actualValue = actualRow.get(key.toUpperCase());
          }

          LOG.info("Field '{}': expectedValue={}, actualValue={}", key, expectedValue, actualValue);

          if (expectedValue == null) {
            assertTrue("Field " + key + " should be null", actualValue == null);
          } else if (expectedValue instanceof byte[] && actualValue instanceof byte[]) {
            assertTrue(
                "Field " + key + " mismatch",
                java.util.Arrays.equals((byte[]) expectedValue, (byte[]) actualValue));
          } else if (expectedValue instanceof Number
              && actualValue instanceof byte[]
              && ((byte[]) actualValue).length == 1) {
            assertTrue(
                "Field " + key + " mismatch",
                ((Number) expectedValue).intValue() == (((byte[]) actualValue)[0] & 0xFF));
          } else if (expectedValue instanceof Number && actualValue instanceof Number) {
            assertTrue(
                "Field " + key + " mismatch",
                Math.abs(
                        ((Number) expectedValue).doubleValue()
                            - ((Number) actualValue).doubleValue())
                    < 0.001);
          } else {
            String exp = expectedValue.toString().replace(" ", "").replace("T", "");
            String act =
                actualValue != null ? actualValue.toString().replace(" ", "").replace("T", "") : "";
            if (act.endsWith(".0")) {
              act = act.substring(0, act.length() - 2);
            }

            boolean isDatePrefix =
                act.length() > exp.length()
                    && act.startsWith(exp)
                    && act.charAt(exp.length()) == '-';
            boolean isTimePrefix =
                exp.length() > act.length()
                    && exp.startsWith(act)
                    && exp.charAt(act.length()) == ':';

            assertTrue(
                "Field " + key + " mismatch: expected " + exp + " but got " + act,
                exp.equals(act) || isDatePrefix || isTimePrefix);
          }
        });
  }
}
