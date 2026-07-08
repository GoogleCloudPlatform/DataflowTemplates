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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
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
 * <p>Edge cases covered in this test include:
 *
 * <ul>
 *   <li>Handling retriable errors such as check constraint and foreign key violations via the
 *       retryAllDLQ pipeline.
 *   <li>Processing severe errors introduced by custom transformation failures via the retryAllDLQ
 *       pipeline.
 *   <li>Retrying fixed items successfully in both retry/ and severe/ buckets: e.g. fixing a foreign
 *       key violation by inserting a missing parent row, and using a corrected transformation file.
 *   <li>Ensuring non-fixable items remain correctly logged under their respective error buckets.
 *   <li>Validating schema complexities between Source and Spanner, including mismatched primary
 *       keys, added, deleted, and renamed columns, as well as all datatypes.
 *   <li>Utilizing the schema overrides file to reconcile schema differences.
 *   <li>Utilizing the static DLQ file-based consumer (instead of the Pub/Sub consumer flow).
 * </ul>
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
                    "com.custom.CustomTransformationForDLQIT")
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

    // 2. Insert all test data into the source Spanner database. This will generate:
    // - 2 severe errors (for id=999 and id=888) due to the custom transformation throwing exception
    // in "bad" mode.
    // - 1 retryable error (for order101) due to missing parent customer (FK violation: Customer 3
    // does not exist).
    // - 1 retryable error (for customer1) due to check constraint violation (CreditLimit is 500,
    // must be > 1000).
    insertDataInSpanner();
    LOG.info("Data inserted into Spanner successfully");

    // 3. Wait for DLQ events to appear in corresponding buckets.
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets");
    PipelineOperator.Result dlqWaitResult =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
                    .setMinEvents(2)
                    .build()
                    .and(
                        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                            .setMinEvents(2)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManager, "Orders")
                            .setMinRows(1) // id=102
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManager, "AllDataTypes")
                            .setMinRows(1) // id=1
                            .setMaxRows(1)
                            .build())
                    .and(
                        JDBCRowsCheck.builder(jdbcResourceManager, "Customers")
                            .setMinRows(1) // id=2
                            .setMaxRows(1)
                            .build()));
    assertThatResult(dlqWaitResult).meetsConditions();
    LOG.info("DLQ events successfully generated in corresponding buckets");

    // 4. Stop the regular pipeline. The retry pipeline must run independently.
    LOG.info("Stopping the regular pipeline: {}", jobInfo.jobId());
    pipelineOperator().cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(15)));
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
                    "input/customShard.jar", "com.custom.CustomTransformationForDLQIT")
                .setCustomParameters(
                    "mode=semi-fixed") // This fixes one of our severe errors simulated in the
                // transformer
                .build(),
            MYSQL_SOURCE_TYPE,
            retryParams);
    LOG.info("RetryAllDLQ job launched: {}", retryJobInfo.jobId());

    assertThatPipeline(retryJobInfo).isRunning();

    // 7. Wait for the retry job to process events and ensure they reach the DLQ correctly BEFORE
    // cancelling
    // The buckets should now have exactly 1 entry each (for Customers 1 in retry and id=888 in
    // severe).
    // The other entries were fixed:
    // - Orders 101 FK violation fixed by inserting missing parent row.
    // - AllDataTypes 999 severe error fixed by updating custom transformation to mode="semi-fixed".
    // Remaining rows:
    // - Customers 1 remains in retry because the check constraint violation was not fixed.
    // - AllDataTypes 888 remains in severe because mode='semi-fixed' only fixes row 999, not 888.
    LOG.info("Waiting for DLQ events to appear in retry and severe buckets after retry");
    ConditionCheck dlqConditionCheck =
        DlqEventsCountCheck.builder(gcsResourceManager, "dlq/retry/")
            .setMinEvents(1)
            .setMaxEvents(1)
            .build()
            .and(
                DlqEventsCountCheck.builder(gcsResourceManager, "dlq/severe/")
                    .setMinEvents(1)
                    .setMaxEvents(1)
                    .build())
            .and(
                JDBCRowsCheck.builder(jdbcResourceManager, "Orders")
                    .setMinRows(2) // id = 102 and 101
                    .build())
            .and(
                JDBCRowsCheck.builder(jdbcResourceManager, "AllDataTypes")
                    .setMinRows(2) // id = 1 and 999
                    .build());

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
        jdbcResourceManager.runSQLQuery("SELECT * FROM AllDataTypes");
    List<Integer> allDataTypesIds =
        allDataTypesRows.stream().map(r -> getIntValueCaseInsensitive(r, "id")).toList();
    assertTrue("id=1 should exist", allDataTypesIds.contains(1));
    assertTrue("id=999 should exist", allDataTypesIds.contains(999));
    assertTrue("id=888 should NOT exist", !allDataTypesIds.contains(888));

    // Assert contents of AllDataTypes rows to ensure data integrity
    Map<String, Object> row999 =
        allDataTypesRows.stream()
            .filter(r -> getIntValueCaseInsensitive(r, "id") == 999)
            .findFirst()
            .orElse(null);
    assertTrue("Row with id=999 should be found", row999 != null);
    Map<String, Object> expectedRow999 = createExpectedRow999();
    assertRowMatchesExpected(row999, expectedRow999);

    // Customers:
    // id=2 should exist (inserted directly)
    // id=3 should exist (inserted as a partial fix)
    // id=1 should NOT exist (check constraint violation wasn't fixed: CreditLimit was 500 but must
    // be > 1000)
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
            .to(999)
            .set("varchar_col")
            .to("test999")
            .set("tinyint_col")
            .to(9)
            .set("tinyint_unsigned_col")
            .to(9)
            .set("text_col")
            .to("text999")
            .set("date_col")
            .to(com.google.cloud.Date.parseDate("2023-01-09"))
            .set("smallint_col")
            .to(99)
            .set("smallint_unsigned_col")
            .to(99)
            .set("mediumint_col")
            .to(999)
            .set("mediumint_unsigned_col")
            .to(999)
            .set("bigint_col")
            .to(9999L)
            .set("bigint_unsigned_col")
            .to(new java.math.BigDecimal("9999"))
            .set("float_col")
            .to(9.9f)
            .set("double_col")
            .to(99.9d)
            .set("decimal_col")
            .to(new java.math.BigDecimal("99.9"))
            .set("datetime_col")
            .to(com.google.cloud.Timestamp.parseTimestamp("2023-01-09T12:00:00Z"))
            .set("time_col")
            .to("12:00:09")
            .set("year_col")
            .to("2023")
            .set("char_col")
            .to("c")
            .set("tinyblob_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("blob9".getBytes())))
            .set("tinytext_col")
            .to("tinytext9")
            .set("blob_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("blob9".getBytes())))
            .set("mediumblob_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("mediumblob9".getBytes())))
            .set("mediumtext_col")
            .to("mediumtext9")
            .set("test_json_col")
            .to(com.google.cloud.spanner.Value.json("{\"k\":\"v9\"}"))
            .set("longblob_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("longblob9".getBytes())))
            .set("longtext_col")
            .to("longtext9")
            .set("enum_col")
            .to("1")
            .set("bool_col")
            .to(true)
            .set("binary_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("bin".getBytes())))
            .set("varbinary_col")
            .to(
                com.google.cloud.ByteArray.fromBase64(
                    java.util.Base64.getEncoder().encodeToString("varbin".getBytes())))
            .set("bit_col")
            .to(com.google.cloud.ByteArray.copyFrom(new byte[] {(byte) 1}))
            .set("bit8_col")
            .to(255)
            .set("bit1_col")
            .to(true)
            .set("boolean_col")
            .to(false)
            .set("int_col")
            .to(9999)
            .set("integer_unsigned_col")
            .to(9999)
            .set("timestamp_col")
            .to(com.google.cloud.Timestamp.parseTimestamp("2023-01-09T12:00:00Z"))
            .set("set_col")
            .to("v1")
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

  private Map<String, Object> createExpectedRow999() {
    Map<String, Object> row = new java.util.HashMap<>();
    row.put("id", 999);
    row.put("varchar_col", "test999");
    row.put("tinyint_col", 9);
    row.put("text_col", "text999");
    row.put("date_col", "2023-01-09"); // Expected as String
    row.put("smallint_col", 99);
    row.put("mediumint_col", 999);
    row.put("bigint_col", 9999L);
    row.put("decimal_col", new java.math.BigDecimal("99.9"));
    row.put("float_col", 9.9f);
    row.put("double_col", 99.9d);
    row.put("char_col", "c");
    row.put("tinytext_col", "tinytext9");
    row.put("mediumtext_col", "mediumtext9");
    row.put("longtext_col", "longtext9");
    row.put("enum_col", "1");
    row.put("bool_col", true);
    row.put("boolean_col", false);
    row.put("int_col", 9999);
    row.put("set_col", "v1");
    row.put("tinyblob_col", "blob9".getBytes());
    row.put("blob_col", "blob9".getBytes());
    row.put("mediumblob_col", "mediumblob9".getBytes());
    row.put("longblob_col", "longblob9".getBytes());
    row.put(
        "binary_col",
        java.util.Arrays.copyOf("bin".getBytes(), 255)); // BINARY(255) is right-padded with zeros
    row.put("varbinary_col", "varbin".getBytes());
    row.put("bit_col", new byte[] {0, 0, 0, 0, 0, 0, 0, 1}); // BIT(64) returns 8 bytes
    row.put("bit8_col", 255); // BIT(8) expected as Integer 255
    row.put("bit1_col", true);
    row.put("tinyint_unsigned_col", 9);
    row.put("smallint_unsigned_col", 99);
    row.put("mediumint_unsigned_col", 999);
    row.put("bigint_unsigned_col", 9999L);
    row.put("integer_unsigned_col", 9999);
    row.put(
        "datetime_col",
        "2023-01-09 12:00:00"); // Expected as String, handles missing seconds in actual
    row.put("time_col", "12:00:09");
    row.put("year_col", "2023"); // Expected as String, handles Date return in actual
    row.put("test_json_col", "{\"k\":\"v9\"}"); // JSON expected as String
    row.put("timestamp_col", "2023-01-09 12:00:00");

    return row;
  }

  private void assertRowMatchesExpected(
      Map<String, Object> actualRow, Map<String, Object> expectedRow) {
    expectedRow.forEach(
        (key, expectedValue) -> {
          Object actualValue = actualRow.get(key);

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
