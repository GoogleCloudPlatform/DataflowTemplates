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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.BOOKS_TABLE;
import static com.google.cloud.teleport.v2.templates.MySQLDataTypesIT.repeatString;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider;
import com.google.cloud.teleport.v2.templates.failureinjectiontesting.SourceDbToSpannerFTBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCountCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
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
 * the template behaviour. This test tests all data types migration with custom transformations,
 * bulk failure injection, and live retry. The Bulk failures are injected both at Custom
 * transformation phase and Spanner write phase. The test case also includes an interleaved table.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLAllDataTypesBulkAndLiveIT extends SourceDbToSpannerFTBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLAllDataTypesBulkAndLiveIT.class);

  private static final String MYSQL_DDL_RESOURCE =
      "MySQLAllDataTypesBulkAndLiveIT/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "MySQLAllDataTypesBulkAndLiveIT/spanner-schema.sql";

  private static final String TABLE_CT = "AllDataTypes_CT"; // Custom transformation failures
  private static final String TABLE_SWF = "AllDataTypes_SWF"; // Spanner write failures

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  private static PipelineLauncher.LaunchInfo liveJobInfo;

  public static CloudMySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws Exception {

    // create and upload GCS Resources
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();

    // Upload Custom Transformation Jar
    createAndUploadJarToGcs("CustomTransformationAllTypes", gcsResourceManager);

    // create Spanner Resources
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

    // create MySQL Resources
    mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();

    // Load DDL for AllDataTypes tables and Authors/Books
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);

    // Insert data for Authors and Books
    MySQLSrcDataProvider.writeRowsInSourceDB(1, 200, mySQLResourceManager);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testAllScenarios() throws IOException, InterruptedException {
    // --------------------------------------------------------------------------------------------
    // Phase 1: Bulk Migration
    // --------------------------------------------------------------------------------------------

    // Launch Bulk Job for All Scenarios
    // We use CustomTransformationAllTypesWithException which is selective:
    // - AllDataTypes_CT table: Fails (Simulated) - Custom transformation class throws exception
    // - AllDataTypes_SWF table: Passes transformation, Fails at Spanner Write (Schema mismatch) -
    // length of bit_col is too small
    // - Authors/Books table: Passes transformation, Fails at Spanner Write (Schema mismatch for
    // Authors) - length of name column in authors table is too small. It is such that 9 rows
    // `author_1` to `author_9` gets inserted and the rest 191 rows fail. Similarly, 9 books rows
    // corresponding to authors would get inserted and rest 191 would fail.

    CustomTransformation customTransformationBad =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypesWithException")
            .build();

    Map<String, String> params = new HashMap<>();
    // No 'tables' parameter needed, migrate everything
    params.put("outputDirectory", getGcsPath("output", gcsResourceManager));

    bulkJobInfo =
        launchBulkDataflowJob(
            getClass().getSimpleName() + "-Bulk",
            null,
            params,
            spannerResourceManager,
            gcsResourceManager,
            mySQLResourceManager,
            customTransformationBad);

    // Wait for bulk job
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo, Duration.ofMinutes(30)));
    assertThatResult(result).isLaunchFinished();

    // Wait for 2 minutes to ensure all DLQ events are created in GCS.
    Thread.sleep(Duration.ofMinutes(2).toMillis());

    // Verify DLQ Events
    // Total events expected:
    // - CT: 4 events (3 rows + 1 null row)
    // - SWF: 3 events (3 rows, bit_col too small)
    // - Authors: 191 events (name column too small)
    // - Books: 191 events (parent Authors failed)
    // Total: 4 + 3 + 191 + 191 = 389
    assertTrue(
        DlqEventsCountCheck.builder(gcsResourceManager, "output/dlq/severe/")
            .setMinEvents(389)
            .build()
            .get());

    // --------------------------------------------------------------------------------------------
    // Phase 2: Live Migration (Retry)
    // --------------------------------------------------------------------------------------------

    // Fix schemas before retry
    spannerResourceManager.executeDdlStatement(
        "ALTER TABLE `" + TABLE_SWF + "` ALTER COLUMN `bit_col` BYTES(MAX)");
    spannerResourceManager.executeDdlStatement(
        "ALTER TABLE `Authors` ALTER COLUMN `name` STRING(200)");

    // Retry with Good Custom Transformation class
    CustomTransformation customTransformationGood =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypes")
            .build();

    liveJobInfo =
        launchFwdDataflowJobInRetryDlqMode(
            spannerResourceManager,
            getGcsPath("output", gcsResourceManager),
            getGcsPath("output/dlq", gcsResourceManager),
            gcsResourceManager,
            customTransformationGood);

    // Wait and Verify All Tables
    ConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_CT)
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_SWF)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, AUTHORS_TABLE)
                        .setMinRows(200)
                        .setMaxRows(200)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, BOOKS_TABLE)
                        .setMinRows(200)
                        .setMaxRows(200)
                        .build()))
            .build();

    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(liveJobInfo, Duration.ofMinutes(15)), conditionCheck))
        .meetsConditions();

    // Verify CT Data
    List<Map<String, Object>> expectedDataNonNull = getExpectedData();
    List<com.google.cloud.spanner.Struct> allRecordsCT =
        spannerResourceManager.runQuery("SELECT * FROM " + TABLE_CT);
    SpannerAsserts.assertThatStructs(allRecordsCT)
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedDataNonNull);
    verifyNullRow(allRecordsCT);

    // Verify SWF Data
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("SELECT * FROM " + TABLE_SWF))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedDataNonNull);
  }

  private void verifyNullRow(List<com.google.cloud.spanner.Struct> structs) {
    for (com.google.cloud.spanner.Struct struct : structs) {
      if (struct.getLong("id") == 4) {
        for (com.google.cloud.spanner.Type.StructField field : struct.getType().getStructFields()) {
          if (field.getName().equalsIgnoreCase("id")) {
            continue;
          }
          assertTrue(
              "Field " + field.getName() + " should be null", struct.isNull(field.getName()));
        }
        break;
      }
    }
  }

  private List<Map<String, Object>> getExpectedData() {
    List<Map<String, Object>> data = new ArrayList<>();
    data.add(createExpectedRow(1, "valid1"));
    data.add(createExpectedRow(2, "valid2"));
    data.add(createExpectedRow(3, "fail_me"));
    return data;
  }

  private Map<String, Object> createExpectedRow(int id, String varcharVal) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("varchar_col", varcharVal);
    row.put("tinyint_col", id);
    row.put("tinyint_unsigned_col", id);
    row.put("text_col", "text" + id);
    row.put("date_col", "2023-01-0" + id);
    row.put("smallint_col", 10 + id);
    row.put("smallint_unsigned_col", 10 + id);
    row.put("mediumint_col", 100 + id);
    row.put("mediumint_unsigned_col", 100 + id);
    row.put("bigint_col", 1000L + id);
    row.put("bigint_unsigned_col", 1000L + id);
    row.put("float_col", (double) (1.5f + id));
    row.put("double_col", 2.5d + id);
    row.put("decimal_col", new java.math.BigDecimal(10.5 + id));
    row.put("datetime_col", "2023-01-0" + id + "T12:00:00Z");
    row.put("time_col", "12:00:0" + id);
    row.put("year_col", String.valueOf(2023 + id));
    row.put("char_col", "c" + id);

    String b64blob = java.util.Base64.getEncoder().encodeToString(("blob" + id).getBytes());
    row.put("tinyblob_col", b64blob);
    row.put("blob_col", b64blob);
    row.put(
        "mediumblob_col",
        java.util.Base64.getEncoder().encodeToString(("mediumblob" + id).getBytes()));

    row.put("tinytext_col", "tinytext" + id);
    row.put("mediumtext_col", "mediumtext" + id);
    row.put("test_json_col", "{\"k\":\"v" + id + "\"}");
    row.put(
        "longblob_col", java.util.Base64.getEncoder().encodeToString(("longblob" + id).getBytes()));
    row.put("longtext_col", "longtext" + id);
    row.put("enum_col", "1");
    row.put("bool_col", true);

    row.put("binary_col", "eDU4MD" + repeatString("A", 334));
    row.put(
        "varbinary_col", java.util.Base64.getEncoder().encodeToString(("varbin" + id).getBytes()));

    row.put("bit_col", "f/////////8=");

    row.put("bit8_col", "255");

    row.put("bit1_col", true);
    row.put("boolean_col", true);
    row.put("int_col", 1000 + id);
    row.put("integer_unsigned_col", 1000 + id);
    row.put("timestamp_col", "2023-01-0" + id + "T12:00:00Z");
    row.put("set_col", "v1");

    return row;
  }
}
