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

import static com.google.cloud.teleport.v2.templates.MySQLDataTypesIT.repeatString;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
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
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all data types
 * migration with custom transformations, bulk failure injection, and live retry.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLAllDataTypesCustomTransformationsBulkAndLiveFT extends SourceDbToSpannerFTBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLAllDataTypesCustomTransformationsBulkAndLiveFT.class);

  private static final String MYSQL_DDL_RESOURCE =
      "MySQLAllDataTypesCustomTransformationsBulkAndLiveFT/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "MySQLAllDataTypesCustomTransformationsBulkAndLiveFT/spanner-schema.sql";
  private static final String TABLE_NAME = "AllDataTypes";

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  private static PipelineLauncher.LaunchInfo retryLiveJobInfo;

  public static CloudMySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static String bulkErrorFolderFullPath;

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

    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);

    bulkErrorFolderFullPath = getGcsPath("output", gcsResourceManager);

    // Define Custom Transformation with Exception (Bad)
    CustomTransformation customTransformationBad =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypesWithException")
            .build();

    // launch bulk migration
    bulkJobInfo =
        launchBulkDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            spannerResourceManager,
            gcsResourceManager,
            mySQLResourceManager,
            customTransformationBad);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, mySQLResourceManager, gcsResourceManager);
  }

  @Test
  public void testAllDataTypesCustomTransformationsBulkAndLive() throws IOException {
    // Wait for Bulk migration job to be in running state
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo, Duration.ofMinutes(30)));
    assertThatResult(result).isLaunchFinished();

    // Verify DLQ has 3 events
    ConditionCheck conditionCheck =
        // Check that there is at least 3 errors in DLQ
        DlqEventsCountCheck.builder(gcsResourceManager, "output/dlq/severe/")
            .setMinEvents(4)
            .build();
    assertTrue(conditionCheck.get());

    // Define Custom Transformation without Exception (Good)
    CustomTransformation customTransformationGood =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationAllTypes")
            .build();

    // launch forward migration template in retryDLQ mode
    retryLiveJobInfo =
        launchFwdDataflowJobInRetryDlqMode(
            spannerResourceManager,
            bulkErrorFolderFullPath,
            bulkErrorFolderFullPath + "/dlq",
            gcsResourceManager,
            customTransformationGood);

    // Wait for Spanner to have all 3 rows
    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_NAME)
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(retryLiveJobInfo, Duration.ofMinutes(15)), conditionCheck);
    assertThatResult(result).meetsConditions();

    // Verify Non null Data Content
    List<Map<String, Object>> expectedDataNonNull = getExpectedData();

    List<com.google.cloud.spanner.Struct> allRecords = spannerResourceManager.runQuery("SELECT * FROM " + TABLE_NAME);

    SpannerAsserts.assertThatStructs(allRecords)
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedDataNonNull);

    // Manual assertion for the null row
    verifyNullRow(allRecords);
  }

  private void verifyNullRow(List<com.google.cloud.spanner.Struct> structs) {
    // Iterate over all structs and verify the struct with id=4
    for (com.google.cloud.spanner.Struct struct : structs) {
      if (struct.getLong("id") == 4) {
          // Verify all columns except id are null
          for (com.google.cloud.spanner.Type.StructField field : struct.getType().getStructFields()) {
              if (field.getName().equalsIgnoreCase("id")) {
                  continue;
              }
              assertTrue("Field " + field.getName() + " should be null", struct.isNull(field.getName()));
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
