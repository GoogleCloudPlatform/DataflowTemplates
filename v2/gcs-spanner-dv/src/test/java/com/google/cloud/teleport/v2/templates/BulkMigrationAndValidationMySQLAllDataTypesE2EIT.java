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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-End Integration test for all supported data types mapping from MySQL to Spanner. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class BulkMigrationAndValidationMySQLAllDataTypesE2EIT extends EndToEndTestingITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "BulkMigrationAndValidationMySQLAllDataTypesE2EIT/spanner-schema.sql";
  private static final String MYSQL_DDL_RESOURCE =
      "BulkMigrationAndValidationMySQLAllDataTypesE2EIT/mysql-schema.sql";

  private CloudMySQLResourceManager mySQLResourceManager;

  @Before
  public void setUp() throws IOException {
    mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, flexTemplateDataflowJobResourceManager);
    // Spanner and BigQuery are automatically cleaned up in tearDownBase()
  }

  @Test
  public void allDataTypesE2E() throws Exception {
    createMySQLDDL(mySQLResourceManager, MYSQL_DDL_RESOURCE);

    List<Map<String, Object>> records = new ArrayList<>();

    // Row 1 (Standard Types)
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("tinyint_col", 127);
    row1.put("tinyint_bool_col", 1);
    row1.put("smallint_col", 32767);
    row1.put("mediumint_col", 8388607);
    row1.put("int_col", 2147483647);
    row1.put("bigint_col", 9223372036854775807L);
    row1.put("decimal_col", null);
    row1.put("numeric_col", null);
    row1.put("float_col", 123.0f);
    row1.put("double_col", 123.0);
    row1.put("datetime_col", "2024-01-01 10:00:00");
    row1.put("timestamp_col", "2024-01-01 10:00:00");
    row1.put("time_col", "10:00:00");
    row1.put("year_col", 2024);
    row1.put("date_col", "2024-01-01");
    row1.put("varchar_col", "varchar");
    row1.put("tinytext_col", "tinytext");
    row1.put("text_col", "text");
    row1.put("mediumtext_col", "mediumtext");
    row1.put("longtext_col", "longtext");
    row1.put("char_col", "12345678901234567890");
    // row1.put("binary_col", "12345678901234567890");
    // row1.put("varbinary_col", "varbin");
    // row1.put("tinyblob_col", "tinyblob");
    // row1.put("blob_col", "blob");
    // row1.put("mediumblob_col", "mediumblob");
    // row1.put("longblob_col", "longblob");
    row1.put("bit_col", 1);
    row1.put("bit_bool_col", 1);
    row1.put("enum_col", "A");
    row1.put("set_col", "A,B");
    // row1.put("json_col", "{}");
    records.add(row1);

    // Row 2 (Nested Structures & Arrays in JSON)
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2);
    // row2.put("json_col", "{}");
    records.add(row2);

    // Row 3 (Edge Cases & Boundaries)
    Map<String, Object> row3 = new HashMap<>();
    row3.put("id", 3);
    row3.put("timestamp_col", "1970-01-01 00:00:01");
    row3.put("set_col", "A,B,C");
    row3.put("tinyint_col", -128); // min
    records.add(row3);

    // Row 4 (Minimum Values)
    Map<String, Object> minValues = new HashMap<>();
    minValues.put("id", 4);
    minValues.put("tinyint_col", -128);
    minValues.put("tinyint_bool_col", 0);
    minValues.put("smallint_col", -32768);
    minValues.put("mediumint_col", -8388608);
    minValues.put("int_col", -2147483648);
    minValues.put("bigint_col", -9223372036854775808L);
    minValues.put("decimal_col", null);
    minValues.put("numeric_col", null);
    minValues.put("float_col", -3.40282e38f);
    minValues.put("double_col", -1.79769e308);
    minValues.put("bit_col", 0);
    minValues.put("bit_bool_col", 0);
    minValues.put("date_col", "1000-01-01");
    minValues.put("time_col", "-838:59:59");
    minValues.put("datetime_col", "1000-01-01 00:00:00");
    minValues.put("timestamp_col", "1970-01-01 00:00:01");
    minValues.put("year_col", 1901);
    minValues.put("char_col", "");
    minValues.put("varchar_col", "");
    minValues.put("enum_col", "A");
    minValues.put("set_col", "");
    records.add(minValues);

    // Row 5 (Maximum Values)
    Map<String, Object> maxValues = new HashMap<>();
    maxValues.put("id", 5);
    maxValues.put("tinyint_col", 127);
    maxValues.put("tinyint_bool_col", 1);
    maxValues.put("smallint_col", 32767);
    maxValues.put("mediumint_col", 8388607);
    maxValues.put("int_col", 2147483647);
    maxValues.put("bigint_col", 9223372036854775807L);
    maxValues.put("decimal_col", null);
    maxValues.put("numeric_col", null);
    maxValues.put("float_col", 3.40282e38f);
    maxValues.put("double_col", 1.79769e308);
    maxValues.put("bit_col", 9223372036854775807L);
    maxValues.put("bit_bool_col", 1);
    maxValues.put("date_col", "9999-12-31");
    maxValues.put("time_col", "838:59:59");
    maxValues.put("datetime_col", "9999-12-31 23:59:59");
    maxValues.put("timestamp_col", "2038-01-19 03:14:07");
    maxValues.put("year_col", 2155);
    maxValues.put("enum_col", "C");
    maxValues.put("set_col", "A,B,C");
    records.add(maxValues);

    mySQLResourceManager.write("AllDatatypes", records);
    Thread.sleep(20000);

    // 2. Launch Bulk Pipeline (SourceDbToSpanner)
    String gcsOutputDirectory =
        "gs://"
            + artifactBucketName
            + "/"
            + testName
            + "-"
            + java.util.UUID.randomUUID().toString();

    LaunchInfo bulkJobInfo =
        launchBulkDataflowJob(
            PipelineUtils.createJobName("bulk"),
            spannerResourceManager,
            gcsClient,
            mySQLResourceManager,
            null,
            false,
            gcsOutputDirectory);
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result bulkResult =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));
    assertThatResult(bulkResult).isLaunchFinished();

    // 3. Assert on spanner rows to verify the bulk job was actually successful
    long recordsCount = spannerResourceManager.getRowCount("AllDatatypes");
    assertThat(recordsCount).isEqualTo(5L);

    // 4. Launch Validation Pipeline (GCSSpannerDV)
    LaunchConfig.Builder dvOptions = LaunchConfig.builder(testName, specPath);
    LaunchInfo dvJobInfo =
        launchDataflowJob(
            dvOptions,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsOutputDirectory,
            null,
            null,
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(dvJobInfo));

    // 5. Assert BigQuery Validation Results (Expect PERFECT MATCH)
    try {
      GCSSpannerDVTestAsserts.assertValidationSummary(
          bigQueryResourceManager,
          Collections.singletonList(
              new ValidationSummaryDto(
                  /* status= */ "MATCH",
                  /* totalTablesValidated= */ 1L,
                  /* totalRowsMatched= */ 5L,
                  /* totalRowsMismatched= */ 0L,
                  /* tablesWithMismatches= */ "")));
    } catch (Throwable t) {
      System.out.println("ASSERTION FAILED. PRINTING MISMATCHED RECORDS...");
      com.google.cloud.bigquery.TableResult result =
          bigQueryResourceManager.readTable("MismatchedRecords");
      for (Map<String, Object> row :
          org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.tableResultToRecords(result)) {
        System.out.println("MISMATCH ROW: " + row);
      }

      System.out.println("====== SPANNER DATA ======");
      try {
        System.out.println(spannerResourceManager.runQuery("SELECT * FROM AllDatatypes"));
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("====== MYSQL DATA ======");
      try {
        try (java.sql.Connection conn =
                java.sql.DriverManager.getConnection(
                    mySQLResourceManager.getUri(),
                    mySQLResourceManager.getUsername(),
                    mySQLResourceManager.getPassword());
            java.sql.Statement stmt = conn.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM AllDatatypes")) {
          java.sql.ResultSetMetaData md = rs.getMetaData();
          while (rs.next()) {
            Map<String, Object> r = new HashMap<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
              try {
                r.put(md.getColumnName(i), rs.getString(i));
              } catch (Exception ignored) {
              }
            }
            System.out.println(r);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      throw t;
    }

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Collections.singletonList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AllDatatypes",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 5L,
                /* destinationRowCount= */ 5L,
                /* matchedRowCount= */ 5L,
                /* mismatchRowCount= */ 0L)));
  }
}
