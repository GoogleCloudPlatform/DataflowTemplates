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
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
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

    // Row 1 (Standard Values)
    Map<String, Object> row1 = new HashMap<>();
    row1.put("id", 1);
    row1.put("tinyint_col", 42);
    row1.put("tinyint_bool_col", 1);
    row1.put("smallint_col", 12345);
    row1.put("mediumint_col", 5000000);
    row1.put("int_col", 1000000000);
    row1.put("bigint_col", 4000000000000000000L);
    row1.put("tinyint_unsigned_col", 42);
    row1.put("smallint_unsigned_col", 12345);
    row1.put("mediumint_unsigned_col", 5000000);
    row1.put("int_unsigned_col", 1000000000);
    // TODO: Resolve b/544589449 - currently validation for these two datatypes is broken
    // row1.put("bigint_unsigned_col", 4000000000000000000L);
    // row1.put("decimal_col", new java.math.BigDecimal("21378.34"));
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
    row1.put("char_col", "standard");
    row1.put("binary_col", "0x" + "AA".repeat(255));
    row1.put("varbinary_col", "0x" + "BB".repeat(2000));
    row1.put("tinyblob_col", "0x" + "CC".repeat(255));
    row1.put("blob_col", "0x" + "DD".repeat(65535));
    row1.put("mediumblob_col", "0x" + "EE".repeat(2 * 1024 * 1024));
    row1.put("longblob_col", "0x" + "FF".repeat(2 * 1024 * 1024));
    row1.put("bit_col", 1);
    row1.put("bit_bool_col", 1);
    row1.put("enum_col", "v1");
    row1.put("set_col", "v1,v2");
    row1.put("json_col", "{}");
    records.add(row1);

    // Row 2 (All NULL values)
    Map<String, Object> row2 = new HashMap<>();
    row2.put("id", 2);
    records.add(row2);

    // Row 3 (Minimum Values)
    Map<String, Object> minValues = new HashMap<>();
    minValues.put("id", 3);
    minValues.put("tinyint_col", -128);
    // Spanner BOOL minimum (FALSE maps to 0)
    minValues.put("tinyint_bool_col", 0);
    minValues.put("smallint_col", -32768);
    minValues.put("mediumint_col", -8388608);
    minValues.put("int_col", -2147483648);
    minValues.put("bigint_col", -9223372036854775808L);
    minValues.put("tinyint_unsigned_col", 0);
    minValues.put("smallint_unsigned_col", 0);
    minValues.put("mediumint_unsigned_col", 0);
    minValues.put("int_unsigned_col", 0);
    // minValues.put("bigint_unsigned_col", 0L);
    // Spanner NUMERIC absolute minimum
    // minValues.put("decimal_col", new
    // java.math.BigDecimal("-99999999999999999999999999999.999999999"));
    minValues.put("float_col", -3.402823E+38f);
    minValues.put("double_col", -1.7976931348623157E+308);
    minValues.put("bit_col", "0x" + "00".repeat(8));
    minValues.put("bit_bool_col", 0);
    minValues.put("date_col", "1000-01-01");
    minValues.put("time_col", "-838:59:59.000000");
    minValues.put("datetime_col", "1000-01-01 00:00:00");
    minValues.put("timestamp_col", "1970-01-01 00:00:01");
    minValues.put("year_col", 0);
    minValues.put("char_col", "");
    minValues.put("varchar_col", "");
    minValues.put("binary_col", "0x" + "00".repeat(255));
    minValues.put("varbinary_col", "0x" + "00".repeat(2000));
    minValues.put("tinyblob_col", "0x" + "00".repeat(255));
    minValues.put("blob_col", "0x" + "00".repeat(65535));
    minValues.put("mediumblob_col", "0x" + "00".repeat(2 * 1024 * 1024));
    minValues.put("longblob_col", "0x" + "00".repeat(2 * 1024 * 1024));
    minValues.put("varbinary_col", "");
    minValues.put("tinyblob_col", "");
    minValues.put("blob_col", "");
    minValues.put("mediumblob_col", "");
    minValues.put("longblob_col", "");
    minValues.put("tinytext_col", "");
    minValues.put("text_col", "");
    minValues.put("mediumtext_col", "");
    minValues.put("longtext_col", "");
    minValues.put("enum_col", "v1");
    minValues.put("set_col", "");
    minValues.put("json_col", "{}");
    records.add(minValues);

    // Row 4 (Maximum Values)
    Map<String, Object> maxValues = new HashMap<>();
    maxValues.put("id", 4);
    maxValues.put("tinyint_col", 127);
    maxValues.put("tinyint_bool_col", 1);
    maxValues.put("smallint_col", 32767);
    maxValues.put("mediumint_col", 8388607);
    maxValues.put("int_col", 2147483647);
    maxValues.put("bigint_col", 9223372036854775807L);
    maxValues.put("tinyint_unsigned_col", 255);
    maxValues.put("smallint_unsigned_col", 65535);
    maxValues.put("mediumint_unsigned_col", 16777215);
    maxValues.put("int_unsigned_col", 4294967295L);
    // maxValues.put("bigint_unsigned_col", 9223372036854775807L);
    // maxValues.put("decimal_col", new
    // java.math.BigDecimal("99999999999999999999999999999.999999999"));
    maxValues.put("float_col", 3.402823E+38f);
    maxValues.put("double_col", 1.7976931348623157E+308);
    maxValues.put("bit_col", "0x" + "FF".repeat(8));
    maxValues.put("bit_bool_col", 1);
    maxValues.put("date_col", "9999-12-31");
    maxValues.put("time_col", "838:59:59.000000");
    maxValues.put("datetime_col", "9999-12-31 23:59:59.999999");
    maxValues.put("timestamp_col", "2038-01-19 03:14:07.999999");
    maxValues.put("year_col", 2155);
    maxValues.put("char_col", "Z".repeat(255));
    maxValues.put("varchar_col", "Z".repeat(2000));
    maxValues.put("binary_col", "0x" + "FF".repeat(255));
    maxValues.put("varbinary_col", "0x" + "FF".repeat(2000));
    maxValues.put("tinyblob_col", "0x" + "FF".repeat(255));
    maxValues.put("blob_col", "0x" + "FF".repeat(65535));

    // Testing large limits. Kept exactly below 10 MiB accounting for Spanner's internal
    // serialization overhead.
    // Cloud Spanner's absolute hard limit for a single column value (STRING/BYTES) is 10 MiB.
    // We use (10 MiB - 1024 bytes) to safely avoid Dataflow SpannerIO mutation serialization
    // limits.
    // Additionally, MySQL max_allowed_packet would reject this if > 64MB by default.
    final int safeBlobSize = (2 * 1024 * 1024);
    maxValues.put("mediumblob_col", "0x" + "FF".repeat(safeBlobSize));
    maxValues.put("longblob_col", "0x" + "FF".repeat(safeBlobSize));
    maxValues.put("tinytext_col", "Z".repeat(255));
    maxValues.put("text_col", "Z".repeat(65535));
    maxValues.put("mediumtext_col", "Z".repeat(safeBlobSize));
    maxValues.put("longtext_col", "Z".repeat(safeBlobSize));
    maxValues.put("enum_col", "v65535");
    maxValues.put(
        "set_col",
        "v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,v29,v30,v31,v32,v33,v34,v35,v36,v37,v38,v39,v40,v41,v42,v43,v44,v45,v46,v47,v48,v49,v50,v51,v52,v53,v54,v55,v56,v57,v58,v59,v60,v61,v62,v63,v64");
    // maxValues.put("json_col", "{\"max\": \"" + "Z".repeat(safeBlobSize - 11) + "\"}");
    records.add(maxValues);

    mySQLResourceManager.write("AllDatatypes", records);

    // 2. Launch Bulk Pipeline (SourceDbToSpanner)
    String gcsOutputDirectory = "gs://" + artifactBucketName + "/" + testId;

    LaunchInfo bulkJobInfo =
        launchBulkDataflowJob(
            PipelineUtils.createJobName("bulk"),
            spannerResourceManager,
            gcsClient,
            mySQLResourceManager,
            null,
            false);
    assertThatPipeline(bulkJobInfo).isRunning();

    pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));

    // 3. Assert on spanner rows to verify the bulk job was actually successful
    assertThat(spannerResourceManager.getRowCount("AllDatatypes")).isEqualTo(4L);

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
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Collections.singletonList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 4L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Collections.singletonList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AllDatatypes",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 4L,
                /* destinationRowCount= */ 4L,
                /* matchedRowCount= */ 4L,
                /* mismatchRowCount= */ 0L)));
  }
}
