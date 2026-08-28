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

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration smoke test for GCSSpannerDV validating all Oracle data types. */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVOracleSmokeIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVOracleSmokeIT/spanner-schema.sql";
  private static final String AVRO_SCHEMA_RESOURCE =
      "GCSSpannerDVOracleSmokeIT/oracle_all_datatypes.avsc";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @Test
  public void testOracleAllDataTypesValidationSmoke() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            getSchemaFromAvscFile(AVRO_SCHEMA_RESOURCE), "OracleAllDatatypes", Arrays.asList("id"));

    java.time.Instant testTimestamp = java.time.Instant.parse("2024-01-01T10:00:00Z");
    BigDecimal testNumeric = new BigDecimal("1234.567890123");
    byte[] testBytes = new byte[] {0x41, 0x42, 0x43, 0x44};

    // 1. Generate Avro Source Records
    List<GenericRecord> records =
        Arrays.asList(
            // Row 1: Standard Row with all types populated
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("id", 1L)
                .set("varchar2_col", "test_varchar2")
                .set("varchar_col", "test_varchar")
                .set("char_col", "test_char  ")
                .set("character_col", "test_char  ")
                .set("nvarchar2_col", "test_nvarchar2")
                .set("nchar_col", "test_nchar ")
                .set("number_col", testNumeric)
                .set("numeric_col", testNumeric)
                .set("decimal_col", testNumeric)
                .set("dec_col", testNumeric)
                .set("float_col", 123.456)
                .set("double_precision_col", 123.456)
                .set("real_col", 123.456)
                .set("binary_float_col", 123.0f)
                .set("binary_double_col", 123.0)
                .set("integer_col", 12345L)
                .set("int_col", 12345L)
                .set("smallint_col", 123L)
                .set("date_col", testTimestamp)
                .set("timestamp_col", testTimestamp)
                .set("timestamp_tz_col", testTimestamp)
                .set("timestamp_ltz_col", testTimestamp)
                .set("interval_ym_col", "P1Y2M")
                .set("interval_ds_col", "PT3H4M5S")
                .set("raw_col", testBytes)
                .set("blob_col", testBytes)
                .set("clob_col", "test_clob_content")
                .set("nclob_col", "test_nclob_content")
                .set("rowid_col", "AAAB12AADAAAAwPAAA")
                .set("json_col", "{}")
                .set("xmltype_col", "<root><elem>test</elem></root>")
                .build(),
            // Row 2: Null Row
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null).set("id", 2L).build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/oracle_all_datatypes.avro", tableDef.schema, records);

    // 2. Insert Matching Records in Destination (Spanner)
    spannerResourceManager.write(
        Arrays.asList(
            // Row 1
            Mutation.newInsertOrUpdateBuilder("OracleAllDatatypes")
                .set("id")
                .to(1L)
                .set("varchar2_col")
                .to("test_varchar2")
                .set("varchar_col")
                .to("test_varchar")
                .set("char_col")
                .to("test_char  ")
                .set("character_col")
                .to("test_char  ")
                .set("nvarchar2_col")
                .to("test_nvarchar2")
                .set("nchar_col")
                .to("test_nchar ")
                .set("number_col")
                .to(testNumeric)
                .set("numeric_col")
                .to(testNumeric)
                .set("decimal_col")
                .to(testNumeric)
                .set("dec_col")
                .to(testNumeric)
                .set("float_col")
                .to(123.456)
                .set("double_precision_col")
                .to(123.456)
                .set("real_col")
                .to(123.456)
                .set("binary_float_col")
                .to(123.0f)
                .set("binary_double_col")
                .to(123.0)
                .set("integer_col")
                .to(12345L)
                .set("int_col")
                .to(12345L)
                .set("smallint_col")
                .to(123L)
                .set("date_col")
                .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
                .set("timestamp_col")
                .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
                .set("timestamp_tz_col")
                .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
                .set("timestamp_ltz_col")
                .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
                .set("interval_ym_col")
                .to("P1Y2M")
                .set("interval_ds_col")
                .to("PT3H4M5S")
                .set("raw_col")
                .to(ByteArray.copyFrom(testBytes))
                .set("blob_col")
                .to(ByteArray.copyFrom(testBytes))
                .set("clob_col")
                .to("test_clob_content")
                .set("nclob_col")
                .to("test_nclob_content")
                .set("rowid_col")
                .to("AAAB12AADAAAAwPAAA")
                .set("json_col")
                .to(Value.json("{}"))
                .set("xmltype_col")
                .to("<root><elem>test</elem></root>")
                .build(),
            // Row 2
            Mutation.newInsertOrUpdateBuilder("OracleAllDatatypes").set("id").to(2L).build()));

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

    // 3. Launch Pipeline
    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsInputDirectory,
            null,
            null,
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    // 4. Assert Validation Results in BigQuery
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Collections.singletonList(
            new ValidationSummaryDto(
                "MATCH",
                1L, // Total tables validated
                2L, // Total rows matched
                0L, // Total rows mismatched
                "" // Tables with mismatches
                )));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Collections.singletonList(
            new TableValidationStatsDto(
                null, // Schema name
                "OracleAllDatatypes", // Table name
                "MATCH", // Status
                2L, // Source row count
                2L, // Destination row count
                2L, // Matched row count
                0L // Mismatch row count
                )));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager, Collections.emptyList());
  }
}
