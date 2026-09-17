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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
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

/**
 * Integration tests validating handling of the Spanner maximum column name length limit (128
 * characters).
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMaxColumnNameIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVWideRowMaxColumnNameIT/spanner-schema.sql";
  private static final String AVRO_SCHEMA_RESOURCE =
      "GCSSpannerDVWideRowMaxColumnNameIT/MaxColumnName.avsc";
  private static final String MAX_COLUMN_NAME = String.join("", Collections.nCopies(128, "C"));

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  // Validates a column named with 128 characters (MATCH) and a corresponding MISMATCH.
  @Test
  public void testMaxColumnNameLength() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef maxColTableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            GCSSpannerDVAvroSetupHelper.getSchemaFromAvscFile(AVRO_SCHEMA_RESOURCE),
            "MaxColumnNameTable",
            Arrays.asList("id"));

    List<GenericRecord> records =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(maxColTableDef, null)
                .set("id", 1L)
                .set(MAX_COLUMN_NAME, "MatchTest")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(maxColTableDef, null)
                .set("id", 2L)
                .set(MAX_COLUMN_NAME, "AvroVal")
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/maxcolumnname.avro", maxColTableDef.schema, records);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("MaxColumnNameTable")
                .set("id")
                .to(1L)
                .set(MAX_COLUMN_NAME)
                .to("MatchTest")
                .build(),
            Mutation.newInsertOrUpdateBuilder("MaxColumnNameTable")
                .set("id")
                .to(2L)
                .set(MAX_COLUMN_NAME)
                .to("SpannerVal")
                .build()));

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

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

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 1L,
                /* totalRowsMismatched= */ 2L,
                /* tablesWithMismatches= */ "MaxColumnNameTable")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "MaxColumnNameTable",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    // Differing row values are emitted as two discrepancies: one MISSING_IN_SOURCE and one
    // MISSING_IN_DESTINATION.
    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                null, null, "MaxColumnNameTable", "[id:2]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "MaxColumnNameTable", "[id:2]", "MISSING_IN_SOURCE")));
  }
}
