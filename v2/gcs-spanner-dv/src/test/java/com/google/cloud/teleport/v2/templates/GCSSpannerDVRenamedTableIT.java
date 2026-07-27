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
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
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
 * Integration test for GCSSpannerDV pipeline covering table renaming scenarios.
 *
 * <p>This test validates that the pipeline correctly maps source tables to Spanner tables when they
 * have been renamed during the migration, for both: overrides file and session file flow. It also
 * validates that mismatches are correctly identified if the configuration file is omitted.
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVRenamedTableIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVRenamedTableIT/spanner-schema.sql";
  private static final String OVERRIDES_FILE_RESOURCE =
      "GCSSpannerDVRenamedTableIT/schema_overrides.json";
  private static final String SESSION_FILE_RESOURCE = "GCSSpannerDVRenamedTableIT/session.json";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  private void generateAndUploadData(String gcsSubDir) throws IOException {
    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build());

    uploadAvroFileToGcs(
        gcsSubDir + "/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Members")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to("Alice")
                .set("age")
                .to(30L)
                .set("created_at")
                .to(com.google.cloud.Timestamp.parseTimestamp(t1.toString()))
                .build()));
  }

  @Test
  public void testWithSchemaOverridesFile() throws Exception {
    String gcsInputDirectory = getGcsPath("input_overrides");
    generateAndUploadData("input_overrides");

    // Wait for Spanner's exact staleness read bound in SpannerReaderTransform
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
            null, // sessionFilePath
            OVERRIDES_FILE_RESOURCE,
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 1L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Members",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }

  @Test
  public void testWithSessionFile() throws Exception {
    String gcsInputDirectory = getGcsPath("input_session");
    generateAndUploadData("input_session");

    // Wait for Spanner's exact staleness read bound in SpannerReaderTransform
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
            SESSION_FILE_RESOURCE,
            null, // schemaOverridesFilePath
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 1L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Members",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }

  @Test
  @org.junit.Ignore("Bug in template: pipeline crashes if source table is missing from Spanner DDL")
  public void testWithoutSchemaMappingFile() throws Exception {
    String gcsInputDirectory = getGcsPath("input_nomapping");
    generateAndUploadData("input_nomapping");

    // Wait for Spanner's exact staleness read bound in SpannerReaderTransform
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
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ 0L,
                /* totalRowsMismatched= */ 2L,
                /* tablesWithMismatches= */ "Members,Users")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 0L,
                /* matchedRowCount= */ 0L,
                /* mismatchRowCount= */ 1L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Members",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 0L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 0L,
                /* mismatchRowCount= */ 1L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:1, event_id:E1]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Members", "[user_id:1, event_id:E1]", "MISSING_IN_SOURCE")));
  }
}
