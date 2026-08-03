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
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
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
 * Integration tests validating handling of the Spanner maximum string length limit (2,621,440
 * characters).
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMaxStringIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVWideRowMaxStringIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  // Validates a row with a string containing 2,621,440 characters (MATCH) and a corresponding
  // MISMATCH.
  @Test
  public void testMaxStringLength() throws Exception {
    String matchString = "A".repeat(2621440);
    String avroMismatchString = "C".repeat(2621440);
    String spannerMismatchString = "B".repeat(2621440);

    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, null)
                .set("role_id", 1)
                .set("role_name", matchString)
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, null)
                .set("role_id", 2)
                .set("role_name", avroMismatchString)
                .build()); // mismatched row

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        rolesRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to(matchString)
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(2L)
                .set("role_name")
                .to(spannerMismatchString)
                .build())); // mismatched row

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
                /* tablesWithMismatches= */ "AccountRoles")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
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
                null, null, "AccountRoles", "[role_id:2]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "AccountRoles", "[role_id:2]", "MISSING_IN_SOURCE")));
  }
}
