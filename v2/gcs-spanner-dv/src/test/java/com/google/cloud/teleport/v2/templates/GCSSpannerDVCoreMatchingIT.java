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
 * Integration tests verifying the core data comparison logic of the GCSSpannerDV pipeline.
 *
 * <p>Ensures the pipeline can accurately compare source records (Avro) against destination records
 * (Spanner) across multiple tables, and correctly report on matches, missing rows, and data
 * discrepancies.
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVCoreMatchingIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVCoreMatchingIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  /**
   * Validates core multi-table matching logic across both healthy and unhealthy tables. Tests all
   * fundamental validation scenarios (exactly matching, missing in source, missing in destination,
   * and value mismatches) and asserts that the resulting metrics are correctly rolled up into the
   * BigQuery tables.
   */
  @Test
  public void validationTestWithMatchingAndMismatchedRecords() throws Exception {

    // 1. Generate and Upload Avro Records (Source)

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");
    Instant t3 = Instant.parse("2024-01-03T10:00:00Z");
    Instant t4 = Instant.parse("2024-01-04T10:00:00Z");

    // 1 matched record, 1 record present only in source, 1 record with different value
    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(), // Matched in both source and spanner
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, null)
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 31)
                .set("created_at", t2)
                .build(), // Present in source but not in destination
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, null)
                .set("user_id", 4L)
                .set("event_id", "E4")
                .set("full_name", "David")
                .set("age", 35)
                .set("created_at", t4)
                .build() // Mismatched record: Source age is 35, while spanner has 40
            );

    // All records are matched in Spanner
    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, null)
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, null)
                .set("role_id", 2)
                .set("role_name", "USER")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, null)
                .set("role_id", 3)
                .set("role_name", "GUEST")
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);
    uploadAvroFileToGcs(
        "input/roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        rolesRecords);

    // 2. Inject Spanner Records (Destination)

    spannerResourceManager.write(
        Arrays.asList(
            // Users: 1 matched record, 1 record present only in destination, 1 record with
            // different values
            Mutation.newInsertOrUpdateBuilder("Users")
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
                .build(), // Matched in both source and spanner
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(3L)
                .set("event_id")
                .to("E3")
                .set("full_name")
                .to("Charlie")
                .set("age")
                .to(32L)
                .set("created_at")
                .to(com.google.cloud.Timestamp.parseTimestamp(t3.toString()))
                .build(), // Present in Spanner but not in source
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(4L)
                .set("event_id")
                .to("E4")
                .set("full_name")
                .to("David")
                .set("age")
                .to(40L)
                .set("created_at")
                .to(com.google.cloud.Timestamp.parseTimestamp(t4.toString()))
                .build(), // Mismatched age
            // AccountRoles: 3 matched records
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to("ADMIN")
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(2L)
                .set("role_name")
                .to("USER")
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(3L)
                .set("role_name")
                .to("GUEST")
                .build()));

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

    // 4. Assert BigQuery Validation Results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ 4L,
                /* totalRowsMismatched= */ 4L,
                /* tablesWithMismatches= */ "Users")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 4L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 3L,
                /* mismatchRowCount= */ 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:2, event_id:E2]", "MISSING_IN_DESTINATION"),
            // Note: gcs-spanner-dv currently lacks a MISMATCHED_VALUE category.
            // Differing row values (like User 4's age) are emitted as two discrepancies:
            // one MISSING_IN_SOURCE and one MISSING_IN_DESTINATION.
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4, event_id:E4]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:3, event_id:E3]", "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4, event_id:E4]", "MISSING_IN_SOURCE")));
  }
}
