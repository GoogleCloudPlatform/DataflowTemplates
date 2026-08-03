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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
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
 * Integration tests covering edge cases where Source and Spanner schemas have differences. 1. Data
 * type differences (that don't require custom transformation) 2. Dropped columns in Spanner 3.
 * Newly added columns in Spanner (populated via a custom transformation).
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVSchemaTransformationsIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVSchemaTransformationsIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  /**
   * Tests data type change (STRING to INT64) of a non-PK column that doesn't require explicit
   * handling via custom transformations and are automatically handled by the pipeline.
   */
  @Test
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  public void testDataTypeDifference() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES;

    List<GenericRecord> sourceRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("role_id", 1)
                .set("role_name", "1234")
                .build(), // Match scenario
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("role_id", 2)
                .set("role_name", "5678")
                .build() // Mismatch scenario
            );

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/account_roles.avro", tableDef.schema, sourceRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to(1234L) // role_name as INT
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(2L)
                .set("role_name")
                .to(9999L) // Mismatch
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

    // Note: In case of a data mismatch, getting two separate rows (one MISSING_IN_SOURCE
    // and one MISSING_IN_DESTINATION) is the expected behavior.
    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* recordKey= */ "[role_id:2]",
                /* mismatchType= */ "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* recordKey= */ "[role_id:2]",
                /* mismatchType= */ "MISSING_IN_SOURCE")));
  }

  /** Tests pipeline resilience when columns present in the source are dropped in Spanner. */
  @Test
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  public void testDroppedColumnInSpanner() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef = GCSSpannerDVAvroSetupHelper.TableDef.USERS;

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> sourceRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30) // Age is dropped in Spanner
                .set("created_at", t1)
                .build(), // Match scenario on remaining columns
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 35)
                .set("created_at", t1)
                .build() // Mismatch scenario on full_name
            );

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/users.avro", tableDef.schema, sourceRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to("Alice")
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(2L)
                .set("event_id")
                .to("E2")
                .set("full_name")
                .to("Bobby") // Mismatch in full_name
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
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
                /* tablesWithMismatches= */ "Users")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    // Verify full_name column mismatch (Source: Bob, Spanner: Bobby)
    // Note: In case of a data mismatch, getting two separate rows (one MISSING_IN_SOURCE
    // and one MISSING_IN_DESTINATION) is the expected behavior.
    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* recordKey= */ "[user_id:2, event_id:E2]",
                /* mismatchType= */ "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* recordKey= */ "[user_id:2, event_id:E2]",
                /* mismatchType= */ "MISSING_IN_SOURCE")));
  }

  /**
   * Tests pipeline validation when new columns are added in Spanner and populated via custom
   * transform.
   */
  @Test
  @Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
  public void testAddedColumnInSpanner() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema,
            "Users_AddedColumn",
            Arrays.asList("user_id", "event_id"));

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> sourceRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(), // Match scenario (Custom transform adds status ACTIVE)
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 35)
                .set("created_at", t1)
                .build() // Mismatch scenario (Spanner has status INACTIVE)
            );

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/users_added_column.avro", tableDef.schema, sourceRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users_AddedColumn")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to("Alice")
                .set("age")
                .to(30L)
                .set("status")
                .to("ACTIVE") // Match scenario
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users_AddedColumn")
                .set("user_id")
                .to(2L)
                .set("event_id")
                .to("E2")
                .set("full_name")
                .to("Bob")
                .set("age")
                .to(35L)
                .set("status")
                .to("INACTIVE") // Mismatch scenario
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build()));

    // DataflowRunner does not need Thread.sleep(20000)

    createAndUploadCustomShardJarToGcs("custom");
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "custom/customTransformation.jar", "com.custom.CustomTransformationForDVIT")
            .build();

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
            customTransformation,
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
                /* tablesWithMismatches= */ "Users_AddedColumn")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users_AddedColumn",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    // Verify status column mismatch (Source+Transform: ACTIVE, Spanner: INACTIVE)
    // Note: In case of a data mismatch, getting two separate rows (one MISSING_IN_SOURCE
    // and one MISSING_IN_DESTINATION) is the expected behavior.
    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Users_AddedColumn",
                /* recordKey= */ "[user_id:2, event_id:E2]",
                /* mismatchType= */ "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Users_AddedColumn",
                /* recordKey= */ "[user_id:2, event_id:E2]",
                /* mismatchType= */ "MISSING_IN_SOURCE")));
  }
}
