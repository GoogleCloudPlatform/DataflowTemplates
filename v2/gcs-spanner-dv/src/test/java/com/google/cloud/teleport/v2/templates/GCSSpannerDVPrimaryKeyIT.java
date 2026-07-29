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
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.RecordBuilder;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.TableDef;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
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
 * Integration tests verifying complex primary key handling in the GCSSpannerDV pipeline. This test
 * suite validates the data validation pipeline's ability to accurately match records between source
 * (Avro files in GCS) and destination (Spanner databases) when complex primary key scenarios are
 * involved. It covers cases where primary key values are transformed, where primary key columns
 * differ between the source schema and the destination database, and where complex Avro datatypes
 * (like timestamp-micros) are used as primary keys.
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVPrimaryKeyIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVPrimaryKeyIT/spanner-schema.sql";

  @Before
  public void setUp() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  /** Transformed primary keys (value change using custom transformation). */
  @Test
  public void testTransformedPrimaryKey() throws Exception {
    // 1. Setup Source Avro Data
    TableDef transformedTableDef =
        new TableDef(
            TableDef.USERS.schema,
            "Users_PKTransformed",
            Arrays.asList(
                "user_id",
                "full_name")); // user_id as PK as this will be transformed by CustomTransformation

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new RecordBuilder(transformedTableDef, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", Instant.parse("2024-01-01T10:00:00Z"))
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/users.avro", transformedTableDef.schema, usersRecords);

    // 2. Insert Destination Spanner Data
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users_Transformed")
                .set("user_id")
                .to(11L) // Transformed
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to("Alice")
                .set("age")
                .to(30L)
                .set("created_at")
                .to(Timestamp.parseTimestamp("2024-01-01T10:00:00Z"))
                .build()));

    // 3. Build Transformation and Launch Job
    createAndUploadCustomShardJarToGcs("input");

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "input/customTransformation.jar", "com.custom.CustomTransformationForDVIT")
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

    // 4. Validate Results
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
                /* tableName= */ "Users_Transformed",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }

  /** Modified primary key columns (where PK columns are different in SQL and Spanner). */
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  @Test
  public void testModifiedPrimaryKeyColumn() throws Exception {
    // 1. Setup Source Avro Data
    // We retain the original TableDef with 'role_id' as the primary key. This intentionally
    // creates a mismatch with Spanner's DDL where 'role_name' is the primary key.
    TableDef originalTableDef = TableDef.ACCOUNT_ROLES;

    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new RecordBuilder(originalTableDef, null)
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/roles.avro", originalTableDef.schema, rolesRecords);

    // 2. Insert Destination Spanner Data
    // Insert data into Spanner where 'role_name' is enforced natively as the primary key.
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to("ADMIN")
                .build()));

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

    // 3. Launch Job
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

    // 4. Validate Results
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
                /* tableName= */ "AccountRoles",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }

  /** Complex Avro datatype as PK column. */
  @Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
  @Test
  public void testTimestampPrimaryKey() throws Exception {
    // 1. Setup Source Avro Data
    // Define a new TableDef pointing to the custom Avro schema where a Timestamp field
    // ('created_at') is the sole primary key.
    TableDef timestampTableDef =
        new TableDef(TableDef.USERS.schema, "Users_TimestampPK", Arrays.asList("created_at"));

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new RecordBuilder(timestampTableDef, null)
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/users_timestamp.avro", timestampTableDef.schema, usersRecords);

    // 2. Insert Destination Spanner Data
    // Write matching data with the timestamp to Spanner.
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users_TimestampPK")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to("Alice")
                .set("age")
                .to(30L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build()));
    Thread.sleep(20000);

    // 3. Launch Job
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

    // 4. Validate Results
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
                /* tableName= */ "Users_TimestampPK",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }
}
