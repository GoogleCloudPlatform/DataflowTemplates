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
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for GCSSpannerDV pipeline covering the core matching logic.
 *
 * <p>Intent: This test validates the core functional accuracy of the gcs-spanner-dv batch pipeline
 * by verifying that it can correctly identify and categorize rows into MATCHED,
 * MISSING_IN_DESTINATION, MISSING_IN_SOURCE, and mismatches.
 *
 * <p>Simulation Methodology: We create two tables to validate multi-table logic: 1. `Users` table
 * (4 rows simulating 4 scenarios): - Exactly Matching: Present in both source and destination
 * identically. - Missing in Spanner: Present in source only. - Missing in Source: Present in
 * Spanner only. - Mismatched Values: Present in both, but with different values. 2. `AccountRoles`
 * table (3 rows): - All 3 rows are exactly matching to simulate a completely healthy table.
 *
 * <p>Assertions: - **ValidationSummary**: Verified that exactly one row exists globally, rolling up
 * metrics correctly across all tables. - **TableValidationStats**: Verified that each table
 * (`Users`, `AccountRoles`) produced exactly one row with accurate, independent status and counts.
 * - **MismatchedRecords**: Verified every individual discrepancy is explicitly logged and
 * categorized (`MISSING_IN_SOURCE`, `MISSING_IN_DESTINATION`) with the correct primary keys.
 */
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVCoreMatchingIT extends GCSSpannerDVITBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVCoreMatchingIT.class);

  private SpannerResourceManager spannerResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVCoreMatchingIT/Users.sql";

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up Spanner and BigQuery resources");
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    LOG.info("Creating BigQuery dataset");
    bigQueryResourceManager.createDataset(REGION);
    LOG.info("BigQuery dataset created");
    LOG.info("Creating Spanner DDL");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    LOG.info("Spanner instance created");
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
  }

  @Test
  public void validationTestWithMatchingAndMismatchedRecords() throws Exception {
    GCSSpannerDVAvroSetupHelper setupHelper = new GCSSpannerDVAvroSetupHelper();

    // 1. Generate and Upload Avro Records (Source)
    LOG.info("Generating and Uploading Avro Records to GCS");

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");
    Instant t3 = Instant.parse("2024-01-03T10:00:00Z");
    Instant t4 = Instant.parse("2024-01-04T10:00:00Z");

    // 1 matched record, 1 record present only in source, 1 record with different value
    List<GenericRecord> usersRecords =
        Arrays.asList(
            setupHelper.createUsersRecord(
                1L, "E1", "Alice", 30, t1, null), // Matched in both source and spanner
            setupHelper.createUsersRecord(
                2L, "E2", "Bob", 31, t2, null), // Present in source but not in destination
            setupHelper.createUsersRecord(
                4L, "E4", "David", 35, t4,
                null) // Mismatched record: Source age is 35, while spanner has 40
            );

    // All records are matched in Spanner
    List<GenericRecord> rolesRecords =
        Arrays.asList(
            setupHelper.createAccountRolesRecord(1, "ADMIN", null),
            setupHelper.createAccountRolesRecord(2, "USER", null),
            setupHelper.createAccountRolesRecord(3, "GUEST", null));

    String gcsInputDirectory = getGcsPath("input");
    setupHelper.uploadAvroFileToGcs(
        gcsClient, "input/users.avro", setupHelper.getUsersSchema(), usersRecords);
    setupHelper.uploadAvroFileToGcs(
        gcsClient, "input/roles.avro", setupHelper.getAccountRolesSchema(), rolesRecords);

    // 2. Inject Spanner Records (Destination)
    LOG.info("Injecting Spanner records");

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

    // Wait for Spanner's 15-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(15000);

    // 3. Launch Pipeline
    LOG.info("Launching Dataflow validation job");
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
        Arrays.asList(new ValidationSummaryDto("MISMATCH", 2L, 4L, 4L, "Users")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(null, "Users", "MISMATCH", 3L, 3L, 1L, 4L),
            new TableValidationStatsDto(null, "AccountRoles", "MATCH", 3L, 3L, 3L, 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:2,event_id:E2]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4,event_id:E4]", "MISSING_IN_DESTINATION"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:3,event_id:E3]", "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                null, null, "Users", "[user_id:4,event_id:E4]", "MISSING_IN_SOURCE")));
  }
}
