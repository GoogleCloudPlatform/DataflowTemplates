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
 * Integration tests verifying the Data Validation logic for sharded environments. This class
 * validates the pipeline's behavior when aggregating data across multiple source database shards.
 *
 * <p>We test the following three scenarios, both with expected MATCH (Users) and MISMATCH
 * (AccountRoles): 1. Globally unique PKs across shards without a shardIdColumn. 2. Duplicate PKs
 * across shards without a shardIdColumn. 3. Duplicate PKs across shards using a shardIdColumn to
 * disambiguate.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVShardedIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVShardedIT/spanner-schema.sql";
  private static final String SPANNER_SHARDED_DDL_RESOURCE =
      "GCSSpannerDVShardedIT/spanner-schema-shardIdColumn.sql";
  private static final String SESSION_SHARDED_RESOURCE =
      "GCSSpannerDVShardedIT/session-sharded.json";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
  }

  /** Simulates globally unique PKs across shards without a shardIdColumn. */
  @Test
  public void testUniquePKs() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard2")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 31)
                .set("created_at", t2)
                .build());

    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard1")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard2")
                .set("role_id", 2)
                .set("role_name", "USER")
                .build() // This record will be dropped in Spanner to simulate a MISMATCH scenario
            );

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);
    uploadAvroFileToGcs(
        "input/roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        rolesRecords);

    spannerResourceManager.write(
        Arrays.asList(
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
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(2L)
                .set("event_id")
                .to("E2")
                .set("full_name")
                .to("Bob")
                .set("age")
                .to(31L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t2.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to("ADMIN")
                .build()));

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
                /* totalRowsMatched= */ 3L,
                /* totalRowsMismatched= */ 1L,
                /* tablesWithMismatches= */ "AccountRoles")));
    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 2L,
                /* mismatchRowCount= */ 0L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 1L)));
  }

  /**
   * Simulates duplicate PKs across shards without a shardIdColumn to disambiguate. Expectation: For
   * Users, Spanner stores only 1 row per PK, but data validation matches both source records to
   * that single row due to identical hashes. For AccountRoles, dropping the single row in Spanner
   * leads to a MISMATCH for both source records.
   */
  @Test
  public void testDuplicatePKsNoShardIdColumn() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard2")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build());

    // These records will be dropped in Spanner to simulate a MISMATCH scenario
    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard1")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard2")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);
    uploadAvroFileToGcs(
        "input/roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        rolesRecords);

    spannerResourceManager.write(
        Arrays.asList(
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
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build()));

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
                /* totalRowsMatched= */ 2L,
                /* totalRowsMismatched= */ 2L,
                /* tablesWithMismatches= */ "AccountRoles")));
    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L, // TODO: @aasthabharill investigate duplicate row
                // reporting - as this is misleading
                /* matchedRowCount= */ 2L,
                /* mismatchRowCount= */ 0L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 0L,
                /* matchedRowCount= */ 0L,
                /* mismatchRowCount= */ 2L)));
  }

  /*
   * Simulates duplicate PKs across shards using a shardIdColumn to disambiguate.
   */
  @Test
  public void testDuplicatePKsWithShardIdColumn() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_SHARDED_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard2")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build());

    List<GenericRecord> rolesRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard1")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES, "shard2")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build() // This record will be dropped in Spanner to simulate a MISMATCH scenario
            );

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);
    uploadAvroFileToGcs(
        "input/roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        rolesRecords);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("migration_shard_id")
                .to("shard1")
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
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("migration_shard_id")
                .to("shard2")
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
                .build(),
            Mutation.newInsertOrUpdateBuilder("AccountRoles")
                .set("migration_shard_id")
                .to("shard1")
                .set("role_id")
                .to(1L)
                .set("role_name")
                .to("ADMIN")
                .build()));

    Thread.sleep(20000);

    // It's mandatory to pass the session file for the ShardIdColumn testcase so the pipeline
    // is aware of it, since the overrides flow is currently broken for this usecase.
    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsInputDirectory,
            SESSION_SHARDED_RESOURCE,
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
                /* totalRowsMatched= */ 3L,
                /* totalRowsMismatched= */ 1L,
                /* tablesWithMismatches= */ "AccountRoles")));
    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 2L,
                /* mismatchRowCount= */ 0L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AccountRoles",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 1L)));
  }
}
