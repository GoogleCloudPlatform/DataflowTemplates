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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-End Integration test for the full Spanner migration lifecycle in a sharded environment.
 * This test simulates a bulk migration from multiple sharded MySQL sources to Spanner, introduces
 * data discrepancies, and runs the GCSSpannerDV validation pipeline to rigorously verify row values
 * and eventual consistency boundaries.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class ShardedBulkMigrationAndValidationE2EIT extends EndToEndTestingITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "ShardedBulkMigrationAndValidationE2EIT/spanner-schema.sql";
  private static final String MYSQL_DDL_RESOURCE =
      "ShardedBulkMigrationAndValidationE2EIT/mysql-schema.sql";
  private static final String SESSION_SHARDED_RESOURCE =
      "ShardedBulkMigrationAndValidationE2EIT/session-sharded.json";

  private static final String LOGICAL_SHARD_1 = "logical_shard1";
  private static final String LOGICAL_SHARD_2 = "logical_shard2";
  private static final String USERS_TABLE = "Users";
  private static final String ACCOUNT_ROLES_TABLE = "AccountRoles";
  private static final String MISSING_IN_DESTINATION = "MISSING_IN_DESTINATION";
  private static final String MISSING_IN_SOURCE = "MISSING_IN_SOURCE";
  private static final String MISMATCH = "MISMATCH";
  private static final String MATCH = "MATCH";

  private CloudMySQLResourceManager mySQLResourceManager1;
  private CloudMySQLResourceManager mySQLResourceManager2;

  @Before
  public void setUp() throws IOException {
    mySQLResourceManager1 = CloudMySQLResourceManager.builder(testName + "1").build();
    mySQLResourceManager2 = CloudMySQLResourceManager.builder(testName + "2").build();
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();

    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager1, mySQLResourceManager2, flexTemplateDataflowJobResourceManager);
    // Spanner and BigQuery are automatically cleaned up in tearDownBase()

  }

  @Test
  public void shardedMigrationAndValidationE2E() throws Exception {
    // 1. Generate and Upload Source Records to MySQL (Both Shards)
    executeSqlScript(mySQLResourceManager1, MYSQL_DDL_RESOURCE);
    executeSqlScript(mySQLResourceManager2, MYSQL_DDL_RESOURCE);

    // Shard Configuration: 2 physical shards with 1 logical shard each = 2 total logical shards
    // Shard 1 Data
    List<Map<String, Object>> shard1Users =
        List.of(
            Map.of(
                "user_id",
                1L,
                "event_id",
                "E1",
                "full_name",
                "Alice",
                "age",
                30,
                "created_at",
                "2024-01-01 10:00:00"),
            Map.of(
                "user_id",
                2L,
                "event_id",
                "E2",
                "full_name",
                "Bob",
                "age",
                31,
                "created_at",
                "2024-01-02 10:00:00"));
    mySQLResourceManager1.write(USERS_TABLE, shard1Users);

    List<Map<String, Object>> shard1Roles =
        List.of(
            Map.of("role_id", 1L, "role_name", "ADMIN"),
            Map.of("role_id", 2L, "role_name", "USER"));
    mySQLResourceManager1.write(ACCOUNT_ROLES_TABLE, shard1Roles);

    // Shard 2 Data
    List<Map<String, Object>> shard2Users =
        List.of(
            Map.of(
                "user_id",
                4L,
                "event_id",
                "E4",
                "full_name",
                "David",
                "age",
                35,
                "created_at",
                "2024-01-04 10:00:00"));
    mySQLResourceManager2.write(USERS_TABLE, shard2Users);

    List<Map<String, Object>> shard2Roles = List.of(Map.of("role_id", 3L, "role_name", "GUEST"));
    mySQLResourceManager2.write(ACCOUNT_ROLES_TABLE, shard2Roles);

    // 2. Generate and upload shard-bulk.json to GCS
    List<DataShard> dataShards =
        List.of(
            new DataShard(
                LOGICAL_SHARD_1,
                mySQLResourceManager1.getHost(),
                mySQLResourceManager1.getUsername(),
                mySQLResourceManager1.getPassword(),
                String.valueOf(mySQLResourceManager1.getPort()),
                mySQLResourceManager1.getDatabaseName(),
                "",
                "",
                List.of(
                    new Database(
                        mySQLResourceManager1.getDatabaseName(),
                        LOGICAL_SHARD_1,
                        LOGICAL_SHARD_1))),
            new DataShard(
                LOGICAL_SHARD_2,
                mySQLResourceManager2.getHost(),
                mySQLResourceManager2.getUsername(),
                mySQLResourceManager2.getPassword(),
                String.valueOf(mySQLResourceManager2.getPort()),
                mySQLResourceManager2.getDatabaseName(),
                "",
                "",
                List.of(
                    new Database(
                        mySQLResourceManager2.getDatabaseName(),
                        LOGICAL_SHARD_2,
                        LOGICAL_SHARD_2))));
    createAndUploadShardConfigToGcs(dataShards, gcsClient);

    // 3. Launch Bulk Pipeline (SourceDbToSpanner) with multiSharded=true
    String gcsOutputDirectory = "gs://" + artifactBucketName + "/" + testId;

    // Passing the session file is required because we're defining a shardIdColumn
    // The session file supplies this mapping to the pipeline; the schema overrides flow for this is
    // currently broken.
    LaunchInfo bulkJobInfo =
        launchBulkDataflowJob(
            PipelineUtils.createJobName("bulk"),
            spannerResourceManager,
            gcsClient,
            mySQLResourceManager1, // Ignored in multiSharded, but required by method signature
            SESSION_SHARDED_RESOURCE,
            true);
    assertThatPipeline(bulkJobInfo).isRunning();

    PipelineOperator.Result bulkResult =
        pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));
    assertThatResult(bulkResult).isLaunchFinished();

    // 4. Assert on spanner rows to verify the bulk job was actually successful
    assertThat(spannerResourceManager.getRowCount(USERS_TABLE)).isEqualTo(3L);
    assertThat(spannerResourceManager.getRowCount(ACCOUNT_ROLES_TABLE)).isEqualTo(3L);

    // 5. Manipulate Spanner data to create mismatches for the validation pipeline
    // Delete User 2 (MISSING_IN_DESTINATION in logical_shard1)
    // Insert User 3 (MISSING_IN_SOURCE in logical_shard2)
    // Update User 4 (MISMATCHED_VALUE in logical_shard2 -> missing in source + missing in dest)
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.delete(USERS_TABLE, Key.of(LOGICAL_SHARD_1, 2L, "E2")),
            Mutation.newInsertOrUpdateBuilder(USERS_TABLE)
                .set("migration_shard_id")
                .to(LOGICAL_SHARD_2)
                .set("user_id")
                .to(3L)
                .set("event_id")
                .to("E3")
                .set("full_name")
                .to("Charlie")
                .set("age")
                .to(32L)
                .set("created_at")
                .to(com.google.cloud.Timestamp.parseTimestamp("2024-01-03T10:00:00Z"))
                .build(),
            Mutation.newUpdateBuilder(USERS_TABLE)
                .set("migration_shard_id")
                .to(LOGICAL_SHARD_2)
                .set("user_id")
                .to(4L)
                .set("event_id")
                .to("E4")
                .set("age")
                .to(40L)
                .build()));

    // 6. Launch Validation Pipeline (GCSSpannerDV)
    LaunchConfig.Builder dvOptions = LaunchConfig.builder(testName, specPath);
    LaunchInfo dvJobInfo =
        launchDataflowJob(
            dvOptions,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsOutputDirectory,
            SESSION_SHARDED_RESOURCE,
            null,
            null,
            null,
            null,
            null);

    pipelineOperator().waitUntilDone(createConfig(dvJobInfo));

    // 7. Assert BigQuery Validation Results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ MISMATCH,
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ 4L,
                /* totalRowsMismatched= */ 4L,
                /* tablesWithMismatches= */ USERS_TABLE)));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ USERS_TABLE,
                /* status= */ MISMATCH,
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 4L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ ACCOUNT_ROLES_TABLE,
                /* status= */ MATCH,
                /* sourceRowCount= */ 3L,
                /* destinationRowCount= */ 3L,
                /* matchedRowCount= */ 3L,
                /* mismatchRowCount= */ 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        Arrays.asList(
            new MismatchedRecordDto(
                LOGICAL_SHARD_1,
                null,
                USERS_TABLE,
                "[migration_shard_id:logical_shard1, user_id:2, event_id:E2]",
                MISSING_IN_DESTINATION),

            // Note: In case of a data mismatch, getting two separate rows (one MISSING_IN_SOURCE
            // and one MISSING_IN_DESTINATION) is the expected behavior.
            new MismatchedRecordDto(
                LOGICAL_SHARD_2,
                null,
                USERS_TABLE,
                "[migration_shard_id:logical_shard2, user_id:4, event_id:E4]",
                MISSING_IN_DESTINATION),
            new MismatchedRecordDto(
                null,
                null,
                USERS_TABLE,
                "[migration_shard_id:logical_shard2, user_id:3, event_id:E3]",
                MISSING_IN_SOURCE),
            new MismatchedRecordDto(
                null,
                null,
                USERS_TABLE,
                "[migration_shard_id:logical_shard2, user_id:4, event_id:E4]",
                MISSING_IN_SOURCE)));
  }
}
