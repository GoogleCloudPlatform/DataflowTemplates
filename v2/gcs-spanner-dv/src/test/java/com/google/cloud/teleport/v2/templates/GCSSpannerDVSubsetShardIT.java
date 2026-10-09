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
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.RecordBuilder;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.TableDef;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for validating a subset of shards with {@code --shardIds}.
 *
 * <p>Every test writes the same rows to GCS ({@code input/Table/shardId/}) and Spanner for several
 * shards, selects some of them, and checks that the row counts cover only the selected shards on
 * both sides: 1. Spanner rows filtered on the session's ShardIdColumn. 2. Spanner rows filtered by
 * a {@code spannerQuery} from the table configuration file, with one renamed table. 3. {@code
 * --shardIds} combined with {@code --tables}. Tests 1 and 2 also validate AuditLog, a table that
 * exists only in Spanner, with the session mapper and the schema overrides mapper respectively.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVSubsetShardIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVSubsetShardIT/spanner-schema.sql";
  private static final String SPANNER_SHARDED_DDL_RESOURCE =
      "GCSSpannerDVSubsetShardIT/spanner-schema-shardIdColumn.sql";
  private static final String SESSION_SHARDED_RESOURCE =
      "GCSSpannerDVSubsetShardIT/session-sharded.json";
  private static final String OVERRIDES_RENAMED_TABLE_RESOURCE =
      "GCSSpannerDVSubsetShardIT/schema-overrides-renamed-table.json";
  private static final String TABLE_CONFIG_SPANNER_QUERY_RESOURCE =
      "GCSSpannerDVSubsetShardIT/table-config-spanner-query.json";
  private static final String TABLE_CONFIG_SPANNER_ONLY_TABLE_RESOURCE =
      "GCSSpannerDVSubsetShardIT/table-config-spanner-only-table.json";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
  }

  /**
   * Users rows are filtered on the session's ShardIdColumn; shard3 is excluded on both sides.
   * AuditLog exists only in Spanner (not in the session file), so it's validated too, read with its
   * spannerQuery: its one selected row has no source row and is reported as a mismatch. This test
   * also validates a scenario where both ShardIdColumn and spannerQuery flows are used
   * simultaneously depending on the table.
   */
  @Test
  public void testShardIdsWithShardIdColumnAndSpannerOnlyTable() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_SHARDED_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    // Source: one Users row per shard (shard1, shard2, shard3).
    uploadAvroFileToGcs(
        "input/Users/shard1/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/Users/shard2/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard2")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 31)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/Users/shard3/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard3")
                .set("user_id", 3L)
                .set("event_id", "E3")
                .set("full_name", "Carol")
                .set("age", 32)
                .set("created_at", t1)
                .build()));

    // Destination: the same rows, tagged with their migration_shard_id.
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
                .to(2L)
                .set("event_id")
                .to("E2")
                .set("full_name")
                .to("Bob")
                .set("age")
                .to(31L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("migration_shard_id")
                .to("shard3")
                .set("user_id")
                .to(3L)
                .set("event_id")
                .to("E3")
                .set("full_name")
                .to("Carol")
                .set("age")
                .to(32L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            // Spanner-only table: the spannerQuery selects log_id 1 only.
            Mutation.newInsertOrUpdateBuilder("AuditLog")
                .set("log_id")
                .to(1L)
                .set("message")
                .to("selected")
                .build(),
            Mutation.newInsertOrUpdateBuilder("AuditLog")
                .set("log_id")
                .to(2L)
                .set("message")
                .to("not selected")
                .build()));

    Thread.sleep(20000);

    gcsClient.uploadArtifact(
        "table-config.json",
        Resources.getResource(TABLE_CONFIG_SPANNER_ONLY_TABLE_RESOURCE).getPath());

    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            getGcsPath("input"),
            SESSION_SHARDED_RESOURCE,
            null,
            null,
            null,
            null,
            Map.of(
                "shardIds",
                "shard1,shard2",
                "tableConfigurationFilePath",
                getGcsPath("table-config.json")));
    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ 2L,
                /* totalRowsMismatched= */ 1L,
                /* tablesWithMismatches= */ "AuditLog")));
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
                /* tableName= */ "AuditLog",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 0L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 0L,
                /* mismatchRowCount= */ 1L)));
  }

  /**
   * No ShardIdColumn: each table's spannerQuery reads only shard1's leading-PK range (user_id 2 or
   * less, role_id 1 or less). Source table AccountRolesSrc is renamed to Spanner AccountRoles; its
   * config key is the source name and its query reads the Spanner table. shard2's rows are in GCS
   * and Spanner but must not be compared. AuditLog exists only in Spanner and is listed in
   * tableNames, so it's validated with its spannerQuery: its one selected row has no source row and
   * is reported as a mismatch.
   */
  @Test
  public void testShardIdsWithSpannerQueryRenamedTableAndSpannerOnlyTable() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    TableDef renamedRolesTableDef =
        new TableDef(TableDef.ACCOUNT_ROLES.schema, "AccountRolesSrc", Arrays.asList("role_id"));

    // Source: shard1 has Users 1-2 and AccountRolesSrc 1; shard2 has Users 3 and AccountRolesSrc 2.
    uploadAvroFileToGcs(
        "input/Users/shard1/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(),
            new RecordBuilder(TableDef.USERS, "shard1")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 31)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/Users/shard2/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard2")
                .set("user_id", 3L)
                .set("event_id", "E3")
                .set("full_name", "Carol")
                .set("age", 32)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/AccountRolesSrc/shard1/roles.avro",
        renamedRolesTableDef.schema,
        Arrays.asList(
            new RecordBuilder(renamedRolesTableDef, "shard1")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build()));
    uploadAvroFileToGcs(
        "input/AccountRolesSrc/shard2/roles.avro",
        renamedRolesTableDef.schema,
        Arrays.asList(
            new RecordBuilder(renamedRolesTableDef, "shard2")
                .set("role_id", 2)
                .set("role_name", "USER")
                .build()));

    // Destination: the same rows; the tables have no shard ID column.
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
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(3L)
                .set("event_id")
                .to("E3")
                .set("full_name")
                .to("Carol")
                .set("age")
                .to(32L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
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
            // Spanner-only table: the spannerQuery selects log_id 1 only.
            Mutation.newInsertOrUpdateBuilder("AuditLog")
                .set("log_id")
                .to(1L)
                .set("message")
                .to("selected")
                .build(),
            Mutation.newInsertOrUpdateBuilder("AuditLog")
                .set("log_id")
                .to(2L)
                .set("message")
                .to("not selected")
                .build()));

    Thread.sleep(20000);

    gcsClient.uploadArtifact(
        "table-config.json", Resources.getResource(TABLE_CONFIG_SPANNER_QUERY_RESOURCE).getPath());

    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            getGcsPath("input"),
            null,
            OVERRIDES_RENAMED_TABLE_RESOURCE,
            null,
            null,
            null,
            Map.of(
                "shardIds",
                "shard1",
                "tableConfigurationFilePath",
                getGcsPath("table-config.json")));
    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 3L,
                /* totalRowsMatched= */ 3L,
                /* totalRowsMismatched= */ 1L,
                /* tablesWithMismatches= */ "AuditLog")));
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
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AuditLog",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 0L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 0L,
                /* mismatchRowCount= */ 1L)));
  }

  /** --shardIds with --tables validates only the selected table's rows of the selected shards. */
  @Test
  public void testShardIdsWithTablesValidatesOnlySelectedTable() throws Exception {
    createSpannerDDL(spannerResourceManager, SPANNER_SHARDED_DDL_RESOURCE);

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    // Source: one Users row per shard (shard1, shard2, shard3), and one AccountRoles row in
    // shard1 that --tables=Users must ignore.
    uploadAvroFileToGcs(
        "input/Users/shard1/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/Users/shard2/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard2")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 31)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/Users/shard3/users.avro",
        TableDef.USERS.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.USERS, "shard3")
                .set("user_id", 3L)
                .set("event_id", "E3")
                .set("full_name", "Carol")
                .set("age", 32)
                .set("created_at", t1)
                .build()));
    uploadAvroFileToGcs(
        "input/AccountRoles/shard1/roles.avro",
        TableDef.ACCOUNT_ROLES.schema,
        Arrays.asList(
            new RecordBuilder(TableDef.ACCOUNT_ROLES, "shard1")
                .set("role_id", 1)
                .set("role_name", "ADMIN")
                .build()));

    // Destination: the same rows, tagged with their migration_shard_id.
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
                .to(2L)
                .set("event_id")
                .to("E2")
                .set("full_name")
                .to("Bob")
                .set("age")
                .to(31L)
                .set("created_at")
                .to(Timestamp.parseTimestamp(t1.toString()))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("migration_shard_id")
                .to("shard3")
                .set("user_id")
                .to(3L)
                .set("event_id")
                .to("E3")
                .set("full_name")
                .to("Carol")
                .set("age")
                .to(32L)
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

    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            getGcsPath("input"),
            SESSION_SHARDED_RESOURCE,
            null,
            null,
            null,
            null,
            Map.of("shardIds", "shard1,shard2", "tables", "Users"));
    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 2L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));
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
                /* mismatchRowCount= */ 0L)));
  }
}
