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
package com.google.cloud.teleport.v2.templates.loadtesting;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Load test for the {@link GCSSpannerDV} template validating ~5TB of Avro data (21 shards, 2
 * tables) against a static, pre-populated Spanner database.
 *
 * <p>Uses {@code com.custom.CustomTransformationForDV5TBLT} to mutate {@code col1} for all shards
 * except {@code shard_1}, resulting in 1 matching shard and 20 mismatched shards.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDV5TBLT extends GCSSpannerDVLTBase {

  // Static Spanner and GCS input resources are used because this is a huge migration (~5TB);
  // creating and populating these resources in-test would be slow, unreliable, and flaky.
  private static final String SPANNER_PROJECT_ID = "cloud-teleport-testing";
  private static final String SPANNER_INSTANCE_ID = "teleport-avro-to-spanner-dv";
  private static final String SPANNER_DATABASE_ID = "spanner_3tables10cols";
  private static final String GCS_INPUT_DIRECTORY =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/avro_data";
  private static final String SESSION_FILE_PATH =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/session_file.json";

  private static final long NUM_TABLES = 2L;
  private static final int NUM_SHARDS = 21;
  // The Avro dataset contains ~26% duplicate records per shard (a sourcedb-to-spanner artifact),
  // so per-shard Avro row counts are higher than the deduplicated Spanner row counts. DV counts
  // every duplicate source copy as matched, and the expected counts below include that behavior.
  // Update them if b/543222130 is fixed.
  private static final long AVRO_ROWS_PER_SHARD_TABLE1 = 5_369_311L;
  private static final long AVRO_ROWS_PER_SHARD_TABLE2 = 13_234_026L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE1 = 4_255_685L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE2 = 10_489_500L;

  private static final Duration JOB_TIMEOUT = Duration.ofHours(1);

  @Test
  public void validate5TBWith20MismatchedShards() throws Exception {
    setUpResourceManagers(SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID);
    uploadCustomShardJarToGcs("custom");

    // 1. Assert Spanner schema before launching the validation job.
    assertWithMessage("Total tables present in Spanner INFORMATION_SCHEMA")
        .that(
            spannerResourceManager
                .runQuery("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''")
                .get(0)
                .getLong(0))
        .isEqualTo(NUM_TABLES);

    // 2. Launch validation pipeline and wait for completion.
    // sessionFilePath is required to resolve shardIdColumn so the custom transformation receives
    // shard_id on each record; 16-vCPU workers size the job for the 5TB shuffle.
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "custom/customTransformation.jar", "com.custom.CustomTransformationForDV5TBLT")
            .build();
    LaunchInfo jobInfo =
        launchValidationJob(
            GCS_INPUT_DIRECTORY,
            JOB_TIMEOUT,
            customTransformation,
            Map.of("sessionFilePath", SESSION_FILE_PATH),
            Map.of("additionalPipelineOptions", List.of("resourceHints=cpu_count=16")));
    collectAndExportMetrics(jobInfo);

    // 3. Assert BigQuery validation summary and table stats across both tables.
    // Every mutated record in shards 2..21 produces both a MISSING_IN_DESTINATION mismatch (from
    // the mutated Avro hash) and a MISSING_IN_SOURCE mismatch (from the unmatched Spanner row
    // hash).
    int mismatchedShards = NUM_SHARDS - 1;
    long table1MissingInDestRows = mismatchedShards * AVRO_ROWS_PER_SHARD_TABLE1;
    long table2MissingInDestRows = mismatchedShards * AVRO_ROWS_PER_SHARD_TABLE2;
    long table1MissingInSourceRows = mismatchedShards * SPANNER_ROWS_PER_SHARD_TABLE1;
    long table2MissingInSourceRows = mismatchedShards * SPANNER_ROWS_PER_SHARD_TABLE2;
    long table1MismatchedRows = table1MissingInDestRows + table1MissingInSourceRows;
    long table2MismatchedRows = table2MissingInDestRows + table2MissingInSourceRows;

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        List.of(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ NUM_TABLES,
                /* totalRowsMatched= */ AVRO_ROWS_PER_SHARD_TABLE1 + AVRO_ROWS_PER_SHARD_TABLE2,
                /* totalRowsMismatched= */ table1MismatchedRows + table2MismatchedRows,
                /* tablesWithMismatches= */ "table1,table2")));

    // ComputeTableStatsFn derives destinationRowCount as matchedRowCount + missingInSourceCount,
    // so shard_1's duplicate Avro records inflate matchedRowCount and destinationRowCount alike.
    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        List.of(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "table1",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ NUM_SHARDS * AVRO_ROWS_PER_SHARD_TABLE1,
                /* destinationRowCount= */ AVRO_ROWS_PER_SHARD_TABLE1 + table1MissingInSourceRows,
                /* matchedRowCount= */ AVRO_ROWS_PER_SHARD_TABLE1,
                /* mismatchRowCount= */ table1MismatchedRows),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "table2",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ NUM_SHARDS * AVRO_ROWS_PER_SHARD_TABLE2,
                /* destinationRowCount= */ AVRO_ROWS_PER_SHARD_TABLE2 + table2MissingInSourceRows,
                /* matchedRowCount= */ AVRO_ROWS_PER_SHARD_TABLE2,
                /* mismatchRowCount= */ table2MismatchedRows)));

    // 4. Assert MismatchedRecords counts per (table, mismatch_type) and verify shard_1 has 0 rows;
    // the table has ~667M rows, so it cannot be read row by row.
    assertThat(
            GCSSpannerDVTestAsserts.countMismatchedRecords(
                bigQueryResourceManager, null, "table1", "MISSING_IN_DESTINATION", null))
        .isEqualTo(table1MissingInDestRows);
    assertThat(
            GCSSpannerDVTestAsserts.countMismatchedRecords(
                bigQueryResourceManager, null, "table2", "MISSING_IN_DESTINATION", null))
        .isEqualTo(table2MissingInDestRows);
    assertThat(
            GCSSpannerDVTestAsserts.countMismatchedRecords(
                bigQueryResourceManager, null, "table1", "MISSING_IN_SOURCE", null))
        .isEqualTo(table1MissingInSourceRows);
    assertThat(
            GCSSpannerDVTestAsserts.countMismatchedRecords(
                bigQueryResourceManager, null, "table2", "MISSING_IN_SOURCE", null))
        .isEqualTo(table2MissingInSourceRows);
    assertThat(
            GCSSpannerDVTestAsserts.countMismatchedRecords(
                bigQueryResourceManager, null, null, null, "shard_1"))
        .isEqualTo(0L);
  }
}
