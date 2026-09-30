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

import static com.google.common.truth.Truth.assertWithMessage;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Load test for the {@link GCSSpannerDV} template validating ~5TB of Avro data (21 shards, 2
 * non-empty tables) against a static, pre-populated Spanner database.
 *
 * <p>Uses {@code com.custom.CustomTransformationForDV5TBLT} to mutate {@code col1} for all shards
 * except {@code shard_1}, resulting in 1 matching shard and 20 mismatched shards.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDV5TBLT extends GCSSpannerDVLTBase {

  private static final String SPANNER_PROJECT_ID = "cloud-teleport-testing";
  private static final String SPANNER_INSTANCE_ID = "teleport-avro-to-spanner-dv";
  private static final String SPANNER_DATABASE_ID = "spanner_3tables10cols";
  private static final String GCS_INPUT_DIRECTORY =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/avro_data";
  private static final String SESSION_FILE_PATH =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/session_file.json";
  private static final String TRANSFORMATION_CLASS = "com.custom.CustomTransformationForDV5TBLT";

  private static final int NUM_SHARDS = 21;
  private static final int MISMATCHED_SHARDS = NUM_SHARDS - 1;
  // The Avro dataset contains ~26% duplicate records per shard (a sourcedb-to-spanner artifact),
  // so per-shard Avro row counts are higher than the deduplicated Spanner row counts. DV counts
  // every duplicate source copy as matched, and the expected counts below include that behavior.
  // Update them if b/543222130 is fixed.
  private static final long AVRO_ROWS_PER_SHARD_TABLE1 = 5_369_311L;
  private static final long AVRO_ROWS_PER_SHARD_TABLE2 = 13_234_026L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE1 = 4_255_685L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE2 = 10_489_500L;

  private static final Duration JOB_TIMEOUT = Duration.ofHours(1);

  @Override
  protected Optional<StaticSpannerTarget> staticSpannerTarget() {
    return Optional.of(
        new StaticSpannerTarget(SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID));
  }

  @Override
  protected List<String> resourceHints() {
    return List.of("cpu_count=16", "min_ram=60GB");
  }

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    uploadCustomShardJarToGcs("custom");
  }

  @Test
  public void validate5TBWith20MismatchedShards() throws Exception {
    // 1. Launch validation pipeline and wait for completion
    CustomTransformation customTransformation =
        CustomTransformation.builder("custom/customTransformation.jar", TRANSFORMATION_CLASS)
            .build();
    LaunchInfo jobInfo =
        launchValidationJob(
            GCS_INPUT_DIRECTORY,
            JOB_TIMEOUT,
            customTransformation,
            Map.of("sessionFilePath", SESSION_FILE_PATH),
            Map.of());
    collectAndExportMetrics(jobInfo);

    // 2. Assert BigQuery validation results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        List.of(
            new ValidationSummaryDto(
                /* status= */ "MISMATCH",
                /* totalTablesValidated= */ 2L,
                /* totalRowsMatched= */ AVRO_ROWS_PER_SHARD_TABLE1 + AVRO_ROWS_PER_SHARD_TABLE2,
                /* totalRowsMismatched= */ MISMATCHED_SHARDS
                    * (AVRO_ROWS_PER_SHARD_TABLE1
                        + SPANNER_ROWS_PER_SHARD_TABLE1
                        + AVRO_ROWS_PER_SHARD_TABLE2
                        + SPANNER_ROWS_PER_SHARD_TABLE2),
                /* tablesWithMismatches= */ "table1,table2")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        List.of(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "table1",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ NUM_SHARDS * AVRO_ROWS_PER_SHARD_TABLE1,
                /* destinationRowCount= */ AVRO_ROWS_PER_SHARD_TABLE1
                    + MISMATCHED_SHARDS * SPANNER_ROWS_PER_SHARD_TABLE1,
                /* matchedRowCount= */ AVRO_ROWS_PER_SHARD_TABLE1,
                /* mismatchRowCount= */ MISMATCHED_SHARDS
                    * (AVRO_ROWS_PER_SHARD_TABLE1 + SPANNER_ROWS_PER_SHARD_TABLE1)),
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "table2",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ NUM_SHARDS * AVRO_ROWS_PER_SHARD_TABLE2,
                /* destinationRowCount= */ AVRO_ROWS_PER_SHARD_TABLE2
                    + MISMATCHED_SHARDS * SPANNER_ROWS_PER_SHARD_TABLE2,
                /* matchedRowCount= */ AVRO_ROWS_PER_SHARD_TABLE2,
                /* mismatchRowCount= */ MISMATCHED_SHARDS
                    * (AVRO_ROWS_PER_SHARD_TABLE2 + SPANNER_ROWS_PER_SHARD_TABLE2))));

    // 3. Assert MismatchedRecords by counts per group; the table has ~667M rows, so it cannot be
    // read row by row. Spanner-side (MISSING_IN_SOURCE) records have a NULL shard_id.
    Map<String, Long> expectedMismatchCounts = new HashMap<>();
    for (int shard = 2; shard <= NUM_SHARDS; shard++) {
      expectedMismatchCounts.put(
          mismatchKey(null, "table1", "MISSING_IN_DESTINATION", "shard_" + shard),
          AVRO_ROWS_PER_SHARD_TABLE1);
      expectedMismatchCounts.put(
          mismatchKey(null, "table2", "MISSING_IN_DESTINATION", "shard_" + shard),
          AVRO_ROWS_PER_SHARD_TABLE2);
    }
    expectedMismatchCounts.put(
        mismatchKey(null, "table1", "MISSING_IN_SOURCE", null),
        MISMATCHED_SHARDS * SPANNER_ROWS_PER_SHARD_TABLE1);
    expectedMismatchCounts.put(
        mismatchKey(null, "table2", "MISSING_IN_SOURCE", null),
        MISMATCHED_SHARDS * SPANNER_ROWS_PER_SHARD_TABLE2);

    assertWithMessage("MismatchedRecords counts per schema/table/mismatch_type/shard_id")
        .that(countMismatchedRecords())
        .containsExactlyEntriesIn(expectedMismatchCounts);
  }
}
