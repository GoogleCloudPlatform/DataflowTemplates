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
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordCountDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load test for the {@link GCSSpannerDV} template validating ~5TB of Avro data (21 shards, 2
 * non-empty tables) against a static, pre-populated Spanner database.
 *
 * <p>The Avro input and the Spanner database were produced by a sourcedb-to-spanner migration and
 * are never modified by this test. To exercise the mismatch reporting path at volume, the custom
 * transformation {@code com.custom.CustomTransformationForDV5TBLT} mutates {@code col1} of every
 * row in every shard except {@code shard_1}, so 20 shards mismatch and 1 shard matches.
 *
 * <p>Note: the Avro input contains ~26% duplicate records per shard (a sourcedb-to-spanner
 * artifact). DV counts every source copy, so the source counts below are Avro counts (with
 * duplicates) and the destination counts are {@code matched + onlyInSpanner}.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDV5TBLT extends GCSSpannerDVLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDV5TBLT.class);

  // Static resources. Do not point a SpannerResourceManager at this database: its cleanup drops
  // the database it manages.
  private static final String SPANNER_PROJECT_ID = "cloud-teleport-testing";
  private static final String SPANNER_INSTANCE_ID = "teleport-avro-to-spanner-dv";
  private static final String SPANNER_DATABASE_ID = "spanner_3tables10cols";
  private static final String GCS_INPUT_DIRECTORY =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/avro_data";
  private static final String SESSION_FILE_PATH =
      "gs://nokill-avro-to-spanner-dv/5tb_load_test/session_file.json";

  // Custom transformation (built from v2/spanner-custom-shard).
  private static final String TRANSFORMATION_CLASS = "com.custom.CustomTransformationForDV5TBLT";
  private static final String LOCAL_JAR_PATH =
      "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  private static final String JAR_ARTIFACT_NAME = "customTransformation.jar";

  // Dataset shape. Shards are named shard_1..shard_21; table3 is empty on both sides.
  private static final int NUM_SHARDS = 21;
  private static final String MATCHING_SHARD = "shard_1";
  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";
  private static final long AVRO_ROWS_PER_SHARD_TABLE1 = 5_369_311L;
  private static final long AVRO_ROWS_PER_SHARD_TABLE2 = 13_234_026L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE1 = 4_255_685L;
  private static final long SPANNER_ROWS_PER_SHARD_TABLE2 = 10_489_500L;

  private static final String MISSING_IN_DESTINATION = "MISSING_IN_DESTINATION";
  private static final String MISSING_IN_SOURCE = "MISSING_IN_SOURCE";

  // A manual run without transformation took ~27 minutes. Tighten after the first runs.
  private static final Duration JOB_TIMEOUT = Duration.ofHours(3);

  private GcsResourceManager gcsResourceManager;
  private String transformationJarGcsPath;

  @Override
  protected @Nullable SpannerResourceManager createSpannerResourceManager() {
    return null;
  }

  @Override
  protected String spannerProjectId() {
    return SPANNER_PROJECT_ID;
  }

  @Override
  protected String spannerInstanceId() {
    return SPANNER_INSTANCE_ID;
  }

  @Override
  protected String spannerDatabaseId() {
    return SPANNER_DATABASE_ID;
  }

  @Override
  protected List<String> resourceHints() {
    // cpu_count alone can pick highcpu-16 (~14GB); a manual run peaked at ~26GB per worker.
    return List.of("cpu_count=16", "min_ram=60GB");
  }

  /**
   * Runs after {@link GCSSpannerDVLTBase#setUp()} (JUnit runs superclass {@code @Before} first).
   */
  @Before
  public void setUpTransformationJar() throws IOException {
    File jar = new File(LOCAL_JAR_PATH);
    LOG.info(
        "[5TB-LT] Looking for custom transformation jar at {} (exists={}, size={} bytes)",
        jar.getAbsolutePath(),
        jar.exists(),
        jar.exists() ? jar.length() : -1);
    if (!jar.isFile()) {
      throw new IllegalStateException(
          "Custom transformation jar not found at "
              + jar.getAbsolutePath()
              + ". Build v2/spanner-custom-shard before running this test.");
    }

    gcsResourceManager = createSpannerLTGcsResourceManager();
    gcsResourceManager.uploadArtifact(JAR_ARTIFACT_NAME, jar.getAbsolutePath());
    transformationJarGcsPath =
        ArtifactUtils.getFullGcsPath(
            gcsResourceManager.getBucket(),
            getClass().getSimpleName(),
            gcsResourceManager.runId(),
            JAR_ARTIFACT_NAME);
    LOG.info("[5TB-LT] Uploaded custom transformation jar to {}", transformationJarGcsPath);
  }

  /**
   * Runs before {@link GCSSpannerDVLTBase#cleanUp()} (JUnit runs subclass {@code @After} first).
   */
  @After
  public void cleanUpTransformationJar() {
    if (gcsResourceManager != null) {
      LOG.info("[5TB-LT] Cleaning up GCS resource manager");
      ResourceManagerUtils.cleanResources(gcsResourceManager);
    }
  }

  @Test
  public void validate5TBWith20MismatchedShards() throws Exception {
    int mismatchedShards = NUM_SHARDS - 1;

    // Per table: every Avro copy of shard_1 is MATCHED; every Avro copy of the other shards is
    // only in GCS (col1 mutated) and every Spanner row of the other shards is only in Spanner.
    long matchedTable1 = AVRO_ROWS_PER_SHARD_TABLE1; // 5,369,311
    long matchedTable2 = AVRO_ROWS_PER_SHARD_TABLE2; // 13,234,026
    long onlyInGcsTable1 = mismatchedShards * AVRO_ROWS_PER_SHARD_TABLE1; // 107,386,220
    long onlyInGcsTable2 = mismatchedShards * AVRO_ROWS_PER_SHARD_TABLE2; // 264,680,520
    long onlyInSpannerTable1 = mismatchedShards * SPANNER_ROWS_PER_SHARD_TABLE1; // 85,113,700
    long onlyInSpannerTable2 = mismatchedShards * SPANNER_ROWS_PER_SHARD_TABLE2; // 209,790,000

    // ComputeTableStatsFn: source = matched + onlyInGcs, destination = matched + onlyInSpanner,
    // mismatch = onlyInGcs + onlyInSpanner.
    TableValidationStatsDto expectedTable1 =
        new TableValidationStatsDto(
            null,
            TABLE1,
            "MISMATCH",
            matchedTable1 + onlyInGcsTable1, // 112,755,531
            matchedTable1 + onlyInSpannerTable1, // 90,483,011
            matchedTable1, // 5,369,311
            onlyInGcsTable1 + onlyInSpannerTable1); // 192,499,920
    TableValidationStatsDto expectedTable2 =
        new TableValidationStatsDto(
            null,
            TABLE2,
            "MISMATCH",
            matchedTable2 + onlyInGcsTable2, // 277,914,546
            matchedTable2 + onlyInSpannerTable2, // 223,024,026
            matchedTable2, // 13,234,026
            onlyInGcsTable2 + onlyInSpannerTable2); // 474,470,520
    ValidationSummaryDto expectedSummary =
        new ValidationSummaryDto(
            "MISMATCH",
            2L,
            expectedTable1.matchedRowCount() + expectedTable2.matchedRowCount(), // 18,603,337
            expectedTable1.mismatchRowCount() + expectedTable2.mismatchRowCount(), // 666,970,440
            TABLE1 + "," + TABLE2);

    // GCS-side mismatches carry the shard id; Spanner-side mismatches have a NULL shard_id.
    List<MismatchedRecordCountDto> expectedMismatchCounts = new ArrayList<>();
    for (int shard = 1; shard <= NUM_SHARDS; shard++) {
      String shardId = "shard_" + shard;
      if (shardId.equals(MATCHING_SHARD)) {
        continue;
      }
      expectedMismatchCounts.add(
          new MismatchedRecordCountDto(
              TABLE1, MISSING_IN_DESTINATION, shardId, AVRO_ROWS_PER_SHARD_TABLE1));
      expectedMismatchCounts.add(
          new MismatchedRecordCountDto(
              TABLE2, MISSING_IN_DESTINATION, shardId, AVRO_ROWS_PER_SHARD_TABLE2));
    }
    expectedMismatchCounts.add(
        new MismatchedRecordCountDto(TABLE1, MISSING_IN_SOURCE, null, onlyInSpannerTable1));
    expectedMismatchCounts.add(
        new MismatchedRecordCountDto(TABLE2, MISSING_IN_SOURCE, null, onlyInSpannerTable2));

    LOG.info(
        "[5TB-LT] Starting 5TB DV load test. gcsInputDirectory={}, spanner={}/{}/{},"
            + " sessionFilePath={}, transformationClass={}, transformationJar={},"
            + " resourceHints={}, timeout={}",
        GCS_INPUT_DIRECTORY,
        SPANNER_PROJECT_ID,
        SPANNER_INSTANCE_ID,
        SPANNER_DATABASE_ID,
        SESSION_FILE_PATH,
        TRANSFORMATION_CLASS,
        transformationJarGcsPath,
        resourceHints(),
        JOB_TIMEOUT);
    LOG.info("[5TB-LT] Expected ValidationSummary: {}", expectedSummary);
    LOG.info("[5TB-LT] Expected TableValidationStats: {}, {}", expectedTable1, expectedTable2);
    LOG.info(
        "[5TB-LT] Expected {} MismatchedRecords groups: {}",
        expectedMismatchCounts.size(),
        expectedMismatchCounts);

    // 1. Run the validation job.
    LaunchInfo jobInfo =
        launchValidationJob(
            GCS_INPUT_DIRECTORY,
            JOB_TIMEOUT,
            Map.of(
                "sessionFilePath", SESSION_FILE_PATH,
                "transformationJarPath", transformationJarGcsPath,
                "transformationClassName", TRANSFORMATION_CLASS),
            Map.of());

    // 2. Metrics are informational; export them before asserting so a failed run still reports.
    collectAndExportMetrics(jobInfo);

    // 3. Log every actual result before asserting, so one failed run shows all numbers.
    GCSSpannerDVTestAsserts.logTableRows(bigQueryResourceManager, "ValidationSummary");
    GCSSpannerDVTestAsserts.logTableRows(bigQueryResourceManager, "TableValidationStats");
    List<MismatchedRecordCountDto> actualMismatchCounts =
        GCSSpannerDVTestAsserts.readMismatchedRecordCounts(bigQueryResourceManager);

    // 4. Assert.
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager, List.of(expectedSummary));
    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager, List.of(expectedTable1, expectedTable2));
    assertWithMessage("MismatchedRecords counts per (table, mismatch_type, shard_id)")
        .that(actualMismatchCounts)
        .containsExactlyElementsIn(expectedMismatchCounts);
    LOG.info("[5TB-LT] All assertions passed for job {}", jobInfo.jobId());
  }
}
