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
package com.google.cloud.teleport.v2.templates.endtoend;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.TimeZone;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-End Integration test validating the migration and validation of all supported data types
 * from PostgreSQL to Spanner.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT extends EndToEndTestingITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT/postgresql-spanner-schema.sql";
  private static final String POSTGRESQL_DDL_RESOURCE =
      "BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT/postgresql-data-types.sql";

  private CloudPostgresResourceManager postgreSQLResourceManager;
  private TimeZone originalTimeZone;

  @Before
  public void setUp() throws Exception {
    originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    postgreSQLResourceManager = CloudPostgresResourceManager.builder(testName).build();
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
  }

  @After
  public void tearDown() {
    if (originalTimeZone != null) {
      TimeZone.setDefault(originalTimeZone);
    }
    ResourceManagerUtils.cleanResources(
        postgreSQLResourceManager, flexTemplateDataflowJobResourceManager);
    // Spanner and BigQuery are automatically cleaned up in tearDownBase()
  }

  @Test
  public void testAllDataTypesPostgreSQLToSpanner() throws IOException, InterruptedException {

    /*
     * Validates all supported PostgreSQL datatype to Spanner GoogleSQL datatype mappings as outlined in
     * go/pg-bulk-migration-support-dd.
     *
     * Testing Methodology & Constraints:
     * 1. Isolated Testing: Each mapping is simulated within a distinct table to isolate and identify
     *    datatype-specific errors cleanly.
     * 2. Edge Case Coverage: 4 rows are inserted per table: a Standard value, a Minimum boundary value,
     *    a Maximum boundary value, and a NULL value.
     * 3. Variable Boundaries: Not all datatypes have structurally defined minimum/maximum boundary
     *    values (e.g., UUID, JSON, bytea). In such instances, a random standard value is injected in
     *    place of the boundary value.
     * 4. NOT NULL Fallbacks: For datatypes that do not accept NULL values (e.g., serial, bigserial),
     *    we insert a static standard value in place of the NULL insert.
     * 5. Spanner Payload Limits (10MB): PostgreSQL supports up to 1GB of data for large object types
     *    (like varchar, text, and bytea). Since Spanner enforces a strict 10MB per-row limit, we clamp
     *    the maximum test values for these datatypes to ~2.6MB (e.g., using repeat('a', 2621440)) to
     *    simulate large payload sizes without triggering database payload rejections.
     * 6. Logical Boundary Clamping: For datatypes whose extreme minimum or maximum values logically fit
     *    within a Spanner row but exceed the supported limits of either Spanner or the migration pipeline
     *    itself, we clamp the insertions to the safest supported boundary.
     * 7. Known Exclusions:
     *      - number-to-NUMERIC mappings are currently skipped due to padding mismatches (b/544589449).
     *      - JSON-to-JSON mappings are skipped due to whitespace minification bugs (b/546487364).
     */
    executeSqlScript(postgreSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    String gcsOutputDirectory = "gs://" + artifactBucketName + "/" + testId;

    // Launch Bulk Pipeline (SourceDbToSpanner)
    LaunchInfo bulkJobInfo =
        launchBulkDataflowJob(
            testName, spannerResourceManager, gcsClient, postgreSQLResourceManager, null, false);

    assertThatPipeline(bulkJobInfo).isRunning();
    pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));

    LaunchConfig.Builder dvOptions = LaunchConfig.builder(testName, specPath);
    LaunchInfo validationJobInfo =
        launchDataflowJob(
            dvOptions,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsOutputDirectory,
            null,
            null,
            null,
            null,
            null,
            null);

    assertThatPipeline(validationJobInfo).isRunning();
    pipelineOperator().waitUntilDone(createConfig(validationJobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        java.util.Collections.singletonList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 129L,
                /* totalRowsMatched= */ 516L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));
  }
}
