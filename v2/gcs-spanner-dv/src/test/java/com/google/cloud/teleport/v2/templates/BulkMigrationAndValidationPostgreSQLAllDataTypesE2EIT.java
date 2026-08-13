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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
 *
 * <p>Note on Edge Cases: 0001-01-01 is clamped to 1970-01-01 for Date/Time edge cases in the test
 * data to avoid Debezium Julian calendar shift bugs that push the date back to 0000-12-30 and crash
 * Spanner mapping.
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

    loadSQLFileResource(postgreSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @After
  public void tearDown() {
    // Preserving spannerResourceManager and bigQueryResourceManager for debugging mismatches
    ResourceManagerUtils.cleanResources(
        postgreSQLResourceManager, flexTemplateDataflowJobResourceManager);
    TimeZone.setDefault(originalTimeZone);
  }

  @Test
  public void testAllDataTypesPostgreSQLToSpanner() throws IOException, InterruptedException {
    String gcsOutputDirectory = "gs://" + artifactBucketName + "/" + testId;

    LaunchInfo migrationJobInfo =
        launchBulkDataflowJob(
            testName, spannerResourceManager, gcsClient, postgreSQLResourceManager, null, false);
    assertThatPipeline(migrationJobInfo).isRunning();

    org.apache.beam.it.common.PipelineOperator.Result migrationResult =
        pipelineOperator().waitUntilDone(createConfig(migrationJobInfo));
    assertThatResult(migrationResult).isLaunchFinished();

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
    org.apache.beam.it.common.PipelineOperator.Result validationResult =
        pipelineOperator().waitUntilDone(createConfig(validationJobInfo));
    assertThatResult(validationResult).isLaunchFinished();

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
