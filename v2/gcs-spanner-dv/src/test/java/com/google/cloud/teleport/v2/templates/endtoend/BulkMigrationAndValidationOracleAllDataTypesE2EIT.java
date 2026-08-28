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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.Collections;
import java.util.TimeZone;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-End Integration test validating the migration and validation of all supported data types
 * from Oracle to Spanner.
 *
 * <p>This test verifies the entire lifecycle of data types across two pipelines (Bulk Migration and
 * Data Validation). Specifically, it evaluates how the bulk migration pipeline maps each Oracle
 * data type to Avro and Spanner, and subsequently, how the validation pipeline uses those Avro
 * files to perform end-to-end data validation.
 *
 * <p>The test is driven by schemas that reflect real-world mappings:
 *
 * <ul>
 *   <li>The Oracle schema contains all supported Oracle data types.
 *   <li>The Spanner schema utilizes the default data type mapping provided by Spanner Migration
 *       Tool (SMT).
 * </ul>
 *
 * <p>To ensure comprehensive boundary coverage, the test injects and validates four distinct rows
 * of data:
 *
 * <ul>
 *   <li><b>Standard Row:</b> Typical, everyday values.
 *   <li><b>Null Row:</b> Tests NULL value handling across all nullable columns.
 *   <li><b>Minimum Row:</b> Tests lower bounds, negative limits, and minimum string lengths.
 *   <li><b>Maximum Row:</b> Tests upper bounds, large text/blob limits, and maximum string sizes.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class BulkMigrationAndValidationOracleAllDataTypesE2EIT extends EndToEndTestingITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "BulkMigrationAndValidationOracleAllDataTypesE2EIT/spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE =
      "BulkMigrationAndValidationOracleAllDataTypesE2EIT/oracle-schema.sql";

  private CloudOracleResourceManager oracleResourceManager;
  private TimeZone originalTimeZone;

  @Before
  public void setUp() throws IOException {
    originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    String password =
        System.getProperty(
            "cloudOraclePassword", System.getProperty("cloudOracleSysPassword", "oracle"));
    String username = System.getProperty("cloudOracleUsername", "system");
    oracleResourceManager =
        (CloudOracleResourceManager)
            CloudOracleResourceManager.builder(testName)
                .setUsername(username)
                .setPassword(password)
                .build();
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
        oracleResourceManager, flexTemplateDataflowJobResourceManager);
    // Spanner and BigQuery are automatically cleaned up in tearDownBase()
  }

  @Test
  public void allDataTypesE2E() throws Exception {
    /*
     * Creates a table and inserts 4 boundary testing rows (Standard, NULL, Minimum, Maximum).
     */
    executeSqlScript(oracleResourceManager, ORACLE_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    // 2. Launch Bulk Pipeline (SourceDbToSpanner)
    String gcsOutputDirectory = "gs://" + artifactBucketName + "/" + testId;

    // Launch Bulk Pipeline (SourceDbToSpanner)
    LaunchInfo bulkJobInfo =
        launchBulkDataflowJob(
            testName, spannerResourceManager, gcsClient, oracleResourceManager, null, false);
    assertThatPipeline(bulkJobInfo).isRunning();
    pipelineOperator().waitUntilDone(createConfig(bulkJobInfo));

    // 3. Assert on spanner rows to verify the bulk job was actually successful
    assertThat(spannerResourceManager.getRowCount("AllDatatypes")).isEqualTo(4L);

    // 4. Launch Validation Pipeline (GCSSpannerDV)
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

    // 5. Assert BigQuery Validation Results (Expect PERFECT MATCH)
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Collections.singletonList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 4L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Collections.singletonList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "AllDatatypes",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 4L,
                /* destinationRowCount= */ 4L,
                /* matchedRowCount= */ 4L,
                /* mismatchRowCount= */ 0L)));
  }
}
