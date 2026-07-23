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
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for GCSSpannerDV pipeline covering schema-related boundary conditions.
 *
 * <p>This test validates:
 * <li>Empty tables (both source and destination have 0 rows).
 * <li>Reserved SQL Keywords (tables and columns that share names with SQL keywords).
 */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVSchemaIT extends GCSSpannerDVITBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVSchemaIT.class);
  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVSchemaIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up Spanner and BigQuery resources");
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    LOG.info("BigQuery dataset created");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    LOG.info("Spanner instance created");
  }

  /**
   * Validates that the pipeline does not fail when processing tables with 0 rows. No BigQuery
   * Tables get created as there was nothing to validate
   */
  @Test
  public void validationTestWithEmptyTables() throws Exception {
    LOG.info("Uploading an empty Avro file to GCS");

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/empty_roles.avro",
        GCSSpannerDVAvroSetupHelper.TableDef.ACCOUNT_ROLES.schema,
        Collections.emptyList());

    LOG.info("No Spanner records to inject for empty table test.");

    // Launch Pipeline
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

    // Since 0 records are processed, Dataflow BigQueryIO never creates the output tables.
    // The successful completion of the pipeline without crashing is the validation.
  }

  /**
   * Validates that tables and columns using Spanner reserved keywords do not break query
   * generation. Tested using a table named 'ORDER' and columns 'SELECT', 'GROUP', and 'TABLE'.
   */
  @Test
  public void validationTestWithReservedKeywords() throws Exception {
    LOG.info("Generating Avro Records for Reserved Keywords");

    Schema reservedKeywordsSchema =
        getSchemaFromAvscFile("GCSSpannerDVSchemaIT/reserved_keywords.avsc");
    GCSSpannerDVAvroSetupHelper.TableDef reservedTableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            reservedKeywordsSchema, "ORDER", Arrays.asList("SELECT"));

    List<GenericRecord> records =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(reservedTableDef, null)
                .set("SELECT", "PK_1")
                .set("GROUP", "group_value")
                .set("TABLE", "table_value")
                .build());

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/reserved.avro", reservedKeywordsSchema, records);

    LOG.info("Injecting Spanner records for ORDER table");

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("ORDER")
                .set("SELECT")
                .to("PK_1")
                .set("GROUP")
                .to("group_value")
                .set("TABLE")
                .to("table_value")
                .build()));

    Thread.sleep(20000);

    // Launch Pipeline
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

    // Assert BigQuery Validation Results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 1L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "ORDER",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L,
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));
  }
}
