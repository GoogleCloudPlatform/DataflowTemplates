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

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
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
 * Integration tests verifying the custom transformation logic of the GCSSpannerDV pipeline.
 *
 * <p>Ensures the pipeline can apply custom user transformations for dropping rows, explicitly type
 * casting columns, handling complex avro datatypes, and adding dynamically computed columns.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVCustomTransformationIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVCustomTransformationIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException, InterruptedException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    // Create and upload jar for custom transformations
    createAndUploadJarToGcs("custom");
  }

  /**
   * Tests custom transformation scenarios:
   *
   * <ol>
   *   <li>Basic non-PK data transformation (inherently tested by points 2 and 3).
   *   <li>Explicit type handling (String full_name -> BYTES).
   *   <li>Complex Avro datatype transformation (modifying TIMESTAMP created_at by +1 hour).
   *   <li>Row filtering via isEventFiltered() (dropping Users record where age = 99).
   *   <li>Passing transformationCustomParameters ("InWonderland" parameter appended to full_name).
   *   <li>Sharded topology (preserving migration_shard_id column without explicit transformations).
   * </ol>
   */
  @Test
  public void testCustomTransformations() throws Exception {

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");

    // 1 matched record, 1 record to be filtered out (age = 99)
    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(), // Matched record
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 99) // Will be filtered out
                .set("created_at", t1)
                .build()); // Dropped record

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);

    // 2. Inject Spanner Records (Destination)
    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to(ByteArray.copyFrom("AliceInWonderland"))
                .set("age")
                .to(30L)
                .set("created_at")
                .to(
                    Timestamp.parseTimestamp(
                        "2024-01-01T11:00:00Z")) // "2024-01-01T10:00:00Z" + 1 hour
                .set("migration_shard_id")
                .to("shard1")
                .build())); // Dropped record (user_id=2) is NOT written to Spanner.

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

    // 3. Launch Pipeline
    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "custom/customTransformation.jar", "com.custom.CustomTransformationForDVIT")
            .setCustomParameters("InWonderland")
            .build();

    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsInputDirectory,
            "GCSSpannerDVCustomTransformationIT/session.json",
            null,
            null,
            null,
            customTransformation,
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    // 4. Assert BigQuery Validation Results
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
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L, // Record 2 is filtered out
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(bigQueryResourceManager, Arrays.asList());
  }

  // TODO: @aasthabharill Add test for scenario where Exception is thrown by Custom transformation
  // once bug is fixed. (Currently, it crashes the validation pipeline)
}
