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
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.RecordBuilder;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVAvroSetupHelper.TableDef;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests transient Spanner read failures (via SpannerIO) for GCSSpannerDV pipeline.
 *
 * <p>Test cases covered:
 *
 * <ul>
 *   <li>Native Beam SpannerIO transient read failures handling (via limited duration error
 *       injection).
 *   <li>Matching large payloads across GCS (Avro) and Spanner despite intermittent read timeouts.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(GCSSpannerDV.class)
@RunWith(JUnit4.class)
public class GCSSpannerDVSpannerReadFT extends GCSSpannerDVFTBase {

  private static final String SPANNER_DDL_RESOURCE = "GCSSpannerDVSpannerReadFT/spanner-schema.sql";
  private static final int NUM_RECORDS = 500;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
  }

  // Tests transient UNAVAILABLE errors on SpannerIO.readAll() to ensure native exponential backoff
  // succeeds.
  @Test
  public void testTransientReadFailure() throws IOException, InterruptedException {
    List<GenericRecord> records = new ArrayList<>();
    List<Mutation> mutations = new ArrayList<>();
    Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

    // We insert 500 rows to ensure Dataflow has enough data to potentially split bundles
    // and exercise the SpannerIO read logic across multiple task execution boundaries,
    // rather than trivially passing with a single row.
    for (int i = 0; i < NUM_RECORDS; i++) {
      long userId = (long) i;
      String eventId = "E" + i;
      String fullName = "User " + i;
      int age = 20 + (i % 30);

      // Avro Record
      GenericRecord record =
          new RecordBuilder(TableDef.USERS, null)
              .set("user_id", userId)
              .set("event_id", eventId)
              .set("full_name", fullName)
              .set("age", age)
              .set("created_at", now)
              .build();
      records.add(record);

      // Spanner Mutation
      mutations.add(
          Mutation.newInsertOrUpdateBuilder("Users")
              .set("user_id")
              .to(userId)
              .set("event_id")
              .to(eventId)
              .set("full_name")
              .to(fullName)
              .set("age")
              .to(age)
              .set("created_at")
              .to(Timestamp.ofTimeSecondsAndNanos(now.getEpochSecond(), now.getNano()))
              .build());
    }

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs("input/users.avro", TableDef.USERS.schema, records);
    spannerResourceManager.write(mutations);

    // Injects a 60-second UNAVAILABLE outage specifically on the Spanner workers to trigger
    // Dataflow task retries.
    String failureInjectionParam =
        "{\"policyType\":\"InitialLimitedDurationErrorInjectionPolicy\", \"policyInput\": {\"duration\":\"PT1M\", \"errorCode\":\"UNAVAILABLE\"}}";
    String bqDatasetId = bigQueryResourceManager.getDatasetId();

    LaunchInfo jobInfo =
        launchFTDataflowJob(
            testName,
            PROJECT,
            spannerResourceManager,
            bqDatasetId,
            gcsInputDirectory,
            null,
            null,
            null,
            null,
            null,
            failureInjectionParam,
            new HashMap<>());

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 500L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 500L,
                /* destinationRowCount= */ 500L,
                /* matchedRowCount= */ 500L,
                /* mismatchRowCount= */ 0L)));

    // No mismatched records should exist
    GCSSpannerDVTestAsserts.assertMismatchedRecords(bigQueryResourceManager, Arrays.asList());
  }
}
