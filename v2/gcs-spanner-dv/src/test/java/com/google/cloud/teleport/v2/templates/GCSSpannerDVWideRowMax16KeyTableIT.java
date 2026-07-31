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
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import java.io.IOException;
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

/** Integration test for GCSSpannerDV validating a wide row with 16 primary key columns. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMax16KeyTableIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVWideRowMax16KeyTableIT/spanner-schema.sql";
  private static final String AVRO_SCHEMA_RESOURCE =
      "GCSSpannerDVWideRowMax16KeyTableIT/16_key_table.avsc";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @Test
  public void test16KeyColumns() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            getSchemaFromAvscFile(AVRO_SCHEMA_RESOURCE),
            "Max16KeyTable",
            Arrays.asList(
                "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9",
                "col_10", "col_11", "col_12", "col_13", "col_14", "col_15", "col_16"));

    // 1 matched, 1 mismatched
    GCSSpannerDVAvroSetupHelper.RecordBuilder matchedAvro =
        new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null);
    GCSSpannerDVAvroSetupHelper.RecordBuilder mismatchedAvro =
        new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null);

    Mutation.WriteBuilder matchedSpanner = Mutation.newInsertOrUpdateBuilder("Max16KeyTable");
    Mutation.WriteBuilder mismatchedSpanner = Mutation.newInsertOrUpdateBuilder("Max16KeyTable");

    // Populate PK columns
    for (int i = 1; i <= 16; i++) {
      matchedAvro.set("col_" + i, 1L);
      matchedSpanner.set("col_" + i).to(1L);

      mismatchedAvro.set("col_" + i, 2L);
      mismatchedSpanner.set("col_" + i).to(2L);
    }

    // Populate non-PK column
    matchedAvro.set("val", "matched");
    matchedSpanner.set("val").to("matched");

    mismatchedAvro.set("val", "avro_val");
    mismatchedSpanner.set("val").to("spanner_val");

    List<GenericRecord> records = Arrays.asList(matchedAvro.build(), mismatchedAvro.build());
    uploadAvroFileToGcs("input/16_keys.avro", tableDef.schema, records);
    spannerResourceManager.write(Arrays.asList(matchedSpanner.build(), mismatchedSpanner.build()));

    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    java.util.Map<String, String> jobParameters = new java.util.HashMap<>();

    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            getGcsPath("input"),
            null,
            null,
            null,
            null,
            null,
            jobParameters);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        List.of(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Max16KeyTable",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    StringBuilder mismatchedPks = new StringBuilder("[");
    for (int i = 1; i <= 16; i++) {
      mismatchedPks.append("col_").append(i).append(":2");
      if (i < 16) {
        mismatchedPks.append(", ");
      }
    }
    mismatchedPks.append("]");

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        List.of(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max16KeyTable",
                /* recordKey= */ mismatchedPks.toString(),
                /* mismatchType= */ "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max16KeyTable",
                /* recordKey= */ mismatchedPks.toString(),
                /* mismatchType= */ "MISSING_IN_DESTINATION")));
  }
}
