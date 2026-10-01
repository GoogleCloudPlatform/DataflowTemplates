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
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import java.io.IOException;
import java.nio.ByteBuffer;
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

/** Integration test for GCSSpannerDV validating a wide row with 10 MiB data per cell. */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMax10MibCellIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVWideRowMax10MibCellIT/spanner-schema.sql";
  private static final String AVRO_SCHEMA_RESOURCE =
      "GCSSpannerDVWideRowMax10MibCellIT/10_mib_cell_table.avsc";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @Test
  public void test10MibCell() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            GCSSpannerDVAvroSetupHelper.getSchemaFromAvscFile(AVRO_SCHEMA_RESOURCE),
            "Max10MibCellTable",
            Arrays.asList("id"));

    final int safeBlobSize = (10 * 1024 * 1024) - 1024; // 9.9MB to avoid limit issues
    byte[] matchBytes = new byte[safeBlobSize];
    Arrays.fill(matchBytes, (byte) 1);
    byte[] mismatchAvroBytes = new byte[safeBlobSize];
    Arrays.fill(mismatchAvroBytes, (byte) 2);
    byte[] mismatchSpannerBytes = new byte[safeBlobSize];
    Arrays.fill(mismatchSpannerBytes, (byte) 3);

    List<GenericRecord> records =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("id", 1L)
                .set("large_blob", ByteBuffer.wrap(matchBytes))
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("id", 2L)
                .set("large_blob", ByteBuffer.wrap(mismatchAvroBytes))
                .build());

    uploadAvroFileToGcs("input/10_mib_cell.avro", tableDef.schema, records);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Max10MibCellTable")
                .set("id")
                .to(1L)
                .set("large_blob")
                .to(ByteArray.copyFrom(matchBytes))
                .build(),
            Mutation.newInsertOrUpdateBuilder("Max10MibCellTable")
                .set("id")
                .to(2L)
                .set("large_blob")
                .to(ByteArray.copyFrom(mismatchSpannerBytes))
                .build()));

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
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
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        List.of(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Max10MibCellTable",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        List.of(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max10MibCellTable",
                /* recordKey= */ "[id:2]",
                /* mismatchType= */ "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max10MibCellTable",
                /* recordKey= */ "[id:2]",
                /* mismatchType= */ "MISSING_IN_DESTINATION")));
  }
}
