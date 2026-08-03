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
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.MismatchedRecordDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for GCSSpannerDV validating a wide row with 8 KiB primary key. */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMax8KibPrimaryKeyIT extends GCSSpannerDVITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVWideRowMax8KibPrimaryKeyIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
  }

  @Test
  public void test8KibPrimaryKey() throws Exception {
    GCSSpannerDVAvroSetupHelper.TableDef tableDef = GCSSpannerDVAvroSetupHelper.TableDef.USERS;

    // The maximum size of a Spanner primary key is 8192 bytes.
    // The Users table has a composite primary key: user_id (INT64) + event_id (STRING).
    // An INT64 takes 8 bytes. A STRING has an 8 byte internal overhead.
    // To hit exactly 8192 bytes, the event_id string length must be: 8192 - 8 (INT64) - 8
    // (overhead) = 8176 characters.
    String matchPkStr = StringUtils.repeat("a", 8176);
    String mismatchPkStr = StringUtils.repeat("b", 8176);

    List<GenericRecord> records =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 1L)
                .set("event_id", matchPkStr)
                .set("full_name", "matched")
                .build(),
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null)
                .set("user_id", 2L)
                .set("event_id", mismatchPkStr)
                .set("full_name", "avro_val")
                .build()); // mismatched row
    uploadAvroFileToGcs("input/8_kib_pk.avro", tableDef.schema, records);

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to(matchPkStr)
                .set("full_name")
                .to("matched")
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(2L)
                .set("event_id")
                .to(mismatchPkStr)
                .set("full_name")
                .to("spanner_val")
                .build())); // mismatched row

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
                /* tableName= */ "Users",
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
                /* tableName= */ "Users",
                /* recordKey= */ "[user_id:2, event_id:" + mismatchPkStr + "]",
                /* mismatchType= */ "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* recordKey= */ "[user_id:2, event_id:" + mismatchPkStr + "]",
                /* mismatchType= */ "MISSING_IN_DESTINATION")));
  }
}
