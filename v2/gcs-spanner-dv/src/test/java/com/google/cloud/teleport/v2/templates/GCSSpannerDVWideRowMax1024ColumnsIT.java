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
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for GCSSpannerDV validating a wide row with 1024 columns.
 *
 * <p>Simulation Methodology: Two records are generated. The first uses a 1024-column row that is
 * identical in both Avro and Spanner (MATCH). The second uses the same PK but with differing data
 * in the 1024th column, resulting in 1 MISMATCHED_PAYLOAD record.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVWideRowMax1024ColumnsIT extends GCSSpannerDVITBase {

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    spannerResourceManager.executeDdlStatement(generate1024ColumnsDdl());
  }

  @Test
  public void test1024Columns() throws Exception {

    GCSSpannerDVAvroSetupHelper.TableDef tableDef =
        new GCSSpannerDVAvroSetupHelper.TableDef(
            generate1024ColumnsSchema(), "Max1024ColumnsTable", List.of("col_1"));

    GCSSpannerDVAvroSetupHelper.RecordBuilder matchedAvro =
        new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null);
    GCSSpannerDVAvroSetupHelper.RecordBuilder mismatchedAvro =
        new GCSSpannerDVAvroSetupHelper.RecordBuilder(tableDef, null);

    Mutation.WriteBuilder matchedSpanner = Mutation.newInsertOrUpdateBuilder("Max1024ColumnsTable");
    Mutation.WriteBuilder mismatchedSpanner =
        Mutation.newInsertOrUpdateBuilder("Max1024ColumnsTable");

    matchedAvro.set("col_1", 1L);
    matchedSpanner.set("col_1").to(1L);

    mismatchedAvro.set("col_1", 2L);
    mismatchedSpanner.set("col_1").to(2L); // 2nd row data simulates MISMATCH scenario for col_1024

    for (int i = 2; i <= 1024; i++) {
      String columnName = "col_" + i;

      // The first row is identical in both Avro and Spanner
      matchedAvro.set(columnName, "data");
      matchedSpanner.set(columnName).to("data");

      // The second row diverges at the very last column
      boolean isLastColumn = (i == 1024);
      mismatchedAvro.set(columnName, isLastColumn ? "avro_data" : "data");
      mismatchedSpanner.set(columnName).to(isLastColumn ? "spanner_data" : "data");
    }

    List<GenericRecord> records = List.of(matchedAvro.build(), mismatchedAvro.build());
    uploadAvroFileToGcs("input/1024_columns.avro", tableDef.schema, records);
    spannerResourceManager.write(List.of(matchedSpanner.build(), mismatchedSpanner.build()));

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
                /* tableName= */ "Max1024ColumnsTable",
                /* status= */ "MISMATCH",
                /* sourceRowCount= */ 2L,
                /* destinationRowCount= */ 2L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 2L)));

    // Note: gcs-spanner-dv currently lacks a MISMATCHED_VALUE category.
    // Differing row values are emitted as two discrepancies: one MISSING_IN_SOURCE and one
    // MISSING_IN_DESTINATION.
    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager,
        List.of(
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max1024ColumnsTable",
                /* recordKey= */ "[col_1:2]",
                /* mismatchType= */ "MISSING_IN_SOURCE"),
            new MismatchedRecordDto(
                /* shardId= */ null,
                /* schemaName= */ null,
                /* tableName= */ "Max1024ColumnsTable",
                /* recordKey= */ "[col_1:2]",
                /* mismatchType= */ "MISSING_IN_DESTINATION")));
  }

  private String generate1024ColumnsDdl() {
    StringBuilder ddl = new StringBuilder("CREATE TABLE Max1024ColumnsTable (");
    ddl.append("col_1 INT64, ");
    for (int i = 2; i <= 1024; i++) {
      ddl.append("col_").append(i).append(" STRING(20)");
      if (i < 1024) {
        ddl.append(", ");
      }
    }
    ddl.append(") PRIMARY KEY(col_1)");
    return ddl.toString();
  }

  private Schema generate1024ColumnsSchema() {
    Schema nullableString =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));

    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record("payload")
            .fields()
            .name("col_1")
            .type()
            .unionOf()
            .nullType()
            .and()
            .longType()
            .endUnion()
            .nullDefault();

    for (int i = 2; i <= 1024; i++) {
      assembler = assembler.name("col_" + i).type(nullableString).withDefault(null);
    }
    Schema payloadSchema = assembler.endRecord();

    return SchemaBuilder.record("SourceRowWithMetadata")
        .fields()
        .name("tableName")
        .type()
        .stringType()
        .noDefault()
        .name("shardId")
        .type()
        .unionOf()
        .stringType()
        .and()
        .nullType()
        .endUnion()
        .noDefault()
        .name("primaryKeys")
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .name("payload")
        .type(payloadSchema)
        .noDefault()
        .endRecord();
  }
}
