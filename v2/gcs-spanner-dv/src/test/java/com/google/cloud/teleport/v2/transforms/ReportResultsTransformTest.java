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
package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.dto.BigQuerySchemas;
import com.google.cloud.teleport.v2.dto.Column;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.dto.MismatchedRecord;
import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTimeUtils;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;




/**
 * Tests the logical units of {@link ReportResultsTransform} and its full DAG 
 * expand() method using {@link FakeBigQueryServices} and a local temporary 
 * folder to avoid external GCS and BigQuery dependencies.
 */
public class ReportResultsTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final org.junit.rules.TemporaryFolder tmp = new org.junit.rules.TemporaryFolder();

  @org.junit.Before
  public void setUp() throws NoSuchSchemaException {
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            TableValidationStats.class,
            SchemaCoder.of(
                pipeline.getSchemaRegistry().getSchema(TableValidationStats.class),
                TypeDescriptor.of(TableValidationStats.class),
                pipeline.getSchemaRegistry().getToRowFunction(TableValidationStats.class),
                pipeline.getSchemaRegistry().getFromRowFunction(TableValidationStats.class)));
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            ValidationSummary.class,
            SchemaCoder.of(
                pipeline.getSchemaRegistry().getSchema(ValidationSummary.class),
                TypeDescriptor.of(ValidationSummary.class),
                pipeline.getSchemaRegistry().getToRowFunction(ValidationSummary.class),
                pipeline.getSchemaRegistry().getFromRowFunction(ValidationSummary.class)));
  }




  @Test
  public void testExpandWithTransientErrors() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(1000000L);
    try {
        FakeDatasetService.setUp();
        FakeDatasetService fakeDatasetService = new FakeDatasetService();
        FakeJobService fakeJobService = new FakeJobService();
        fakeDatasetService.createDataset("project", "dataset", "", "", null);
        
        TableReference matchedRef = new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("ValidationSummary");
        fakeDatasetService.createTable(new Table().setTableReference(matchedRef).setSchema(BigQuerySchemas.VALIDATION_SUMMARY_SCHEMA));
        
        TableReference mismatchesRef = new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("MismatchedRecords");
        fakeDatasetService.createTable(new Table().setTableReference(mismatchesRef).setSchema(BigQuerySchemas.MISMATCHED_RECORDS_SCHEMA));
        
        TableReference statsRef = new TableReference().setProjectId("project").setDatasetId("dataset").setTableId("TableValidationStats");
        fakeDatasetService.createTable(new Table().setTableReference(statsRef).setSchema(BigQuerySchemas.TABLE_VALIDATION_STATS_SCHEMA));

        FakeBigQueryServices fakeBigQueryServices = new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService);

        Instant now = Instant.now();
        ReportResultsTransform transform = new ReportResultsTransform("project:dataset", "run1", now)
                .withTestServices(fakeBigQueryServices, tmp.newFolder("bq-temp").getAbsolutePath());

        ComparisonRecord matched = ComparisonRecord.builder().setTableName("Table1").setHash("hash1").setPrimaryKeyColumns(Collections.emptyList()).build();
        PCollection<ComparisonRecord> pMatched = pipeline.apply("CreateMatched", Create.of(matched));
        PCollection<ComparisonRecord> pMissingInSpanner = pipeline.apply("CreateMissingInSpanner", Create.empty(TypeDescriptor.of(ComparisonRecord.class)));
        PCollection<ComparisonRecord> pMissingInSource = pipeline.apply("CreateMissingInSource", Create.empty(TypeDescriptor.of(ComparisonRecord.class)));

        PCollectionTuple input = PCollectionTuple.of(MATCHED_TAG, pMatched)
                .and(MISSING_IN_SPANNER_TAG, pMissingInSpanner)
                .and(MISSING_IN_SOURCE_TAG, pMissingInSource);

        input.apply("TestTransform", transform);

        // Inject failure
        TableRow expectedSummaryRow = new TableRow()
            .set(ValidationSummary.RUN_ID_COLUMN_NAME, "run1")
            .set(ValidationSummary.SOURCE_DATABASE_COLUMN_NAME, ReportResultsTransform.GCS_SOURCE)
            .set(ValidationSummary.DESTINATION_DATABASE_COLUMN_NAME, ReportResultsTransform.SPANNER_DESTINATION)
            .set(ValidationSummary.STATUS_COLUMN_NAME, "MATCH")
            .set(ValidationSummary.TOTAL_TABLES_VALIDATED_COLUMN_NAME, 1L)
            .set(ValidationSummary.TABLES_WITH_MISMATCHES_COLUMN_NAME, "")
            .set(ValidationSummary.TOTAL_ROWS_MATCHED_COLUMN_NAME, 1L)
            .set(ValidationSummary.TOTAL_ROWS_MISMATCHED_COLUMN_NAME, 0L)
            .set(ValidationSummary.START_TIMESTAMP_COLUMN_NAME, now.toString())
            .set(ValidationSummary.END_TIMESTAMP_COLUMN_NAME, now.toString());

        InsertErrors error = new InsertErrors().setErrors(Collections.singletonList(new ErrorProto().setReason("timeout")));

        fakeDatasetService.failOnInsert(Collections.singletonMap(expectedSummaryRow, Collections.singletonList(error)));


        pipeline.run();
        
        assertEquals(1, fakeDatasetService.getAllRows("project", "dataset", "ValidationSummary").size());
        assertEquals(1, fakeDatasetService.getAllRows("project", "dataset", "TableValidationStats").size());
        assertEquals(0, fakeDatasetService.getAllRows("project", "dataset", "MismatchedRecords").size());

    } finally {
        DateTimeUtils.setCurrentMillisSystem();
    }
  }

  @Test
  public void testTransformMismatchedRecords() {
    Instant now = Instant.now();
    ReportResultsTransform transform = new ReportResultsTransform("dataset", "run1", now);

    ComparisonRecord missingInSpanner =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash1")
            .setShardId("shard1")
            .setPrimaryKeyColumns(
                Arrays.asList(Column.builder().setColName("id").setColValue("1").build()))
            .build();

    ComparisonRecord missingInSource =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash2")
            .setPrimaryKeyColumns(
                Arrays.asList(Column.builder().setColName("id").setColValue("2").build()))
            .build();

    PCollection<ComparisonRecord> pMissingInSpanner =
        pipeline.apply("CreateMissingInSpanner", Create.of(missingInSpanner));
    PCollection<ComparisonRecord> pMissingInSource =
        pipeline.apply("CreateMissingInSource", Create.of(missingInSource));

    PCollection<MismatchedRecord> output =
        transform.transformMismatchedRecords(pMissingInSpanner, pMissingInSource);

    MismatchedRecord expectedSpannerMiss =
        MismatchedRecord.builder()
            .setRunId("run1")
            .setTableName("Table1")
            .setMismatchType("MISSING_IN_DESTINATION")
            .setRecordKey("[id:1]")
            .setSource(ReportResultsTransform.GCS_SOURCE)
            .setHash("hash1")
            .setShardId("shard1")
            .build();

    MismatchedRecord expectedSourceMiss =
        MismatchedRecord.builder()
            .setRunId("run1")
            .setTableName("Table1")
            .setMismatchType("MISSING_IN_SOURCE")
            .setRecordKey("[id:2]")
            .setSource(ReportResultsTransform.SPANNER_DESTINATION)
            .setHash("hash2")
            .build();

    PAssert.that(output).containsInAnyOrder(expectedSpannerMiss, expectedSourceMiss);

    pipeline.run();
  }

  @Test
  public void testCalculateTableStats() {
    Instant now = Instant.now();
    ReportResultsTransform transform = new ReportResultsTransform("dataset", "run1", now);

    ComparisonRecord matched =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash1")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    ComparisonRecord missingInSpanner =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash2")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    ComparisonRecord missingInSource =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash3")
            .setPrimaryKeyColumns(Collections.emptyList())
            .build();

    PCollection<ComparisonRecord> pMatched = pipeline.apply("CreateMatched", Create.of(matched));
    PCollection<ComparisonRecord> pMissingInSpanner =
        pipeline.apply("CreateMissingInSpanner", Create.of(missingInSpanner));
    PCollection<ComparisonRecord> pMissingInSource =
        pipeline.apply("CreateMissingInSource", Create.of(missingInSource));

    PCollection<TableValidationStats> output =
        transform.calculateTableStats(pMatched, pMissingInSpanner, pMissingInSource);

    PAssert.that(output)
        .satisfies(
            stats -> {
              TableValidationStats stat = stats.iterator().next();
              boolean valid =
                  stat.getRunId().equals("run1")
                      && stat.getTableName().equals("Table1")
                      && stat.getStatus().equals("MISMATCH")
                      && stat.getSourceRowCount()
                          == 2 // 1 match + 1 missing in spanner (only in GCS)
                      && stat.getDestinationRowCount()
                          == 2 // 1 match + 1 missing in source (only in
                      // Spanner)
                      && stat.getMatchedRowCount() == 1
                      && stat.getMismatchRowCount() == 2;
              if (!valid) {
                throw new AssertionError("Invalid stats: " + stat);
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testCalculateValidationSummary() {
    Instant now = Instant.now();
    ReportResultsTransform transform = new ReportResultsTransform("dataset", "run1", now);

    TableValidationStats stats1 =
        TableValidationStats.builder()
            .setRunId("run1")
            .setTableName("Table1")
            .setStatus("MATCH")
            .setSourceRowCount(10L)
            .setDestinationRowCount(10L)
            .setMatchedRowCount(10L)
            .setMismatchRowCount(0L)
            .setStartTimestamp(now)
            .setEndTimestamp(now)
            .build();

    TableValidationStats stats2 =
        TableValidationStats.builder()
            .setRunId("run1")
            .setTableName("Table2")
            .setStatus("MISMATCH")
            .setSourceRowCount(5L)
            .setDestinationRowCount(6L)
            .setMatchedRowCount(4L)
            .setMismatchRowCount(3L) // 1 miss spanner + 2 miss source
            .setStartTimestamp(now)
            .setEndTimestamp(now)
            .build();

    PCollection<TableValidationStats> pStats =
        pipeline.apply("CreateStats", Create.of(stats1, stats2));

    PCollection<ValidationSummary> output = transform.calculateValidationSummary(pStats);

    PAssert.that(output)
        .satisfies(
            summaries -> {
              ValidationSummary summary = summaries.iterator().next();
              boolean valid =
                  summary.getRunId().equals("run1")
                      && summary.getSourceDatabase().equals(ReportResultsTransform.GCS_SOURCE)
                      && summary
                          .getDestinationDatabase()
                          .equals(ReportResultsTransform.SPANNER_DESTINATION)
                      && summary.getTotalTablesValidated() == 2L
                      && summary.getTablesWithMismatches().equals("Table2")
                      && summary.getTotalRowsMatched() == 14L
                      && summary.getTotalRowsMismatched() == 3L
                      && summary.getStartTimestamp().equals(now)
                      && summary.getStatus().equals("MISMATCH")
                      && !summary.getEndTimestamp().isBefore(now);
              if (!valid) {
                throw new AssertionError("Invalid summary: " + summary);
              }
              return null;
            });

    pipeline.run();
  }
}
