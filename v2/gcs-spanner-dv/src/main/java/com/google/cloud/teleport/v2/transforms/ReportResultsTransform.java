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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.dto.MismatchedRecord;
import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

/**
 * A {@link PTransform} that takes a {@link PCollectionTuple} of {@link ComparisonRecord}s and
 * reports the results to BigQuery.
 */
public class ReportResultsTransform extends PTransform<PCollectionTuple, PDone> {

  private static final String TABLE_VALIDATION_STATS_TABLE = "TableValidationStats";
  private static final String MISMATCHED_RECORDS_TABLE = "MismatchedRecords";
  private static final String VALIDATION_SUMMARY_TABLE = "ValidationSummary";

  private final String bigQueryDataset;
  private final String runId;

  public ReportResultsTransform(String bigQueryDataset, String runId) {
    this.bigQueryDataset = bigQueryDataset;
    // If runId is not provided, we should ideally handle it.
    // However, for PTransform, we might expect it to be resolved or passed from
    // options.
    // In GCSSpannerDV, we default it if null, but here we just take what's given.
    this.runId = runId;
  }

  @Override
  public PDone expand(PCollectionTuple input) {
    String resolvedRunId =
        runId != null ? runId : input.getPipeline().getOptions().getJobName() + "_" + Instant.now();

    PCollection<ComparisonRecord> matched = input.get(MATCHED_TAG);
    PCollection<ComparisonRecord> missingInSpanner = input.get(MISSING_IN_SPANNER_TAG);
    PCollection<ComparisonRecord> missingInSource = input.get(MISSING_IN_SOURCE_TAG);

    // 1. Write Mismatched Records
    PCollection<MismatchedRecord> mismatchFromSpannerMiss =
        missingInSpanner.apply(
            "TransformMissingInSpanner",
            MapElements.into(TypeDescriptor.of(MismatchedRecord.class))
                .via(
                    r ->
                        MismatchedRecord.builder()
                            .setRunId(resolvedRunId)
                            .setTableName(r.getTableName())
                            .setMismatchType("MISSING_IN_DESTINATION")
                            .setRecordKey(formatRecordKey(r.getPrimaryKeyColumns()))
                            .setSource("GCS")
                            .setHash(r.getHash())
                            .build()));

    PCollection<MismatchedRecord> mismatchFromSourceMiss =
        missingInSource.apply(
            "TransformMissingInSource",
            MapElements.into(TypeDescriptor.of(MismatchedRecord.class))
                .via(
                    r ->
                        MismatchedRecord.builder()
                            .setRunId(resolvedRunId)
                            .setTableName(r.getTableName())
                            .setMismatchType("MISSING_IN_SOURCE")
                            .setRecordKey(formatRecordKey(r.getPrimaryKeyColumns()))
                            .setSource("SPANNER")
                            .setHash(r.getHash())
                            .build()));

    PCollection<MismatchedRecord> allMismatches =
        PCollectionList.of(mismatchFromSpannerMiss)
            .and(mismatchFromSourceMiss)
            .apply("FlattenMismatches", Flatten.pCollections());

    allMismatches.apply(
        "WriteMismatchedRecords",
        BigQueryIO.<MismatchedRecord>write()
            .to(String.format("%s.%s", bigQueryDataset, MISMATCHED_RECORDS_TABLE))
            .withSchema(
                new TableSchema()
                    .setFields(
                        Lists.newArrayList(
                            new TableFieldSchema()
                                .setName("run_id")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("table_name")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("mismatch_type")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("record_key")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("source")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("hash")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("mismatched_columns")
                                .setType("STRING")
                                .setMode("NULLABLE"))))
            .withFormatFunction(
                r ->
                    new TableRow()
                        .set("run_id", r.getRunId())
                        .set("table_name", r.getTableName())
                        .set("mismatch_type", r.getMismatchType())
                        .set("record_key", r.getRecordKey())
                        .set("source", r.getSource())
                        .set("hash", r.getHash())
                        .set("mismatched_columns", r.getMismatchedColumns()))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    // 2. Aggregate Stats
    PCollection<KV<String, Long>> matchedCounts =
        matched
            .apply(
                "ExtractTableNameMatched",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMatched", Count.perElement());

    PCollection<KV<String, Long>> missingInSpannerCounts =
        missingInSpanner
            .apply(
                "ExtractTableNameMissInSpanner",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMissInSpanner", Count.perElement());

    PCollection<KV<String, Long>> missingInSourceCounts =
        missingInSource
            .apply(
                "ExtractTableNameMissInSource",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMissInSource", Count.perElement());

    final TupleTag<Long> matchedTag = new TupleTag<>();
    final TupleTag<Long> missInSpannerTag = new TupleTag<>();
    final TupleTag<Long> missInSourceTag = new TupleTag<>();

    PCollection<TableValidationStats> tableStats =
        KeyedPCollectionTuple.of(matchedTag, matchedCounts)
            .and(missInSpannerTag, missingInSpannerCounts)
            .and(missInSourceTag, missingInSourceCounts)
            .apply("CoGroupByKeyStats", CoGroupByKey.create())
            .apply(
                "ComputeTableStats",
                ParDo.of(
                    new DoFn<KV<String, CoGbkResult>, TableValidationStats>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String tableName = c.element().getKey();
                        CoGbkResult result = c.element().getValue();

                        long matched = result.getOnly(matchedTag, 0L);
                        long onlyInGcs = result.getOnly(missInSpannerTag, 0L);
                        long onlyInSpanner = result.getOnly(missInSourceTag, 0L);
                        long mismatch = onlyInGcs + onlyInSpanner;

                        String status = mismatch == 0 ? "MATCH" : "MISMATCH";
                        Instant now = Instant.now(); // Approximation for start/end in batch

                        c.output(
                            TableValidationStats.builder()
                                .setRunId(resolvedRunId)
                                .setTableName(tableName)
                                .setStatus(status)
                                .setSourceRowCount(matched + onlyInGcs)
                                .setDestinationRowCount(matched + onlyInSpanner)
                                .setMatchedRowCount(matched)
                                .setMismatchRowCount(mismatch)
                                .setStartTimestamp(now)
                                .setEndTimestamp(now)
                                .build());
                      }
                    }));

    tableStats.apply(
        "WriteTableStats",
        BigQueryIO.<TableValidationStats>write()
            .to(String.format("%s.%s", bigQueryDataset, TABLE_VALIDATION_STATS_TABLE))
            .withSchema(
                new TableSchema()
                    .setFields(
                        Lists.newArrayList(
                            new TableFieldSchema()
                                .setName("run_id")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("table_name")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("status")
                                .setType("STRING")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("source_row_count")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("destination_row_count")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("matched_row_count")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("mismatch_row_count")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("start_timestamp")
                                .setType("TIMESTAMP")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("end_timestamp")
                                .setType("TIMESTAMP")
                                .setMode("REQUIRED"))))
            .withFormatFunction(
                stats ->
                    new TableRow()
                        .set("run_id", stats.getRunId())
                        .set("table_name", stats.getTableName())
                        .set("status", stats.getStatus())
                        .set("source_row_count", stats.getSourceRowCount())
                        .set("destination_row_count", stats.getDestinationRowCount())
                        .set("matched_row_count", stats.getMatchedRowCount())
                        .set("mismatch_row_count", stats.getMismatchRowCount())
                        .set("start_timestamp", stats.getStartTimestamp().toString())
                        .set("end_timestamp", stats.getEndTimestamp().toString()))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    // 3. Validation Summary
    /**
     * Because the {@link ValidationSummary} is never materialized in the pipeline
     * and is the output of a Combine operation, beam has never seen it before and
     * is asked to output a PCollection of it. When this happens, beam looks up for the
     * coder in the coderRegistry. Now even though the DTO object has been annotated with
     * a @DefaultSchema(AutoValueSchema.class) this look up process does not seem to be bulletproof.
     * As a result, the Combine transform fails to find a coder for this object and fail.
     * Therefore, we explicitly use the SchemaRegistry to create and register the coder in the
     * coder registry so that beam can use it for encoding/decoding this object.
     * Think of {@link org.apache.beam.sdk.schemas.SchemaRegistry} as the "recipe" and the
     * {@link CoderRegistry} as the "product". The recipe is always registered, but the
     * "automated" product creation may fail in certain scenarios such as this.
     */
    try {
      SchemaCoder<ValidationSummary> validationSummaryCoder =
          input.getPipeline().getSchemaRegistry().getSchemaCoder(ValidationSummary.class);
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      coderRegistry.registerCoderForClass(ValidationSummary.class, validationSummaryCoder);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Unable to retrieve SchemaCoder for ValidationSummary", e);
    }

    tableStats
        .apply("WindowGlobal", Window.into(new GlobalWindows()))
        .apply(
            "CombineSummary",
            Combine.globally(new ValidationSummaryCombineFn(resolvedRunId))
                .withoutDefaults()) // Only output if there is data
        .apply(
            "WriteValidationSummary",
            BigQueryIO.<ValidationSummary>write()
                .to(String.format("%s.%s", bigQueryDataset, VALIDATION_SUMMARY_TABLE))
                .withSchema(
                    new TableSchema()
                        .setFields(
                            Lists.newArrayList(
                                new TableFieldSchema()
                                    .setName("run_id")
                                    .setType("STRING")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("source_database")
                                    .setType("STRING")
                                    .setMode("NULLABLE"),
                                new TableFieldSchema()
                                    .setName("destination_database")
                                    .setType("STRING")
                                    .setMode("NULLABLE"),
                                new TableFieldSchema()
                                    .setName("status")
                                    .setType("STRING")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("total_tables_validated")
                                    .setType("INTEGER")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("tables_with_mismatches")
                                    .setType("STRING")
                                    .setMode("NULLABLE"),
                                new TableFieldSchema()
                                    .setName("total_rows_matched")
                                    .setType("INTEGER")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("total_rows_mismatched")
                                    .setType("INTEGER")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("start_timestamp")
                                    .setType("TIMESTAMP")
                                    .setMode("REQUIRED"),
                                new TableFieldSchema()
                                    .setName("end_timestamp")
                                    .setType("TIMESTAMP")
                                    .setMode("REQUIRED"))))
                .withFormatFunction(
                    s ->
                        new TableRow()
                            .set("run_id", s.getRunId())
                            .set("source_database", s.getSourceDatabase())
                            .set("destination_database", s.getDestinationDatabase())
                            .set("status", s.getStatus())
                            .set("total_tables_validated", s.getTotalTablesValidated())
                            .set("tables_with_mismatches", s.getTablesWithMismatches())
                            .set("total_rows_matched", s.getTotalRowsMatched())
                            .set("total_rows_mismatched", s.getTotalRowsMismatched())
                            .set("start_timestamp", s.getStartTimestamp().toString())
                            .set("end_timestamp", s.getEndTimestamp().toString()))
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    return PDone.in(input.getPipeline());
  }

  private static class ValidationSummaryCombineFn
      extends Combine.CombineFn<
          TableValidationStats, ValidationSummaryAccumulator, ValidationSummary> {

    private final String runId;

    public ValidationSummaryCombineFn(String runId) {
      this.runId = runId;
    }

    @Override
    public ValidationSummaryAccumulator createAccumulator() {
      return new ValidationSummaryAccumulator();
    }

    @Override
    public ValidationSummaryAccumulator addInput(
        ValidationSummaryAccumulator accumulator, TableValidationStats input) {
      accumulator.totalTables++;
      accumulator.totalMatched += input.getMatchedRowCount();
      accumulator.totalMismatched += input.getMismatchRowCount();
      if (input.getMismatchRowCount() > 0) {
        accumulator.tablesWithMismatches.add(input.getTableName());
      }
      return accumulator;
    }

    @Override
    public ValidationSummaryAccumulator mergeAccumulators(
        Iterable<ValidationSummaryAccumulator> accumulators) {
      ValidationSummaryAccumulator merged = new ValidationSummaryAccumulator();
      for (ValidationSummaryAccumulator acc : accumulators) {
        merged.totalTables += acc.totalTables;
        merged.totalMatched += acc.totalMatched;
        merged.totalMismatched += acc.totalMismatched;
        merged.tablesWithMismatches.addAll(acc.tablesWithMismatches);
      }
      return merged;
    }

    @Override
    public ValidationSummary extractOutput(ValidationSummaryAccumulator accumulator) {
      String status = accumulator.totalMismatched == 0 ? "COMPLETED" : "FAILED";
      Instant now = Instant.now();
      return ValidationSummary.builder()
          .setRunId(runId)
          .setSourceDatabase("GCS") // Fixed as per template
          .setDestinationDatabase("Spanner") // Fixed as per template
          .setStatus(status)
          .setTotalTablesValidated(accumulator.totalTables)
          .setTablesWithMismatches(String.join(",", accumulator.tablesWithMismatches))
          .setTotalRowsMatched(accumulator.totalMatched)
          .setTotalRowsMismatched(accumulator.totalMismatched)
          .setStartTimestamp(now)
          .setEndTimestamp(now)
          .build();
    }
  }

  private static class ValidationSummaryAccumulator implements java.io.Serializable {
    long totalTables = 0;
    long totalMatched = 0;
    long totalMismatched = 0;
    List<String> tablesWithMismatches = new ArrayList<>();
  }

  private String formatRecordKey(List<com.google.cloud.teleport.v2.dto.Column> columns) {
    if (columns == null) {
      return "";
    }
    return columns.stream()
        .map(c -> c.getColName() + ":" + c.getColValue())
        .collect(Collectors.joining(", ", "[", "]"));
  }
}
