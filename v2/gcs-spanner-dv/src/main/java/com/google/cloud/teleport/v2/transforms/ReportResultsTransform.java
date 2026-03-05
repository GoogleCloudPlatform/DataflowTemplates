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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.dofn.ComputeTableStatsFn;
import com.google.cloud.teleport.v2.dto.BigQuerySchemas;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.dto.MismatchedRecord;
import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import com.google.cloud.teleport.v2.fn.ValidationSummaryCombineFn;
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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
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
import org.jetbrains.annotations.NotNull;
import org.joda.time.Instant;

/**
 * A {@link PTransform} that takes a {@link PCollectionTuple} of {@link ComparisonRecord}s and
 * reports the results to BigQuery.
 */
public class ReportResultsTransform extends PTransform<PCollectionTuple, PDone> {

  private static final String TABLE_VALIDATION_STATS_TABLE = "TableValidationStats";
  private static final String MISMATCHED_RECORDS_TABLE = "MismatchedRecords";
  private static final String VALIDATION_SUMMARY_TABLE = "ValidationSummary";
  public static final String GCS_SOURCE = "GCS";
  public static final String SPANNER_DESTINATION = "Spanner";

  private final String bigQueryDataset;
  private final String runId;
  private final Instant startTimestamp;

  public ReportResultsTransform(String bigQueryDataset, String runId, Instant startTimestamp) {
    this.bigQueryDataset = bigQueryDataset;
    this.runId = runId;
    this.startTimestamp = startTimestamp;
  }

  @Override
  public @NotNull PDone expand(PCollectionTuple input) {
    PCollection<ComparisonRecord> matched = input.get(MATCHED_TAG);
    PCollection<ComparisonRecord> missingInSpanner = input.get(MISSING_IN_SPANNER_TAG);
    PCollection<ComparisonRecord> missingInSource = input.get(MISSING_IN_SOURCE_TAG);

    // 1. Write Mismatched Records
    PCollection<MismatchedRecord> allMismatches =
        transformMismatchedRecords(missingInSpanner, missingInSource);

    allMismatches.apply(
        "WriteMismatchedRecords",
        BigQueryIO.<MismatchedRecord>write()
            .to(String.format("%s.%s", bigQueryDataset, MISMATCHED_RECORDS_TABLE))
            .withSchema(BigQuerySchemas.MISMATCHED_RECORDS_SCHEMA)
            .withFormatFunction(
                r ->
                    new TableRow()
                        .set(MismatchedRecord.RUN_ID_COLUMN_NAME, r.getRunId())
                        .set(MismatchedRecord.TABLE_NAME_COLUMN_NAME, r.getTableName())
                        .set(MismatchedRecord.MISMATCH_TYPE_COLUMN_NAME, r.getMismatchType())
                        .set(MismatchedRecord.RECORD_KEY_COLUMN_NAME, r.getRecordKey())
                        .set(MismatchedRecord.SOURCE_COLUMN_NAME, r.getSource())
                        .set(MismatchedRecord.HASH_COLUMN_NAME, r.getHash()))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    // 2. Aggregate Stats
    PCollection<TableValidationStats> tableStats =
        calculateTableStats(matched, missingInSpanner, missingInSource);

    tableStats.apply(
        "WriteTableStats",
        BigQueryIO.<TableValidationStats>write()
            .to(String.format("%s.%s", bigQueryDataset, TABLE_VALIDATION_STATS_TABLE))
            .withSchema(BigQuerySchemas.TABLE_VALIDATION_STATS_SCHEMA)
            .withFormatFunction(
                stats ->
                    new TableRow()
                        .set(TableValidationStats.RUN_ID_COLUMN_NAME, stats.getRunId())
                        .set(TableValidationStats.TABLE_NAME_COLUMN_NAME, stats.getTableName())
                        .set(TableValidationStats.STATUS_COLUMN_NAME, stats.getStatus())
                        .set(
                            TableValidationStats.SOURCE_ROW_COUNT_COLUMN_NAME,
                            stats.getSourceRowCount())
                        .set(
                            TableValidationStats.DESTINATION_ROW_COUNT_COLUMN_NAME,
                            stats.getDestinationRowCount())
                        .set(
                            TableValidationStats.MATCHED_ROW_COUNT_COLUMN_NAME,
                            stats.getMatchedRowCount())
                        .set(
                            TableValidationStats.MISMATCH_ROW_COUNT_COLUMN_NAME,
                            stats.getMismatchRowCount())
                        .set(
                            TableValidationStats.START_TIMESTAMP_COLUMN_NAME,
                            stats.getStartTimestamp().toString())
                        .set(
                            TableValidationStats.END_TIMESTAMP_COLUMN_NAME,
                            stats.getEndTimestamp().toString()))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

    // 3. Validation Summary
    /**
     * Because the {@link ValidationSummary} is never materialized in the pipeline and is the output
     * of a Combine operation, beam has never seen it before and is asked to output a PCollection of
     * it. When this happens, beam looks up for the coder in the coderRegistry. Now even though the
     * DTO object has been annotated with a @DefaultSchema(AutoValueSchema.class) this look up
     * process does not seem to be bulletproof. As a result, the Combine transform fails to find a
     * coder for this object and fail. Therefore, we explicitly use the SchemaRegistry to create and
     * register the coder in the coder registry so that beam can use it for encoding/decoding this
     * object. Think of {@link org.apache.beam.sdk.schemas.SchemaRegistry} as the "recipe" and the
     * {@link CoderRegistry} as the "product". The recipe is always registered, but the "automated"
     * product creation may fail in certain scenarios such as this.
     */
    try {
      SchemaCoder<ValidationSummary> validationSummaryCoder =
          input.getPipeline().getSchemaRegistry().getSchemaCoder(ValidationSummary.class);
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      coderRegistry.registerCoderForClass(ValidationSummary.class, validationSummaryCoder);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Unable to retrieve SchemaCoder for ValidationSummary", e);
    }

    PCollection<ValidationSummary> validationSummary = calculateValidationSummary(tableStats);

    validationSummary.apply(
        "WriteValidationSummary",
        BigQueryIO.<ValidationSummary>write()
            .to(String.format("%s.%s", this.bigQueryDataset, VALIDATION_SUMMARY_TABLE))
            .withSchema(BigQuerySchemas.VALIDATION_SUMMARY_SCHEMA)
            .withFormatFunction(
                s ->
                    new TableRow()
                        .set(ValidationSummary.RUN_ID_COLUMN_NAME, s.getRunId())
                        .set(ValidationSummary.SOURCE_DATABASE_COLUMN_NAME, s.getSourceDatabase())
                        .set(
                            ValidationSummary.DESTINATION_DATABASE_COLUMN_NAME,
                            s.getDestinationDatabase())
                        .set(ValidationSummary.STATUS_COLUMN_NAME, s.getStatus())
                        .set(
                            ValidationSummary.TOTAL_TABLES_VALIDATED_COLUMN_NAME,
                            s.getTotalTablesValidated())
                        .set(
                            ValidationSummary.TABLES_WITH_MISMATCHES_COLUMN_NAME,
                            s.getTablesWithMismatches())
                        .set(
                            ValidationSummary.TOTAL_ROWS_MATCHED_COLUMN_NAME,
                            s.getTotalRowsMatched())
                        .set(
                            ValidationSummary.TOTAL_ROWS_MISMATCHED_COLUMN_NAME,
                            s.getTotalRowsMismatched())
                        .set(
                            ValidationSummary.START_TIMESTAMP_COLUMN_NAME,
                            s.getStartTimestamp().toString())
                        .set(
                            ValidationSummary.END_TIMESTAMP_COLUMN_NAME,
                            s.getEndTimestamp().toString()))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

    return PDone.in(input.getPipeline());
  }

  PCollection<MismatchedRecord> transformMismatchedRecords(
      PCollection<ComparisonRecord> missingInSpanner,
      PCollection<ComparisonRecord> missingInSource) {
    PCollection<MismatchedRecord> spannerMismatchedRecords =
        missingInSpanner.apply(
            "ConvertToMismatchedRecordsFromSpanner",
            MapElements.into(TypeDescriptor.of(MismatchedRecord.class))
                .via(
                    r ->
                        MismatchedRecord.builder()
                            .setRunId(this.runId)
                            .setTableName(r.getTableName())
                            .setMismatchType("MISSING_IN_DESTINATION")
                            .setRecordKey(formatRecordKey(r.getPrimaryKeyColumns()))
                            .setSource(GCS_SOURCE)
                            .setHash(r.getHash())
                            .build()));

    PCollection<MismatchedRecord> sourceMismatchedRecords =
        missingInSource.apply(
            "ConvertToMismatchedRecordsFromSource",
            MapElements.into(TypeDescriptor.of(MismatchedRecord.class))
                .via(
                    r ->
                        MismatchedRecord.builder()
                            .setRunId(this.runId)
                            .setTableName(r.getTableName())
                            .setMismatchType("MISSING_IN_SOURCE")
                            .setRecordKey(formatRecordKey(r.getPrimaryKeyColumns()))
                            .setSource(SPANNER_DESTINATION)
                            .setHash(r.getHash())
                            .build()));

    return PCollectionList.of(spannerMismatchedRecords)
        .and(sourceMismatchedRecords)
        .apply("FlattenMismatches", Flatten.pCollections());
  }

  PCollection<TableValidationStats> calculateTableStats(
      PCollection<ComparisonRecord> matched,
      PCollection<ComparisonRecord> missingInSpanner,
      PCollection<ComparisonRecord> missingInSource) {
    PCollection<KV<String, Long>> matchedCounts =
        matched
            .apply(
                "ExtractMatchedTableNames",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMatched", Count.perElement());

    PCollection<KV<String, Long>> missingInSpannerCounts =
        missingInSpanner
            .apply(
                "ExtractTableNamesMissedInSpanner",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMissInSpanner", Count.perElement());

    PCollection<KV<String, Long>> missingInSourceCounts =
        missingInSource
            .apply(
                "ExtractTableNameMissedInSource",
                MapElements.into(TypeDescriptors.strings()).via(ComparisonRecord::getTableName))
            .apply("CountMissInSource", Count.perElement());

    final TupleTag<Long> matchedTag = new TupleTag<>();
    final TupleTag<Long> missInSpannerTag = new TupleTag<>();
    final TupleTag<Long> missInSourceTag = new TupleTag<>();

    return KeyedPCollectionTuple.of(matchedTag, matchedCounts)
        .and(missInSpannerTag, missingInSpannerCounts)
        .and(missInSourceTag, missingInSourceCounts)
        .apply("CoGroupByKeyStats", CoGroupByKey.create())
        .apply(
            "ComputeTableStats",
            ParDo.of(
                new ComputeTableStatsFn(
                    runId, startTimestamp, matchedTag, missInSpannerTag, missInSourceTag)));
  }

  PCollection<ValidationSummary> calculateValidationSummary(
      PCollection<TableValidationStats> tableStats) {
    return tableStats
        .apply("WindowGlobal", Window.into(new GlobalWindows()))
        .apply(
            "CombineSummary",
            Combine.globally(
                    new ValidationSummaryCombineFn(
                        this.runId, this.startTimestamp, GCS_SOURCE, SPANNER_DESTINATION))
                .withoutDefaults());
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
