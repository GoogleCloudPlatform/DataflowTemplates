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
package com.google.cloud.teleport.v2.mongodb.validator.transforms;

import com.google.cloud.teleport.v2.mongodb.validator.dofn.RecheckMismatchesFn;
import com.google.cloud.teleport.v2.mongodb.validator.dto.BigQuerySchemas;
import com.google.cloud.teleport.v2.mongodb.validator.dto.CollectionValidationStats;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.SliceStats;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSummary;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Aggregates slice counters, recheck resolutions, and confirmed discrepancies into per-collection
 * and run-wide summaries, writing JSONL/JSON artifacts to GCS and optionally appending to BigQuery
 * tables.
 */
public class ValidationReportTransform extends PTransform<PCollection<SliceStats>, PDone> {

  private static final TupleTag<SliceStats> STATS_TAG = new TupleTag<SliceStats>() {};
  private static final TupleTag<Long> RESOLVED_TAG = new TupleTag<Long>() {};
  private static final TupleTag<MismatchedDocument> MISMATCH_TAG =
      new TupleTag<MismatchedDocument>() {};

  private final String runId;
  private final String sourceDb;
  private final String targetDb;
  private final String gcsOutputPath;
  private final String bigQueryDataset;
  private final String startTimestampIso;
  private final Map<String, String> allCollectionPairs;
  private final PCollection<KV<String, Long>> resolvedMatchCounts;
  private final PCollection<MismatchedDocument> confirmedMismatches;

  public ValidationReportTransform(
      String runId,
      String sourceDb,
      String targetDb,
      String gcsOutputPath,
      String bigQueryDataset,
      String startTimestampIso,
      Map<String, String> allCollectionPairs,
      PCollection<KV<String, Long>> resolvedMatchCounts,
      PCollection<MismatchedDocument> confirmedMismatches) {
    this.runId = Objects.requireNonNull(runId, "runId");
    this.sourceDb = Objects.requireNonNull(sourceDb, "sourceDb");
    this.targetDb = Objects.requireNonNull(targetDb, "targetDb");
    this.gcsOutputPath = Objects.requireNonNull(gcsOutputPath, "gcsOutputPath");
    this.bigQueryDataset = bigQueryDataset == null ? "" : bigQueryDataset.trim();
    this.startTimestampIso = Objects.requireNonNull(startTimestampIso, "startTimestampIso");
    this.allCollectionPairs = Objects.requireNonNull(allCollectionPairs, "allCollectionPairs");
    this.resolvedMatchCounts = Objects.requireNonNull(resolvedMatchCounts, "resolvedMatchCounts");
    this.confirmedMismatches = Objects.requireNonNull(confirmedMismatches, "confirmedMismatches");
  }

  @Override
  public PDone expand(PCollection<SliceStats> sliceStats) {
    String baseDir = trimTrailingSlash(gcsOutputPath) + "/" + runId;

    // 1. Seed zero-stat entries for all discovered collection pairs so missing/extra collections
    // with 0 slices still appear in CollectionValidationStats.
    List<SliceStats> seeds = new ArrayList<>();
    for (Map.Entry<String, String> pair : allCollectionPairs.entrySet()) {
      seeds.add(new SliceStats(pair.getKey(), pair.getValue(), "seed", 0L, 0L, 0L));
    }
    PCollection<SliceStats> allSliceStats;
    if (seeds.isEmpty()) {
      allSliceStats = sliceStats;
    } else {
      PCollection<SliceStats> seedStats =
          sliceStats
              .getPipeline()
              .apply(
                  "SeedCollectionPairs",
                  Create.of(seeds).withCoder(SerializableCoder.of(SliceStats.class)));
      allSliceStats =
          PCollectionList.of(seedStats)
              .and(sliceStats)
              .apply("MergeSeedStats", Flatten.pCollections());
    }

    PCollection<KV<String, SliceStats>> keyedStats =
        allSliceStats
            .apply(
                "KeySliceStats",
                WithKeys.of(SliceStats::getCollectionPairKey)
                    .withKeyType(TypeDescriptors.strings()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(SliceStats.class)));

    PCollection<KV<String, MismatchedDocument>> keyedMismatches =
        confirmedMismatches
            .apply(
                "KeyMismatches",
                WithKeys.of(
                        (MismatchedDocument m) ->
                            RecheckMismatchesFn.pairKey(
                                m.getSourceCollection(), m.getTargetCollection()))
                    .withKeyType(TypeDescriptors.strings()))
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(MismatchedDocument.class)));

    PCollection<KV<String, Long>> typedResolved =
        resolvedMatchCounts.setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()));

    PCollection<CollectionValidationStats> collectionStats =
        KeyedPCollectionTuple.of(STATS_TAG, keyedStats)
            .and(RESOLVED_TAG, typedResolved)
            .and(MISMATCH_TAG, keyedMismatches)
            .apply("JoinCollectionMetrics", CoGroupByKey.create())
            .apply(
                "BuildCollectionStats",
                ParDo.of(
                    new BuildCollectionStatsFn(
                        runId,
                        sourceDb,
                        targetDb,
                        startTimestampIso,
                        STATS_TAG,
                        RESOLVED_TAG,
                        MISMATCH_TAG)))
            .setCoder(SerializableCoder.of(CollectionValidationStats.class));

    PCollection<ValidationSummary> summary =
        collectionStats
            .apply(
                "CombineValidationSummary",
                Combine.globally(
                    new SummaryCombineFn(runId, sourceDb, targetDb, startTimestampIso)))
            .setCoder(SerializableCoder.of(ValidationSummary.class));

    // 2. Write GCS JSONL / JSON Reports
    confirmedMismatches
        .apply(
            "FormatMismatchJsonl",
            MapElements.into(TypeDescriptors.strings()).via(MismatchedDocument::toJsonLine))
        .apply(
            "WriteGcsMismatches", TextIO.write().to(baseDir + "/mismatches").withSuffix(".jsonl"));

    collectionStats
        .apply(
            "FormatCollectionStatsJsonl",
            MapElements.into(TypeDescriptors.strings()).via(CollectionValidationStats::toJsonLine))
        .apply(
            "WriteGcsCollectionStats",
            TextIO.write()
                .to(baseDir + "/collection_stats")
                .withSuffix(".jsonl")
                .withoutSharding());

    summary
        .apply(
            "FormatSummaryJson",
            MapElements.into(TypeDescriptors.strings()).via(ValidationSummary::toPrettyJson))
        .apply(
            "WriteGcsSummary",
            TextIO.write()
                .to(baseDir + "/validation_summary")
                .withSuffix(".json")
                .withoutSharding());

    // 3. Optional BigQuery Reporting
    if (!bigQueryDataset.isEmpty()) {
      String tablePrefix = bigQueryDataset + ".";
      confirmedMismatches.apply(
          "WriteBqMismatches",
          BigQueryIO.<MismatchedDocument>write()
              .to(tablePrefix + BigQuerySchemas.MISMATCHED_RECORDS_TABLE)
              .withSchema(BigQuerySchemas.MISMATCHED_RECORDS_SCHEMA)
              .withFormatFunction(MismatchedDocument::toTableRow)
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

      collectionStats.apply(
          "WriteBqCollectionStats",
          BigQueryIO.<CollectionValidationStats>write()
              .to(tablePrefix + BigQuerySchemas.COLLECTION_VALIDATION_STATS_TABLE)
              .withSchema(BigQuerySchemas.COLLECTION_VALIDATION_STATS_SCHEMA)
              .withFormatFunction(CollectionValidationStats::toTableRow)
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

      summary.apply(
          "WriteBqValidationSummary",
          BigQueryIO.<ValidationSummary>write()
              .to(tablePrefix + BigQuerySchemas.VALIDATION_SUMMARY_TABLE)
              .withSchema(BigQuerySchemas.VALIDATION_SUMMARY_SCHEMA)
              .withFormatFunction(ValidationSummary::toTableRow)
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
    }

    return PDone.in(sliceStats.getPipeline());
  }

  private static String trimTrailingSlash(String path) {
    String out = path;
    while (out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  private static final class BuildCollectionStatsFn
      extends DoFn<KV<String, CoGbkResult>, CollectionValidationStats> {

    private final String runId;
    private final String sourceDb;
    private final String targetDb;
    private final String startTimestampIso;
    private final TupleTag<SliceStats> statsTag;
    private final TupleTag<Long> resolvedTag;
    private final TupleTag<MismatchedDocument> mismatchTag;

    BuildCollectionStatsFn(
        String runId,
        String sourceDb,
        String targetDb,
        String startTimestampIso,
        TupleTag<SliceStats> statsTag,
        TupleTag<Long> resolvedTag,
        TupleTag<MismatchedDocument> mismatchTag) {
      this.runId = runId;
      this.sourceDb = sourceDb;
      this.targetDb = targetDb;
      this.startTimestampIso = startTimestampIso;
      this.statsTag = statsTag;
      this.resolvedTag = resolvedTag;
      this.mismatchTag = mismatchTag;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, CoGbkResult> kv, OutputReceiver<CollectionValidationStats> out) {
      String[] pair = kv.getKey().split("\u0000", 2);
      String srcCol = pair[0];
      String tgtCol = pair.length > 1 ? pair[1] : pair[0];

      long srcDocs = 0L;
      long tgtDocs = 0L;
      long immediateMatched = 0L;
      for (SliceStats s : kv.getValue().getAll(statsTag)) {
        srcDocs += s.getSourceDocsScanned();
        tgtDocs += s.getTargetDocsScanned();
        immediateMatched += s.getImmediateMatchedDocs();
      }

      long resolvedRecheck = 0L;
      for (Long r : kv.getValue().getAll(resolvedTag)) {
        if (r != null) {
          resolvedRecheck += r;
        }
      }

      long docMismatches = 0L;
      long indexMismatches = 0L;
      for (MismatchedDocument m : kv.getValue().getAll(mismatchTag)) {
        if (m.getMismatchType().isIndexLevel()) {
          indexMismatches++;
        } else {
          docMismatches++;
        }
      }

      long totalMatched = immediateMatched + resolvedRecheck;
      String status =
          (docMismatches == 0L && indexMismatches == 0L)
              ? CollectionValidationStats.STATUS_MATCH
              : CollectionValidationStats.STATUS_MISMATCH;

      out.output(
          new CollectionValidationStats(
              runId,
              sourceDb,
              targetDb,
              srcCol,
              tgtCol,
              status,
              srcDocs,
              tgtDocs,
              totalMatched,
              docMismatches,
              resolvedRecheck,
              indexMismatches,
              startTimestampIso,
              Instant.now().toString()));
    }
  }

  private static final class SummaryCombineFn
      extends Combine.CombineFn<
          CollectionValidationStats, List<CollectionValidationStats>, ValidationSummary> {

    private final String runId;
    private final String sourceDb;
    private final String targetDb;
    private final String startTimestampIso;

    SummaryCombineFn(String runId, String sourceDb, String targetDb, String startTimestampIso) {
      this.runId = runId;
      this.sourceDb = sourceDb;
      this.targetDb = targetDb;
      this.startTimestampIso = startTimestampIso;
    }

    @Override
    public List<CollectionValidationStats> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<CollectionValidationStats> addInput(
        List<CollectionValidationStats> acc, CollectionValidationStats input) {
      acc.add(input);
      return acc;
    }

    @Override
    public List<CollectionValidationStats> mergeAccumulators(
        Iterable<List<CollectionValidationStats>> accumulators) {
      List<CollectionValidationStats> merged = new ArrayList<>();
      for (List<CollectionValidationStats> acc : accumulators) {
        merged.addAll(acc);
      }
      return merged;
    }

    @Override
    public ValidationSummary extractOutput(List<CollectionValidationStats> all) {
      all.sort(Comparator.comparing(CollectionValidationStats::getSourceCollection));
      long totalCols = all.size();
      long matchedCols = 0L;
      long mismatchedCols = 0L;
      long totalSrc = 0L;
      long totalTgt = 0L;
      long totalMatched = 0L;
      long totalMismatched = 0L;
      long totalResolved = 0L;
      long totalIndexMismatches = 0L;

      for (CollectionValidationStats c : all) {
        if (CollectionValidationStats.STATUS_MATCH.equals(c.getStatus())) {
          matchedCols++;
        } else {
          mismatchedCols++;
        }
        totalSrc += c.getSourceDocCount();
        totalTgt += c.getTargetDocCount();
        totalMatched += c.getMatchedDocCount();
        totalMismatched += c.getMismatchedDocCount();
        totalResolved += c.getResolvedOnRecheckCount();
        totalIndexMismatches += c.getIndexMismatchCount();
      }

      String overallStatus =
          mismatchedCols == 0L
              ? CollectionValidationStats.STATUS_MATCH
              : CollectionValidationStats.STATUS_MISMATCH;

      return new ValidationSummary(
          runId,
          sourceDb,
          targetDb,
          overallStatus,
          totalCols,
          matchedCols,
          mismatchedCols,
          totalSrc,
          totalTgt,
          totalMatched,
          totalMismatched,
          totalResolved,
          totalIndexMismatches,
          startTimestampIso,
          Instant.now().toString());
    }
  }
}
