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
package com.google.cloud.teleport.v2.mongodb.validator.dofn;

import com.google.cloud.teleport.v2.mongodb.validator.comparison.BsonDocumentComparator;
import com.google.cloud.teleport.v2.mongodb.validator.comparison.BsonOrderComparator;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.SliceReaderFactory.CloseableDocIterator;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchType;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.SliceStats;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice.SliceMode;
import com.google.cloud.teleport.v2.mongodb.validator.util.BsonEncodingUtils;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes zero-shuffle lockstep sorted merge-join (or byte-bounded bidirectional batch lookup for
 * heterogeneous {@code other} types) over a single {@link ValidationSlice}, counting matched
 * documents in-memory and emitting only candidate discrepancies.
 */
public class ComparePartitionSliceFn extends DoFn<ValidationSlice, SliceStats> {

  private static final Logger LOG = LoggerFactory.getLogger(ComparePartitionSliceFn.class);
  private static final int MAX_CURSOR_RESUMES = 5;

  public static final TupleTag<SliceStats> SLICE_STATS_TAG = new TupleTag<SliceStats>() {};
  public static final TupleTag<MismatchedDocument> CANDIDATE_MISMATCH_TAG =
      new TupleTag<MismatchedDocument>() {};

  private final Counter sourceDocsRead =
      Metrics.counter(ComparePartitionSliceFn.class, "source_docs_read");
  private final Counter targetDocsRead =
      Metrics.counter(ComparePartitionSliceFn.class, "target_docs_read");
  private final Counter immediateMatched =
      Metrics.counter(ComparePartitionSliceFn.class, "immediate_matched_docs");
  private final Counter candidateMismatches =
      Metrics.counter(ComparePartitionSliceFn.class, "candidate_mismatches");
  private final Counter cursorResumes =
      Metrics.counter(ComparePartitionSliceFn.class, "cursor_resumes");

  private final String runId;
  private final String sourceDb;
  private final String targetDb;
  private final int batchSize;
  private final int maxBatchBytes;
  private final BsonDocumentComparator comparator;
  private final SliceReaderFactory readerFactory;
  private final long resumeBackoffBaseMs;

  public ComparePartitionSliceFn(
      String runId,
      String sourceDb,
      String targetDb,
      int batchSize,
      int maxBatchBytes,
      BsonDocumentComparator.Config comparatorConfig,
      SliceReaderFactory readerFactory) {
    this(
        runId, sourceDb, targetDb, batchSize, maxBatchBytes, comparatorConfig, readerFactory, 200L);
  }

  public ComparePartitionSliceFn(
      String runId,
      String sourceDb,
      String targetDb,
      int batchSize,
      int maxBatchBytes,
      BsonDocumentComparator.Config comparatorConfig,
      SliceReaderFactory readerFactory,
      long resumeBackoffBaseMs) {
    this.runId = Objects.requireNonNull(runId, "runId");
    this.sourceDb = Objects.requireNonNull(sourceDb, "sourceDb");
    this.targetDb = Objects.requireNonNull(targetDb, "targetDb");
    this.batchSize = Math.max(1, batchSize);
    this.maxBatchBytes = Math.max(4096, maxBatchBytes);
    this.comparator = new BsonDocumentComparator(comparatorConfig);
    this.readerFactory = Objects.requireNonNull(readerFactory, "readerFactory");
    this.resumeBackoffBaseMs = Math.max(0L, resumeBackoffBaseMs);
  }

  @ProcessElement
  public void processElement(@Element ValidationSlice slice, MultiOutputReceiver out) {
    BsonDocument filter = BsonDocument.parse(slice.getFilterJson());
    try {
      SliceStats stats =
          slice.getSliceMode() == SliceMode.SORTED_MERGE
              ? executeSortedMerge(slice, filter, out)
              : executeBatchLookup(slice, filter, out);
      out.get(SLICE_STATS_TAG).output(stats);
    } catch (Exception e) {
      LOG.error("Unrecoverable error validating slice {}: {}", slice, e.getMessage(), e);
      candidateMismatches.inc();
      out.get(CANDIDATE_MISMATCH_TAG)
          .output(
              new MismatchedDocument(
                  runId,
                  sourceDb,
                  targetDb,
                  slice.getSourceCollection(),
                  slice.getTargetCollection(),
                  "{\"$sliceError\":\"" + slice.getSliceId() + "\"}",
                  null,
                  MismatchType.SLICE_READ_ERROR,
                  "Unrecoverable slice read error: " + e.getMessage(),
                  slice.getSliceId(),
                  Instant.now().toString(),
                  256));
      out.get(SLICE_STATS_TAG)
          .output(
              new SliceStats(
                  slice.getSourceCollection(),
                  slice.getTargetCollection(),
                  slice.getSliceId(),
                  0L,
                  0L,
                  0L));
    }
  }

  private SliceStats executeSortedMerge(
      ValidationSlice slice, BsonDocument baseFilter, MultiOutputReceiver out) {
    long srcCount = 0L;
    long tgtCount = 0L;
    long matched = 0L;

    try (ResumableSortedStream srcStream =
            new ResumableSortedStream(false, slice.getSourceCollection(), baseFilter);
        ResumableSortedStream tgtStream =
            new ResumableSortedStream(true, slice.getTargetCollection(), baseFilter)) {

      BsonDocument srcDoc = srcStream.nextOrNull();
      if (srcDoc != null) {
        srcCount++;
      }
      BsonDocument tgtDoc = tgtStream.nextOrNull();
      if (tgtDoc != null) {
        tgtCount++;
      }

      while (srcDoc != null || tgtDoc != null) {
        if (srcDoc != null && tgtDoc == null) {
          emitCandidateMismatch(
              out,
              slice,
              srcDoc.get("_id"),
              MismatchType.MISSING_IN_TARGET,
              "Document missing in target collection '" + slice.getTargetCollection() + "'",
              BsonEncodingUtils.estimateBytes(srcDoc));
          srcDoc = srcStream.nextOrNull();
          if (srcDoc != null) {
            srcCount++;
          }
        } else if (srcDoc == null) {
          emitCandidateMismatch(
              out,
              slice,
              tgtDoc.get("_id"),
              MismatchType.EXTRA_IN_TARGET,
              "Extra document present in target collection '"
                  + slice.getTargetCollection()
                  + "' but absent in source",
              BsonEncodingUtils.estimateBytes(tgtDoc));
          tgtDoc = tgtStream.nextOrNull();
          if (tgtDoc != null) {
            tgtCount++;
          }
        } else {
          BsonValue srcId = srcDoc.get("_id");
          BsonValue tgtId = tgtDoc.get("_id");
          int cmp = BsonOrderComparator.INSTANCE.compare(srcId, tgtId);

          if (cmp < 0) {
            emitCandidateMismatch(
                out,
                slice,
                srcId,
                MismatchType.MISSING_IN_TARGET,
                "Document missing in target collection '" + slice.getTargetCollection() + "'",
                BsonEncodingUtils.estimateBytes(srcDoc));
            srcDoc = srcStream.nextOrNull();
            if (srcDoc != null) {
              srcCount++;
            }
          } else if (cmp > 0) {
            emitCandidateMismatch(
                out,
                slice,
                tgtId,
                MismatchType.EXTRA_IN_TARGET,
                "Extra document present in target collection '"
                    + slice.getTargetCollection()
                    + "' but absent in source",
                BsonEncodingUtils.estimateBytes(tgtDoc));
            tgtDoc = tgtStream.nextOrNull();
            if (tgtDoc != null) {
              tgtCount++;
            }
          } else {
            List<String> diffs = comparator.compare(srcDoc, tgtDoc);
            if (diffs.isEmpty()) {
              matched++;
            } else {
              int estBytes =
                  Math.max(
                      BsonEncodingUtils.estimateBytes(srcDoc),
                      BsonEncodingUtils.estimateBytes(tgtDoc));
              emitCandidateMismatch(
                  out,
                  slice,
                  srcId,
                  MismatchType.CONTENT_MISMATCH,
                  String.join("; ", diffs),
                  estBytes);
            }
            srcDoc = srcStream.nextOrNull();
            if (srcDoc != null) {
              srcCount++;
            }
            tgtDoc = tgtStream.nextOrNull();
            if (tgtDoc != null) {
              tgtCount++;
            }
          }
        }
      }
    }

    sourceDocsRead.inc(srcCount);
    targetDocsRead.inc(tgtCount);
    immediateMatched.inc(matched);
    return new SliceStats(
        slice.getSourceCollection(),
        slice.getTargetCollection(),
        slice.getSliceId(),
        srcCount,
        tgtCount,
        matched);
  }

  private SliceStats executeBatchLookup(
      ValidationSlice slice, BsonDocument filter, MultiOutputReceiver out) {
    long srcCount = 0L;
    long tgtCount = 0L;
    long matched = 0L;

    // 1. Forward pass: Source -> Target (content & missing-in-target check, byte-bounded batches).
    List<BsonDocument> batchDocs = new ArrayList<>(batchSize);
    List<BsonValue> batchIds = new ArrayList<>(batchSize);
    int batchBytes = 0;

    try (CloseableDocIterator srcIter =
        readerFactory.openUnorderedCursor(
            false, slice.getSourceCollection(), filter, batchSize, /* idOnlyProjection= */ false)) {
      while (srcIter.hasNext()) {
        BsonDocument sDoc = srcIter.next();
        BsonValue id = sDoc.get("_id");
        if (id == null) {
          continue;
        }
        srcCount++;
        int docBytes = BsonEncodingUtils.estimateBytes(sDoc);
        batchDocs.add(sDoc);
        batchIds.add(id);
        batchBytes += docBytes;

        if (batchDocs.size() >= batchSize || batchBytes >= maxBatchBytes) {
          matched += flushForwardLookupBatch(slice, batchDocs, batchIds, out);
          batchDocs.clear();
          batchIds.clear();
          batchBytes = 0;
        }
      }
      if (!batchDocs.isEmpty()) {
        matched += flushForwardLookupBatch(slice, batchDocs, batchIds, out);
        batchDocs.clear();
        batchIds.clear();
      }
    }

    // 2. Reverse ID-only pass: Target -> Source (catches orphaned extra target docs).
    List<BsonValue> tgtIds = new ArrayList<>(batchSize);
    try (CloseableDocIterator tgtIter =
        readerFactory.openUnorderedCursor(
            true, slice.getTargetCollection(), filter, batchSize, /* idOnlyProjection= */ true)) {
      while (tgtIter.hasNext()) {
        BsonDocument tDoc = tgtIter.next();
        BsonValue id = tDoc.get("_id");
        if (id == null) {
          continue;
        }
        tgtCount++;
        tgtIds.add(id);
        if (tgtIds.size() >= batchSize) {
          flushReverseOrphanBatch(slice, tgtIds, out);
          tgtIds.clear();
        }
      }
      if (!tgtIds.isEmpty()) {
        flushReverseOrphanBatch(slice, tgtIds, out);
      }
    }

    sourceDocsRead.inc(srcCount);
    targetDocsRead.inc(tgtCount);
    immediateMatched.inc(matched);
    return new SliceStats(
        slice.getSourceCollection(),
        slice.getTargetCollection(),
        slice.getSliceId(),
        srcCount,
        tgtCount,
        matched);
  }

  private long flushForwardLookupBatch(
      ValidationSlice slice,
      List<BsonDocument> srcDocs,
      List<BsonValue> ids,
      MultiOutputReceiver out) {
    Map<ByteBuffer, BsonDocument> tgtMap =
        readerFactory.fetchBatchByIds(
            true,
            slice.getTargetCollection(),
            ids,
            /* strongPrimaryRead= */ false,
            /* idOnlyProjection= */ false);
    long matched = 0L;
    for (BsonDocument sDoc : srcDocs) {
      BsonValue id = sDoc.get("_id");
      ByteBuffer key = ByteBuffer.wrap(BsonEncodingUtils.encodeIdBytes(id));
      BsonDocument tDoc = tgtMap.get(key);
      if (tDoc == null) {
        emitCandidateMismatch(
            out,
            slice,
            id,
            MismatchType.MISSING_IN_TARGET,
            "Document missing in target collection '" + slice.getTargetCollection() + "'",
            BsonEncodingUtils.estimateBytes(sDoc));
      } else {
        List<String> diffs = comparator.compare(sDoc, tDoc);
        if (diffs.isEmpty()) {
          matched++;
        } else {
          emitCandidateMismatch(
              out,
              slice,
              id,
              MismatchType.CONTENT_MISMATCH,
              String.join("; ", diffs),
              Math.max(
                  BsonEncodingUtils.estimateBytes(sDoc), BsonEncodingUtils.estimateBytes(tDoc)));
        }
      }
    }
    return matched;
  }

  private void flushReverseOrphanBatch(
      ValidationSlice slice, List<BsonValue> tgtIds, MultiOutputReceiver out) {
    Map<ByteBuffer, BsonDocument> srcMap =
        readerFactory.fetchBatchByIds(
            false,
            slice.getSourceCollection(),
            tgtIds,
            /* strongPrimaryRead= */ false,
            /* idOnlyProjection= */ true);
    for (BsonValue id : tgtIds) {
      ByteBuffer key = ByteBuffer.wrap(BsonEncodingUtils.encodeIdBytes(id));
      if (!srcMap.containsKey(key)) {
        emitCandidateMismatch(
            out,
            slice,
            id,
            MismatchType.EXTRA_IN_TARGET,
            "Extra document present in target collection '"
                + slice.getTargetCollection()
                + "' but absent in source",
            128);
      }
    }
  }

  private void emitCandidateMismatch(
      MultiOutputReceiver out,
      ValidationSlice slice,
      BsonValue id,
      MismatchType type,
      String diffDetails,
      int estimatedBytes) {
    candidateMismatches.inc();
    byte[] idBytes = id == null ? null : BsonEncodingUtils.encodeIdBytes(id);
    out.get(CANDIDATE_MISMATCH_TAG)
        .output(
            new MismatchedDocument(
                runId,
                sourceDb,
                targetDb,
                slice.getSourceCollection(),
                slice.getTargetCollection(),
                BsonEncodingUtils.toExtendedJson(id),
                idBytes,
                type,
                diffDetails,
                slice.getSliceId(),
                Instant.now().toString(),
                estimatedBytes));
  }

  /**
   * Sorted cursor wrapper that automatically resumes from {@code lastYieldedId} upon transient
   * socket timeouts or {@code CURSOR_NOT_FOUND} expirations.
   */
  private final class ResumableSortedStream implements AutoCloseable {
    private final boolean isTarget;
    private final String collection;
    private final BsonDocument baseFilter;
    @Nullable private CloseableDocIterator currentIter;
    @Nullable private BsonValue lastYieldedId;

    ResumableSortedStream(boolean isTarget, String collection, BsonDocument baseFilter) {
      this.isTarget = isTarget;
      this.collection = collection;
      this.baseFilter = baseFilter;
      this.currentIter =
          readerFactory.openSortedCursor(
              isTarget, collection, baseFilter, null, batchSize, /* idOnlyProjection= */ false);
    }

    @Nullable
    BsonDocument nextOrNull() {
      int attempt = 0;
      while (true) {
        try {
          if (currentIter == null) {
            currentIter =
                readerFactory.openSortedCursor(
                    isTarget,
                    collection,
                    baseFilter,
                    lastYieldedId,
                    batchSize,
                    /* idOnlyProjection= */ false);
          }
          if (!currentIter.hasNext()) {
            return null;
          }
          BsonDocument doc = currentIter.next();
          BsonValue id = doc.get("_id");
          if (id != null) {
            lastYieldedId = id;
          }
          return doc;
        } catch (RuntimeException e) {
          closeQuietly();
          attempt++;
          if (attempt > MAX_CURSOR_RESUMES) {
            throw e;
          }
          cursorResumes.inc();
          LOG.warn(
              "Transient cursor failure on {} '{}' after _id={} (attempt {}/{}): {}. Resuming...",
              isTarget ? "target" : "source",
              collection,
              BsonEncodingUtils.toExtendedJson(lastYieldedId),
              attempt,
              MAX_CURSOR_RESUMES,
              e.getMessage());
          sleepQuietly(resumeBackoffBaseMs * (1L << (attempt - 1)));
        }
      }
    }

    @Override
    public void close() {
      closeQuietly();
    }

    private void closeQuietly() {
      if (currentIter != null) {
        try {
          currentIter.close();
        } catch (Exception ignored) {
          // Ignore close errors on broken cursors.
        }
        currentIter = null;
      }
    }
  }

  private static void sleepQuietly(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
