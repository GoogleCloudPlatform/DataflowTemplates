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
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchType;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.util.BsonEncodingUtils;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Buffers candidate document mismatches into byte-and-count-bounded batches and re-fetches them
 * after a configurable delay from Source ({@code primary} + {@code majority}) and Target Firestore
 * to eliminate false positives from in-flight live-CDC replication lag or stale secondary reads.
 */
public class RecheckMismatchesFn extends DoFn<MismatchedDocument, MismatchedDocument> {

  public static final TupleTag<MismatchedDocument> CONFIRMED_MISMATCH_TAG =
      new TupleTag<MismatchedDocument>() {};
  public static final TupleTag<KV<String, Long>> RESOLVED_MATCH_COUNT_TAG =
      new TupleTag<KV<String, Long>>() {};

  private final Counter resolvedLiveDocs =
      Metrics.counter(RecheckMismatchesFn.class, "resolved_live_docs_on_recheck");
  private final Counter resolvedDeletedDocs =
      Metrics.counter(RecheckMismatchesFn.class, "resolved_deleted_docs_on_recheck");
  private final Counter confirmedMismatches =
      Metrics.counter(RecheckMismatchesFn.class, "confirmed_mismatches");

  private final int maxRetries;
  private final long baseDelayMs;
  private final int maxBatchCount;
  private final int maxBatchBytes;
  private final BsonDocumentComparator comparator;
  private final SliceReaderFactory readerFactory;

  private transient Map<String, List<MismatchedDocument>> pendingByPair;
  private transient Map<String, Integer> pendingBytesByPair;

  public RecheckMismatchesFn(
      int maxRetries,
      long baseDelayMs,
      int maxBatchCount,
      int maxBatchBytes,
      BsonDocumentComparator.Config comparatorConfig,
      SliceReaderFactory readerFactory) {
    this.maxRetries = Math.max(0, maxRetries);
    this.baseDelayMs = Math.max(0L, baseDelayMs);
    this.maxBatchCount = Math.max(1, Math.min(500, maxBatchCount));
    this.maxBatchBytes = Math.max(4096, maxBatchBytes);
    this.comparator = new BsonDocumentComparator(comparatorConfig);
    this.readerFactory = Objects.requireNonNull(readerFactory, "readerFactory");
  }

  @StartBundle
  public void startBundle() {
    pendingByPair = new LinkedHashMap<>();
    pendingBytesByPair = new LinkedHashMap<>();
  }

  @ProcessElement
  public void processElement(@Element MismatchedDocument candidate, MultiOutputReceiver out) {
    if (maxRetries == 0
        || !candidate.getMismatchType().isDocumentLevel()
        || candidate.getRawIdBsonBytes() == null) {
      confirmedMismatches.inc();
      out.get(CONFIRMED_MISMATCH_TAG).output(candidate);
      return;
    }

    String pairKey = pairKey(candidate.getSourceCollection(), candidate.getTargetCollection());
    List<MismatchedDocument> list =
        pendingByPair.computeIfAbsent(pairKey, k -> new ArrayList<>(maxBatchCount));
    list.add(candidate);
    int newBytes = pendingBytesByPair.getOrDefault(pairKey, 0) + candidate.getEstimatedBytes();
    pendingBytesByPair.put(pairKey, newBytes);

    if (list.size() >= maxBatchCount || newBytes >= maxBatchBytes) {
      flushBatch(
          pairKey, candidate.getSourceCollection(), candidate.getTargetCollection(), list, out);
      list.clear();
      pendingBytesByPair.put(pairKey, 0);
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext context) {
    for (Map.Entry<String, List<MismatchedDocument>> entry : pendingByPair.entrySet()) {
      List<MismatchedDocument> list = entry.getValue();
      if (list.isEmpty()) {
        continue;
      }
      MismatchedDocument sample = list.get(0);
      flushBatchToFinishContext(
          entry.getKey(),
          sample.getSourceCollection(),
          sample.getTargetCollection(),
          list,
          context);
    }
    pendingByPair.clear();
    pendingBytesByPair.clear();
  }

  private void flushBatch(
      String pairKey,
      String srcCol,
      String tgtCol,
      List<MismatchedDocument> batch,
      MultiOutputReceiver out) {
    RecheckOutcome outcome = executeRecheckLoop(srcCol, tgtCol, batch);
    if (outcome.resolvedLiveMatches > 0) {
      out.get(RESOLVED_MATCH_COUNT_TAG).output(KV.of(pairKey, outcome.resolvedLiveMatches));
    }
    for (MismatchedDocument m : outcome.stillMismatched) {
      confirmedMismatches.inc();
      out.get(CONFIRMED_MISMATCH_TAG).output(m);
    }
  }

  private void flushBatchToFinishContext(
      String pairKey,
      String srcCol,
      String tgtCol,
      List<MismatchedDocument> batch,
      FinishBundleContext context) {
    RecheckOutcome outcome = executeRecheckLoop(srcCol, tgtCol, batch);
    org.joda.time.Instant ts =
        org.apache.beam.sdk.transforms.windowing.GlobalWindow.TIMESTAMP_MIN_VALUE;
    org.apache.beam.sdk.transforms.windowing.GlobalWindow win =
        org.apache.beam.sdk.transforms.windowing.GlobalWindow.INSTANCE;
    if (outcome.resolvedLiveMatches > 0) {
      context.output(
          RESOLVED_MATCH_COUNT_TAG, KV.of(pairKey, outcome.resolvedLiveMatches), ts, win);
    }
    for (MismatchedDocument m : outcome.stillMismatched) {
      confirmedMismatches.inc();
      context.output(CONFIRMED_MISMATCH_TAG, m, ts, win);
    }
  }

  private RecheckOutcome executeRecheckLoop(
      String srcCol, String tgtCol, List<MismatchedDocument> initialBatch) {
    List<MismatchedDocument> unresolved = new ArrayList<>(initialBatch);
    long resolvedLive = 0L;

    for (int attempt = 1; attempt <= maxRetries && !unresolved.isEmpty(); attempt++) {
      sleepQuietly(baseDelayMs * attempt);

      List<BsonValue> ids = new ArrayList<>(unresolved.size());
      for (MismatchedDocument doc : unresolved) {
        byte[] rawId = doc.getRawIdBsonBytes();
        if (rawId != null) {
          ids.add(BsonEncodingUtils.decodeIdBytes(rawId));
        }
      }

      Map<ByteBuffer, BsonDocument> srcDocs =
          readerFactory.fetchBatchByIds(
              false, srcCol, ids, /* strongPrimaryRead= */ true, /* idOnlyProjection= */ false);
      Map<ByteBuffer, BsonDocument> tgtDocs =
          readerFactory.fetchBatchByIds(
              true, tgtCol, ids, /* strongPrimaryRead= */ true, /* idOnlyProjection= */ false);

      List<MismatchedDocument> nextUnresolved = new ArrayList<>();
      String nowIso = Instant.now().toString();

      for (MismatchedDocument prev : unresolved) {
        ByteBuffer key = ByteBuffer.wrap(prev.getRawIdBsonBytes());
        BsonDocument sDoc = srcDocs.get(key);
        BsonDocument tDoc = tgtDocs.get(key);

        if (sDoc == null && tDoc == null) {
          // Document was deleted on source and delete replicated to target.
          resolvedDeletedDocs.inc();
        } else if (sDoc != null && tDoc != null) {
          List<String> diffs = comparator.compare(sDoc, tDoc);
          if (diffs.isEmpty()) {
            resolvedLive++;
            resolvedLiveDocs.inc();
          } else {
            nextUnresolved.add(
                new MismatchedDocument(
                    prev.getRunId(),
                    prev.getSourceDatabase(),
                    prev.getTargetDatabase(),
                    prev.getSourceCollection(),
                    prev.getTargetCollection(),
                    prev.getDocumentIdExtendedJson(),
                    prev.getRawIdBsonBytes(),
                    MismatchType.CONTENT_MISMATCH,
                    String.join("; ", diffs),
                    prev.getSliceId(),
                    nowIso,
                    prev.getEstimatedBytes()));
          }
        } else if (sDoc != null) {
          nextUnresolved.add(
              new MismatchedDocument(
                  prev.getRunId(),
                  prev.getSourceDatabase(),
                  prev.getTargetDatabase(),
                  prev.getSourceCollection(),
                  prev.getTargetCollection(),
                  prev.getDocumentIdExtendedJson(),
                  prev.getRawIdBsonBytes(),
                  MismatchType.MISSING_IN_TARGET,
                  "Document still missing in target after " + attempt + " primary recheck(s)",
                  prev.getSliceId(),
                  nowIso,
                  prev.getEstimatedBytes()));
        } else {
          nextUnresolved.add(
              new MismatchedDocument(
                  prev.getRunId(),
                  prev.getSourceDatabase(),
                  prev.getTargetDatabase(),
                  prev.getSourceCollection(),
                  prev.getTargetCollection(),
                  prev.getDocumentIdExtendedJson(),
                  prev.getRawIdBsonBytes(),
                  MismatchType.EXTRA_IN_TARGET,
                  "Extra document still present in target after " + attempt + " primary recheck(s)",
                  prev.getSliceId(),
                  nowIso,
                  prev.getEstimatedBytes()));
        }
      }

      unresolved = nextUnresolved;
    }

    return new RecheckOutcome(resolvedLive, unresolved);
  }

  public static String pairKey(String srcCol, String tgtCol) {
    return srcCol + "\u0000" + tgtCol;
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

  private static final class RecheckOutcome {
    final long resolvedLiveMatches;
    final List<MismatchedDocument> stillMismatched;

    RecheckOutcome(long resolvedLiveMatches, List<MismatchedDocument> stillMismatched) {
      this.resolvedLiveMatches = resolvedLiveMatches;
      this.stillMismatched = stillMismatched;
    }
  }
}
