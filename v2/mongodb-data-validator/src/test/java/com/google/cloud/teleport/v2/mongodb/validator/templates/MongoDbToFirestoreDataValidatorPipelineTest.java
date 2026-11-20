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
package com.google.cloud.teleport.v2.mongodb.validator.templates;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.mongodb.validator.comparison.BsonOrderComparator;
import com.google.cloud.teleport.v2.mongodb.validator.dofn.SliceReaderFactory;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchType;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice.SliceMode;
import com.google.cloud.teleport.v2.mongodb.validator.options.MongoDbDataValidatorOptions;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.PlanResult;
import com.google.cloud.teleport.v2.mongodb.validator.util.BsonEncodingUtils;
import com.google.cloud.teleport.v2.mongodb.validator.util.UriSanitizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end {@link TestPipeline} tests covering sorted merge-join, simulated {@code
 * CURSOR_NOT_FOUND} cursor resume, mixed-type {@code BATCH_LOOKUP}, live-CDC primary recheck
 * convergence, and GCS JSONL/JSON output generation.
 */
@RunWith(JUnit4.class)
public class MongoDbToFirestoreDataValidatorPipelineTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void uriSanitizer_masksPasswordsCleanly() {
    assertThat(
            UriSanitizer.sanitize(
                "mongodb://u-dataflow:s3cr3tP@ss!@10.0.0.80:27017/?authSource=admin"))
        .isEqualTo("mongodb://u-dataflow:****@10.0.0.80:27017/?authSource=admin");
  }

  @Test
  public void pipeline_resumesDroppedCursor_resolvesLiveCdcLag_andReportsTrueDiscrepancies()
      throws Exception {
    File outDir = tempFolder.newFolder("dv_out");
    String runId = "test_run_e2e";

    // Source initial scan docs for "orders" (sorted by _id: 1..5):
    // - _id=1: matches immediately
    // - _id=2: matches immediately (after simulated CURSOR_NOT_FOUND drop on target!)
    // - _id=3: initially stale on target ("pending" vs "paid"), converges to "paid" on recheck!
    // - _id=4: genuine persistent content mismatch ("alice" vs "mallory")
    // - _id=5: missing in target even after recheck -> MISSING_IN_TARGET
    // Target also has _id=6 initially, which was deleted on both sides by recheck time -> resolved!
    List<BsonDocument> srcScanOrders =
        ImmutableList.of(
            BsonDocument.parse("{\"_id\": 1, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 2, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 3, \"status\": \"paid\"}"),
            BsonDocument.parse("{\"_id\": 4, \"owner\": \"alice\"}"),
            BsonDocument.parse("{\"_id\": 5, \"status\": \"only_src\"}"));

    List<BsonDocument> tgtScanOrders =
        ImmutableList.of(
            BsonDocument.parse("{\"_id\": 1, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 2, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 3, \"status\": \"pending\"}"),
            BsonDocument.parse("{\"_id\": 4, \"owner\": \"mallory\"}"),
            BsonDocument.parse("{\"_id\": 6, \"status\": \"transient_orphan\"}"));

    // Primary state at recheck time: _id=3 caught up ("paid"), _id=6 deleted on both, _id=4 & 5
    // remain genuinely mismatched.
    List<BsonDocument> srcPrimaryOrders = srcScanOrders;
    List<BsonDocument> tgtPrimaryOrders =
        ImmutableList.of(
            BsonDocument.parse("{\"_id\": 1, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 2, \"status\": \"ok\"}"),
            BsonDocument.parse("{\"_id\": 3, \"status\": \"paid\"}"),
            BsonDocument.parse("{\"_id\": 4, \"owner\": \"mallory\"}"));

    // Mixed-type _id collection ("mixed") tested via BATCH_LOOKUP:
    // Verifies string "42" and int32 42 do NOT collide in lookup maps!
    List<BsonDocument> mixedDocs =
        ImmutableList.of(
            BsonDocument.parse("{\"_id\": \"42\", \"kind\": \"str\"}"),
            BsonDocument.parse("{\"_id\": 42, \"kind\": \"int\"}"));

    InMemoryFakeReaderFactory fakeFactory =
        new InMemoryFakeReaderFactory(
            ImmutableMap.of("orders", srcScanOrders, "mixed", mixedDocs),
            ImmutableMap.of("orders", tgtScanOrders, "mixed", mixedDocs),
            ImmutableMap.of("orders", srcPrimaryOrders, "mixed", mixedDocs),
            ImmutableMap.of("orders", tgtPrimaryOrders, "mixed", mixedDocs),
            /* simulateTargetCursorDropOnce= */ true);

    List<ValidationSlice> slices =
        ImmutableList.of(
            new ValidationSlice(
                "orders", "orders", "orders#number-0000", SliceMode.SORTED_MERGE, "{}"),
            new ValidationSlice(
                "mixed", "mixed", "mixed#other-0000", SliceMode.BATCH_LOOKUP, "{}"));

    List<MismatchedDocument> catalogMismatches =
        ImmutableList.of(
            new MismatchedDocument(
                runId,
                "app_db",
                "app_db",
                "orders",
                "orders",
                "{\"$index\":{\"customerId\":1}}",
                null,
                MismatchType.MISSING_INDEX_IN_TARGET,
                "Missing secondary index customerId_1",
                "index-audit",
                "2026-11-16T00:00:00Z",
                128));

    PlanResult plan =
        new PlanResult(
            slices, catalogMismatches, ImmutableMap.of("orders", "orders", "mixed", "mixed"));

    MongoDbDataValidatorOptions options =
        PipelineOptionsFactory.as(MongoDbDataValidatorOptions.class);
    options.setSourceUri("mongodb://localhost:27017");
    options.setTargetUri("mongodb://localhost:27018");
    options.setSourceDatabase("app_db");
    options.setTargetDatabase("app_db");
    options.setGcsOutputPath(outDir.getAbsolutePath());
    options.setRunId(runId);
    options.setBatchSize(2);
    options.setMaxBatchBytes(65536);
    options.setStrictTypeChecking(true);
    options.setIgnoreSubdocumentFieldOrder(false);
    options.setValidateIndexes(true);
    options.setMismatchRecheckRetries(2);
    options.setMismatchRecheckDelayMs(1);

    MongoDbToFirestoreDataValidator.buildPipelineGraph(
        pipeline,
        options,
        runId,
        "app_db",
        "app_db",
        Instant.parse("2026-11-16T00:00:00Z"),
        plan,
        fakeFactory);

    pipeline.run().waitUntilFinish();

    Path runOutDir = outDir.toPath().resolve(runId);
    String summaryText =
        Files.readString(runOutDir.resolve("validation_summary.json"), StandardCharsets.UTF_8);
    JsonObject summary = JsonParser.parseString(summaryText).getAsJsonObject();

    assertThat(summary.get("status").getAsString()).isEqualTo("MISMATCH");
    assertThat(summary.get("total_collections").getAsLong()).isEqualTo(2L);
    assertThat(summary.get("matched_collections").getAsLong()).isEqualTo(1L); // "mixed" matched!
    assertThat(summary.get("mismatched_collections").getAsLong()).isEqualTo(1L); // "orders"
    // Matched docs: orders (_id=1, _id=2 immediate + _id=3 resolved on recheck = 3) + mixed (2) = 5
    assertThat(summary.get("total_matched_docs").getAsLong()).isEqualTo(5L);
    assertThat(summary.get("total_resolved_on_recheck_docs").getAsLong()).isEqualTo(1L);
    // Remaining doc mismatches: _id=4 (CONTENT_MISMATCH) + _id=5 (MISSING_IN_TARGET) = 2
    assertThat(summary.get("total_mismatched_docs").getAsLong()).isEqualTo(2L);
    assertThat(summary.get("total_index_mismatches").getAsLong()).isEqualTo(1L);

    List<String> mismatchLines = new ArrayList<>();
    try (Stream<Path> paths = Files.list(runOutDir)) {
      for (Path p : paths.collect(Collectors.toList())) {
        String name = p.getFileName().toString();
        if (name.startsWith("mismatches") && name.endsWith(".jsonl")) {
          mismatchLines.addAll(Files.readAllLines(p, StandardCharsets.UTF_8));
        }
      }
    }
    assertThat(mismatchLines).hasSize(3); // _id=4, _id=5, and MISSING_INDEX_IN_TARGET
  }

  /** Hermetic in-memory {@link SliceReaderFactory} for pipeline tests. */
  private static final class InMemoryFakeReaderFactory implements SliceReaderFactory {
    private static final long serialVersionUID = 1L;
    private static final AtomicBoolean DROPPED_ONCE = new AtomicBoolean(false);

    private final Map<String, List<BsonDocument>> srcScan;
    private final Map<String, List<BsonDocument>> tgtScan;
    private final Map<String, List<BsonDocument>> srcPrimary;
    private final Map<String, List<BsonDocument>> tgtPrimary;
    private final boolean simulateDrop;

    InMemoryFakeReaderFactory(
        Map<String, List<BsonDocument>> srcScan,
        Map<String, List<BsonDocument>> tgtScan,
        Map<String, List<BsonDocument>> srcPrimary,
        Map<String, List<BsonDocument>> tgtPrimary,
        boolean simulateDrop) {
      this.srcScan = srcScan;
      this.tgtScan = tgtScan;
      this.srcPrimary = srcPrimary;
      this.tgtPrimary = tgtPrimary;
      this.simulateDrop = simulateDrop;
      DROPPED_ONCE.set(false);
    }

    @Override
    public CloseableDocIterator openSortedCursor(
        boolean isTarget,
        String collection,
        BsonDocument baseFilter,
        @Nullable BsonValue resumeAfterExclusiveId,
        int batchSize,
        boolean idOnlyProjection) {
      List<BsonDocument> all =
          new ArrayList<>(
              (isTarget ? tgtScan : srcScan).getOrDefault(collection, ImmutableList.of()));
      all.sort((a, b) -> BsonOrderComparator.INSTANCE.compare(a.get("_id"), b.get("_id")));
      List<BsonDocument> filtered = new ArrayList<>();
      for (BsonDocument d : all) {
        if (resumeAfterExclusiveId == null
            || BsonOrderComparator.INSTANCE.compare(d.get("_id"), resumeAfterExclusiveId) > 0) {
          filtered.add(d);
        }
      }
      Iterator<BsonDocument> it = filtered.iterator();
      return new CloseableDocIterator() {
        private int count = 0;

        @Override
        public boolean hasNext() {
          if (simulateDrop
              && isTarget
              && "orders".equals(collection)
              && count == 1
              && DROPPED_ONCE.compareAndSet(false, true)) {
            throw new RuntimeException("Simulated CURSOR_NOT_FOUND (code 43)");
          }
          return it.hasNext();
        }

        @Override
        public BsonDocument next() {
          count++;
          return it.next();
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public CloseableDocIterator openUnorderedCursor(
        boolean isTarget,
        String collection,
        BsonDocument filter,
        int batchSize,
        boolean idOnlyProjection) {
      Iterator<BsonDocument> it =
          (isTarget ? tgtScan : srcScan).getOrDefault(collection, ImmutableList.of()).iterator();
      return new CloseableDocIterator() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public BsonDocument next() {
          return it.next();
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public Map<ByteBuffer, BsonDocument> fetchBatchByIds(
        boolean isTarget,
        String collection,
        List<BsonValue> ids,
        boolean strongPrimaryRead,
        boolean idOnlyProjection) {
      Map<String, List<BsonDocument>> store =
          strongPrimaryRead ? (isTarget ? tgtPrimary : srcPrimary) : (isTarget ? tgtScan : srcScan);
      Map<ByteBuffer, BsonDocument> indexed = new HashMap<>();
      for (BsonDocument d : store.getOrDefault(collection, ImmutableList.of())) {
        indexed.put(ByteBuffer.wrap(BsonEncodingUtils.encodeIdBytes(d.get("_id"))), d);
      }
      Map<ByteBuffer, BsonDocument> out = new HashMap<>();
      for (BsonValue id : ids) {
        ByteBuffer k = ByteBuffer.wrap(BsonEncodingUtils.encodeIdBytes(id));
        if (indexed.containsKey(k)) {
          out.put(k, indexed.get(k));
        }
      }
      return out;
    }
  }
}
