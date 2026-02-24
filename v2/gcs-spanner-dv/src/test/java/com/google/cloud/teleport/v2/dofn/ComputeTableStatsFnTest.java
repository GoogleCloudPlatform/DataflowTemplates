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
package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.dto.TableValidationStats;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class ComputeTableStatsFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testComputeTableStats_Match() {
    String runId = "test-run";
    Instant startTimestamp = Instant.now().minus(1000);
    TupleTag<Long> matchedTag = new TupleTag<>();
    TupleTag<Long> missInSpannerTag = new TupleTag<>();
    TupleTag<Long> missInSourceTag = new TupleTag<>();

    PCollection<KV<String, Long>> matched =
        pipeline.apply("CreateMatched", Create.of(KV.of("Table1", 10L)));
    PCollection<KV<String, Long>> missInSpanner =
        pipeline.apply("CreateMissSpanner", Create.empty(matched.getCoder()));
    PCollection<KV<String, Long>> missInSource =
        pipeline.apply("CreateMissSource", Create.empty(matched.getCoder()));

    PCollection<KV<String, CoGbkResult>> coGbk =
        KeyedPCollectionTuple.of(matchedTag, matched)
            .and(missInSpannerTag, missInSpanner)
            .and(missInSourceTag, missInSource)
            .apply(CoGroupByKey.create());

    PCollection<TableValidationStats> stats =
        coGbk.apply(
            ParDo.of(
                new ComputeTableStatsFn(
                    runId, startTimestamp, matchedTag, missInSpannerTag, missInSourceTag)));

    PAssert.that(stats)
        .satisfies(
            collection -> {
              TableValidationStats stat = collection.iterator().next();
              if (!stat.getRunId().equals(runId)) {
                throw new AssertionError("RunId mismatch");
              }
              if (!stat.getTableName().equals("Table1")) {
                throw new AssertionError("TableName mismatch");
              }
              if (!stat.getStatus().equals("MATCH")) {
                throw new AssertionError("Status mismatch: " + stat.getStatus());
              }
              if (stat.getMatchedRowCount() != 10L) {
                throw new AssertionError("Matched count mismatch");
              }
              if (stat.getMismatchRowCount() != 0L) {
                throw new AssertionError("Mismatch count mismatch");
              }
              if (!stat.getStartTimestamp().equals(startTimestamp)) {
                throw new AssertionError("StartTimestamp mismatch");
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testComputeTableStats_Mismatch() {
    String runId = "test-run";
    Instant startTimestamp = Instant.now().minus(1000);
    TupleTag<Long> matchedTag = new TupleTag<>();
    TupleTag<Long> missInSpannerTag = new TupleTag<>();
    TupleTag<Long> missInSourceTag = new TupleTag<>();

    // Matched=10, MissInSpanner=5, MissInSource=2
    PCollection<KV<String, Long>> matched =
        pipeline.apply("CreateMatched", Create.of(KV.of("Table1", 10L)));
    PCollection<KV<String, Long>> missInSpanner =
        pipeline.apply("CreateMissSpanner", Create.of(KV.of("Table1", 5L)));
    PCollection<KV<String, Long>> missInSource =
        pipeline.apply("CreateMissSource", Create.of(KV.of("Table1", 2L)));

    PCollection<KV<String, CoGbkResult>> coGbk =
        KeyedPCollectionTuple.of(matchedTag, matched)
            .and(missInSpannerTag, missInSpanner)
            .and(missInSourceTag, missInSource)
            .apply(CoGroupByKey.create());

    PCollection<TableValidationStats> stats =
        coGbk.apply(
            ParDo.of(
                new ComputeTableStatsFn(
                    runId, startTimestamp, matchedTag, missInSpannerTag, missInSourceTag)));

    PAssert.that(stats)
        .satisfies(
            collection -> {
              TableValidationStats stat = collection.iterator().next();
              if (!stat.getRunId().equals(runId)) {
                throw new AssertionError("RunId mismatch");
              }
              if (!stat.getTableName().equals("Table1")) {
                throw new AssertionError("TableName mismatch");
              }
              if (!stat.getStatus().equals("MISMATCH")) {
                throw new AssertionError("Status mismatch: " + stat.getStatus());
              }
              if (stat.getMatchedRowCount() != 10L) {
                throw new AssertionError("Matched count mismatch");
              }
              if (stat.getMismatchRowCount() != 7L) {
                throw new AssertionError("Mismatch count mismatch (5+2)");
              }
              if (stat.getSourceRowCount() != 15L) {
                throw new AssertionError("Source row count mismatch (10+5)");
              }
              if (stat.getDestinationRowCount() != 12L) {
                throw new AssertionError("Dest row count mismatch (10+2)");
              }
              return null;
            });

    pipeline.run();
  }
}
