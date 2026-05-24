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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** A {@link DoFn} that computes {@link TableValidationStats} from a {@link CoGbkResult}. */
public class ComputeTableStatsFn extends DoFn<KV<String, CoGbkResult>, TableValidationStats> {

  private final String runId;
  private final Instant startTimestamp;
  private final TupleTag<Long> matchedTag;
  private final TupleTag<Long> missInSpannerTag;
  private final TupleTag<Long> missInSourceTag;

  public ComputeTableStatsFn(
      String runId,
      Instant startTimestamp,
      TupleTag<Long> matchedTag,
      TupleTag<Long> missInSpannerTag,
      TupleTag<Long> missInSourceTag) {
    this.runId = runId;
    this.startTimestamp = startTimestamp;
    this.matchedTag = matchedTag;
    this.missInSpannerTag = missInSpannerTag;
    this.missInSourceTag = missInSourceTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String tableName = c.element().getKey();
    CoGbkResult result = c.element().getValue();

    long matched = result.getOnly(matchedTag, 0L);
    long onlyInGcs = result.getOnly(missInSpannerTag, 0L);
    long onlyInSpanner = result.getOnly(missInSourceTag, 0L);
    long mismatch = onlyInGcs + onlyInSpanner;

    String status = mismatch == 0 ? "MATCH" : "MISMATCH";
    Instant now = Instant.now();

    c.output(
        TableValidationStats.builder()
            .setRunId(runId)
            .setTableName(tableName)
            .setStatus(status)
            .setSourceRowCount(matched + onlyInGcs)
            .setDestinationRowCount(matched + onlyInSpanner)
            .setMatchedRowCount(matched)
            .setMismatchRowCount(mismatch)
            .setStartTimestamp(startTimestamp)
            .setEndTimestamp(now)
            .build());
  }
}
