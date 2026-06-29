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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class LatestRecordCombineFn
    extends CombineFn<
        TrimmedShardedDataChangeRecord,
        List<TrimmedShardedDataChangeRecord>,
        TrimmedShardedDataChangeRecord> {

  @Override
  public List<TrimmedShardedDataChangeRecord> createAccumulator() {
    return new ArrayList<>();
  }

  @Override
  public List<TrimmedShardedDataChangeRecord> addInput(
      List<TrimmedShardedDataChangeRecord> accumulator, TrimmedShardedDataChangeRecord input) {
    if (accumulator.isEmpty()) {
      accumulator.add(input);
    } else {
      accumulator.set(0, getLatest(accumulator.get(0), input));
    }
    return accumulator;
  }

  @Override
  public List<TrimmedShardedDataChangeRecord> mergeAccumulators(
      Iterable<List<TrimmedShardedDataChangeRecord>> accumulators) {
    TrimmedShardedDataChangeRecord latest = null;
    for (List<TrimmedShardedDataChangeRecord> accumulator : accumulators) {
      if (!accumulator.isEmpty()) {
        if (latest == null) {
          latest = accumulator.get(0);
        } else {
          latest = getLatest(latest, accumulator.get(0));
        }
      }
    }
    List<TrimmedShardedDataChangeRecord> merged = new ArrayList<>();
    if (latest != null) {
      merged.add(latest);
    }
    return merged;
  }

  @Override
  public TrimmedShardedDataChangeRecord extractOutput(
      List<TrimmedShardedDataChangeRecord> accumulator) {
    if (accumulator.isEmpty()) {
      return null;
    }
    return accumulator.get(0);
  }

  private TrimmedShardedDataChangeRecord getLatest(
      TrimmedShardedDataChangeRecord r1, TrimmedShardedDataChangeRecord r2) {
    int timestampComparison = r1.getCommitTimestamp().compareTo(r2.getCommitTimestamp());
    if (timestampComparison > 0) {
      return r1;
    } else if (timestampComparison < 0) {
      return r2;
    } else {
      long seq1 = Long.parseLong(r1.getRecordSequence());
      long seq2 = Long.parseLong(r2.getRecordSequence());
      return seq1 >= seq2 ? r1 : r2;
    }
  }
}
