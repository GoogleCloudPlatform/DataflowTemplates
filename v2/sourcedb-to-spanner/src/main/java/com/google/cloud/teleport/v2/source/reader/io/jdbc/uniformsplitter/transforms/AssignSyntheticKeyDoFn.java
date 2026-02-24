/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * A {@link DoFn} that assigns a synthetic integer key to each {@link Range} based on its {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier}.
 *
 * <p>This enables downstream transforms like {@link RangeCombiner} to process ranges in parallel,
 * memory-safe batches (keyed-combinations) rather than attempting a global combination that could
 * lead to Out-Of-Memory (OOM) errors when dealing with millions of ranges from thousands of tables.
 */
@AutoValue
public abstract class AssignSyntheticKeyDoFn extends DoFn<Range, KV<Integer, Range>> {

  abstract int numBatches();

  public static Builder builder() {
    return new AutoValue_AssignSyntheticKeyDoFn.Builder();
  }

  @ProcessElement
  public void processElement(@Element Range range, OutputReceiver<KV<Integer, Range>> out) {
    int batchKey = Math.floorMod(range.tableIdentifier().hashCode(), numBatches());
    out.output(KV.of(batchKey, range));
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setNumBatches(int value);

    public abstract AssignSyntheticKeyDoFn build();
  }
}
