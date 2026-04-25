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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Rescales the input tick stream to match the total target QPS of all root tables in the schema.
 *
 * <p>This DoFn acts as a rate converter between the fixed {@code baseTickRate} (configured at
 * pipeline start) and the dynamic {@code totalQps} (read from the schema side-input). It handles
 * rate conversion in two ways:
 *
 * <ul>
 *   <li>**Rate Reduction** (Total QPS &lt; baseTickRate): Elements are thinned out by emitting each
 *       input with a probability of {@code totalQps / baseTickRate}.
 *   <li>**Rate Amplification** (Total QPS &gt;= baseTickRate): Elements are multiplied. It
 *       deterministically emits {@code totalQps / baseTickRate} copies per input, and
 *       probabilistically emits one more copy to account for the fractional remainder.
 * </ul>
 */
public class ScaleTicksFn extends DoFn<Long, Long> {

  private final PCollectionView<DataGeneratorSchema> schemaView;
  private final int baseTickRate;
  private transient DataGeneratorSchema cachedSchema;

  private transient int cachedTotalQps;

  public ScaleTicksFn(PCollectionView<DataGeneratorSchema> schemaView, int baseTickRate) {
    this.schemaView = schemaView;
    this.baseTickRate = baseTickRate;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    DataGeneratorSchema schema = c.sideInput(schemaView);

    // Schemas arrive as a cached side-input object that's reused across
    // bundles, so this avoids re-summing the QPS on every element.
    if (cachedSchema != schema) {
      cachedTotalQps = totalRootQps(schema);
      cachedSchema = schema;
    }

    if (cachedTotalQps <= 0) {
      return; // Nothing to do — no root tables, or all have zero QPS.
    }

    if (cachedTotalQps < baseTickRate) {
      double probability = (double) cachedTotalQps / baseTickRate;
      if (ThreadLocalRandom.current().nextDouble() < probability) {
        c.output(c.element());
      }
      return;
    }

    // Scale up. Note: multiplier=1/remainder=0 covers the "totalQps == baseTickRate" case, so
    // we don't need a separate equality branch.
    int multiplier = cachedTotalQps / baseTickRate;
    int remainder = cachedTotalQps % baseTickRate;

    for (int i = 0; i < multiplier; i++) {
      c.output(c.element());
    }
    if (remainder > 0) {
      double probability = (double) remainder / baseTickRate;
      if (ThreadLocalRandom.current().nextDouble() < probability) {
        c.output(c.element());
      }
    }
  }

  /** Sum {@code insertQps} across every root table in the schema. */
  @VisibleForTesting
  public static int totalRootQps(DataGeneratorSchema schema) {
    int total = 0;
    for (DataGeneratorTable table : schema.tables().values()) {
      if (table.isRoot()) {
        total += table.insertQps();
      }
    }
    return total;
  }
}
