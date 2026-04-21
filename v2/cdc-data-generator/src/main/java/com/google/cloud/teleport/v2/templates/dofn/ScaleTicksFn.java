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
 * Reshapes a fixed baseline tick stream into a rate that matches the schema's total root-table
 * QPS.
 *
 * <p>{@code GenerateSequence.withRate(rate, ...)} needs {@code rate} to be a compile-time
 * constant — a value known at pipeline construction. But the schema (and therefore the total
 * root QPS) is delivered as a side input because fetching the real schema from Spanner/MySQL at
 * construction time can take minutes on a 5000-table database and exceeds the Dataflow template
 * launcher's construction-time budget.
 *
 * <p>This DoFn bridges the two: the pipeline emits at a fixed {@code baseTickRate}, and this
 * DoFn rescales that stream per element using the schema side input. Two cases:
 *
 * <ul>
 *   <li>Total QPS &lt; baseTickRate: emit each input with probability {@code totalQps /
 *       baseTickRate}. Expected output rate = baseTickRate × probability = totalQps.
 *   <li>Total QPS ≥ baseTickRate: deterministically emit {@code multiplier} copies per input,
 *       then probabilistically emit one more copy to cover the sub-tick remainder. Expected
 *       output rate = baseTickRate × (multiplier + remainder/baseTickRate) = totalQps.
 * </ul>
 *
 * <p>{@link ThreadLocalRandom} is used rather than a seeded {@link java.util.Random} because the
 * probability sampling is cosmetic (it shapes the average rate) and doesn't need to be
 * reproducible across workers; using the thread-local avoids contention under high fan-out.
 */
public class ScaleTicksFn extends DoFn<Long, Long> {

  private final PCollectionView<DataGeneratorSchema> schemaView;
  private final int baseTickRate;

  /**
   * Cached total-root-QPS sum. Recomputed only when the side input object identity changes.
   * Schemas are effectively static in this pipeline, so the cache hits on every element after
   * the first.
   */
  private transient DataGeneratorSchema cachedSchema;

  private transient int cachedTotalQps;

  public ScaleTicksFn(PCollectionView<DataGeneratorSchema> schemaView, int baseTickRate) {
    this.schemaView = schemaView;
    this.baseTickRate = baseTickRate;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    DataGeneratorSchema schema = c.sideInput(schemaView);

    // Cheap identity check — schemas arrive as a cached side-input object that's reused across
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
