/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

import static com.google.cloud.teleport.spanner.spannerio.MutationUtils.isPointDelete;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
final class MutationCellCounter {
  // Prevent construction.
  private MutationCellCounter() {}

  /** Count the number of cells modified by {@link MutationGroup}. */
  public static long countOf(SpannerSchema spannerSchema, MutationGroup mutationGroup) {
    long mutatedCells = 0L;
    for (Mutation mutation : mutationGroup) {
      if (mutation.getOperation() == Op.DELETE) {
        // For single key deletes sum up all the columns in the schema.
        // There is no clear way to estimate range deletes, so they are ignored.
        if (isPointDelete(mutation)) {
          final KeySet keySet = mutation.getKeySet();

          final long rows = Iterables.size(keySet.getKeys());
          mutatedCells += rows * spannerSchema.getCellsMutatedPerRow(mutation.getTable());
        }
      } else {
        // sum the cells of the columns included in the mutation
        for (String column : mutation.getColumns()) {
          mutatedCells += spannerSchema.getCellsMutatedPerColumn(mutation.getTable(), column);
        }
      }
    }

    return mutatedCells;
  }
}
