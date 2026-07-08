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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rescales the input tick stream to match the total target QPS of all root tables in the schema.
 *
 * <p>Since the input tick rate is 1 tick per second (driven by PeriodicImpulse), this DoFn simply
 * outputs {@code totalQps} copies of the timestamp (in milliseconds) for each incoming tick.
 */
public class ScaleTicksFn extends DoFn<Instant, Long> {

  private static final Logger LOG = LoggerFactory.getLogger(ScaleTicksFn.class);
  private final PCollectionView<DataGeneratorSchema> schemaView;
  private transient DataGeneratorSchema cachedSchema;
  private transient int cachedTotalQps;

  public ScaleTicksFn(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    DataGeneratorSchema schema = c.sideInput(schemaView);

    // Schemas arrive as a cached side-input object that's reused across
    // bundles, so this avoids re-summing the QPS on every element.
    if (cachedSchema != schema) {
      cachedTotalQps = totalRootQps(schema);
      cachedSchema = schema;
      LOG.info("Total QPS resolved to: {}", cachedTotalQps);
    }

    if (cachedTotalQps <= 0) {
      throw new IllegalStateException(
          "Total QPS must be greater than zero. Found: "
              + cachedTotalQps
              + " . Please verify your config.");
    }

    // Since base tick rate is effectively 1 per second, we just output totalQps times
    Long millis = c.element().getMillis();
    for (int i = 0; i < cachedTotalQps; i++) {
      c.output(millis);
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
