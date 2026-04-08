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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that selects a table for each tick based on weighted probability. The weight
 * for a table is (table QPS + QPS of its children). The denominator is the total QPS across all
 * tables.
 */
public class SelectTable extends PTransform<PCollection<Long>, PCollection<DataGeneratorTable>> {

  private final PCollectionView<DataGeneratorSchema> schemaView;

  public SelectTable(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<DataGeneratorTable> expand(PCollection<Long> input) {
    return input.apply(
        "SelectTableFn", ParDo.of(new SelectTableFn(schemaView)).withSideInputs(schemaView));
  }

  static class SelectTableFn extends DoFn<Long, DataGeneratorTable> {
    private static final Logger LOG = LoggerFactory.getLogger(SelectTableFn.class);
    private final PCollectionView<DataGeneratorSchema> schemaView;

    // Cache for table weights: Key = Cumulative Probability (0.0 to 1.0), Value =
    // Table Name
    private transient NavigableMap<Double, DataGeneratorTable> tableSelectionMap;

    public SelectTableFn(PCollectionView<DataGeneratorSchema> schemaView) {
      this.schemaView = schemaView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DataGeneratorSchema schema = c.sideInput(schemaView);

      // Lazily initialize selection map. Schema is assumed to never change.
      if (tableSelectionMap == null) {
        tableSelectionMap = buildSelectionMap(schema);
      }

      if (tableSelectionMap.isEmpty()) {
        LOG.warn("No tables available for selection.");
        return;
      }

      double randomValue = ThreadLocalRandom.current().nextDouble();
      Map.Entry<Double, DataGeneratorTable> entry = tableSelectionMap.higherEntry(randomValue);

      // Fallback for rounding errors (should not happen if normalized correctly, but
      // added for safety)
      if (entry == null) {
        entry = tableSelectionMap.lastEntry();
      }

      c.output(entry.getValue());
    }

    @VisibleForTesting
    static NavigableMap<Double, DataGeneratorTable> buildSelectionMap(DataGeneratorSchema schema) {
      NavigableMap<Double, DataGeneratorTable> selectionMap = new TreeMap<>();

      double totalWeight = 0;
      Map<String, Double> tableWeights = new HashMap<>();

      // 1. Calculate weight for each root table (including descendants)
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          double weight = calculateTotalQps(table, schema);
          tableWeights.put(table.name(), weight);
          totalWeight += weight;
        }
      }

      // 2. Build Selection Map based on calculated weights
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          double weight = tableWeights.getOrDefault(table.name(), 0.0);
          if (totalWeight > 0) {
            double probability = weight / totalWeight;
            double previousMax = selectionMap.isEmpty() ? 0.0 : selectionMap.lastKey();
            selectionMap.put(previousMax + probability, table);
          }
        }
      }

      LOG.info(
          "Initialized Table Selection Map with {} entries. Total Weight (Root + Children QPS): {}",
          selectionMap.size(),
          totalWeight);

      return selectionMap;
    }

    private static double calculateTotalQps(DataGeneratorTable table, DataGeneratorSchema schema) {
      double totalQps = table.insertQps();
      if (table.childTables() != null) {
        for (String childName : table.childTables()) {
          DataGeneratorTable childTable = schema.tables().get(childName);
          if (childTable != null) {
            totalQps += calculateTotalQps(childTable, schema);
          }
        }
      }
      return totalQps;
    }
  }
}
