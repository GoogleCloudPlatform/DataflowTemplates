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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectTableFn extends DoFn<Long, DataGeneratorTable> {
  private static final Logger LOG = LoggerFactory.getLogger(SelectTableFn.class);
  private final PCollectionView<DataGeneratorSchema> schemaView;

  // Cache for table distribution
  private transient EnumeratedDistribution<DataGeneratorTable> tableDistribution;

  public SelectTableFn(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    DataGeneratorSchema schema = c.sideInput(schemaView);

    // Lazily initialize distribution. Schema is assumed to never change.
    if (tableDistribution == null) {
      tableDistribution = buildDistribution(schema);
    }

    if (tableDistribution == null) {
      LOG.warn("No tables available for selection (total weight may be 0).");
      return;
    }

    c.output(tableDistribution.sample());
  }

  @VisibleForTesting
  public static EnumeratedDistribution<DataGeneratorTable> buildDistribution(
      DataGeneratorSchema schema) {
    List<Pair<DataGeneratorTable, Double>> pmf =
        schema.tables().values().stream()
            .filter(DataGeneratorTable::isRoot)
            .map(table -> new Pair<>(table, calculateTotalQps(table, schema)))
            .collect(Collectors.toList());

    if (pmf.isEmpty()) {
      return null;
    }

    double totalWeight = pmf.stream().mapToDouble(Pair::getValue).sum();
    if (totalWeight <= 0) {
      return null;
    }

    LOG.info(
        "Initialized Table Distribution with {} entries. Total Weight (Root + Children QPS): {}",
        pmf.size(),
        totalWeight);

    return new EnumeratedDistribution<>(pmf);
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
