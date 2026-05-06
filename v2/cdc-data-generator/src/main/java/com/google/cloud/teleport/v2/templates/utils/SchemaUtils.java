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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Utilities for manipulating {@link DataGeneratorSchema}. */
public class SchemaUtils {

  /**
   * Constructs a Directed Acyclic Graph (DAG) of tables in the schema. Identifies parent-child
   * relationships based on Foreign Keys and Interleaving. Handles multiple parents by selecting the
   * one with the *least* QPS. Populates the `children` list for each table and sets `isRoot`
   * accordingly.
   *
   * <p>Note: Circular dependencies are not supported right now.
   *
   * @param schema The input schema.
   * @return A new schema with DAG information populated.
   */
  public static DataGeneratorSchema generateSchemaDAG(DataGeneratorSchema schema) {
    Map<String, DataGeneratorTable> tableMap = schema.tables();
    Map<String, List<String>> parentToSequenceChild = new HashMap<>();
    Set<String> hasSequenceParent = new HashSet<>();

    // 1. Build Dependency Chains for Each Table
    for (DataGeneratorTable childTable : tableMap.values()) {
      String childName = childTable.name();

      // Collect Parents (Interleaved and FK)
      List<DataGeneratorTable> parents = new ArrayList<>();

      if (childTable.interleavedInTable() != null) {
        String parentName = childTable.interleavedInTable();
        DataGeneratorTable parentTable = tableMap.get(parentName);
        if (parentTable != null) {
          parents.add(parentTable);
        }
      }

      for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
        DataGeneratorTable parentTable = tableMap.get(fk.referencedTable());
        if (parentTable != null && !parents.contains(parentTable)) {
          parents.add(parentTable);
        }
      }

      if (parents.isEmpty()) {
        continue; // No parents for this table
      }

      // Sort Parents by QPS
      parents.sort(java.util.Comparator.comparingInt(DataGeneratorTable::insertQps));

      // Chain the Parents: P1 -> P2 -> ... -> Pn -> Child
      for (int i = 0; i < parents.size() - 1; i++) {
        String currentParentName = parents.get(i).name();
        String nextParentName = parents.get(i + 1).name();
        // Avoid adding duplicate dependencies if a table is part of multiple chains
        List<String> currentChildren =
            parentToSequenceChild.computeIfAbsent(currentParentName, k -> new ArrayList<>());
        if (!currentChildren.contains(nextParentName)) {
          currentChildren.add(nextParentName);
        }
        hasSequenceParent.add(nextParentName);
      }

      // Link the last parent in the chain to the child table
      String lastParentName = parents.get(parents.size() - 1).name();
      List<String> lastParentChildren =
          parentToSequenceChild.computeIfAbsent(lastParentName, k -> new ArrayList<>());
      if (!lastParentChildren.contains(childName)) {
        lastParentChildren.add(childName);
      }
      hasSequenceParent.add(childName);
    }

    // 2. Update Tables with Sequence Children and isRoot
    ImmutableMap.Builder<String, DataGeneratorTable> newTablesBuilder = ImmutableMap.builder();
    for (DataGeneratorTable table : tableMap.values()) {
      String tableName = table.name();
      List<String> sequenceChildren =
          parentToSequenceChild.getOrDefault(tableName, ImmutableList.of());
      boolean isRoot = !hasSequenceParent.contains(tableName);

      newTablesBuilder.put(
          tableName,
          table.toBuilder()
              .childTables(
                  ImmutableList.copyOf(
                      sequenceChildren)) // These are tables to generate AFTER this one
              .isRoot(isRoot)
              .build());
    }

    return DataGeneratorSchema.builder().tables(newTablesBuilder.buildOrThrow()).build();
  }
}
