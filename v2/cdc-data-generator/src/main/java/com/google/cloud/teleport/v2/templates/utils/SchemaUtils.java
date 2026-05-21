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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for manipulating {@link DataGeneratorSchema}. */
public class SchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  /**
   * Constructs a Directed Acyclic Graph (DAG) of tables in the schema. Merges parallel parent
   * tracks into sequential execution chains sorted by QPS.
   */
  public static DataGeneratorSchema generateSchemaDAG(DataGeneratorSchema schema) {
    Map<String, DataGeneratorTable> tableMap = schema.tables();
    Map<String, List<String>> parentToSequenceChild = new HashMap<>();
    Set<String> hasSequenceParent = new HashSet<>();

    // 1. Generate a single, authoritative global topological order sorted by QPS
    List<DataGeneratorTable> globalOrder = sortGloballyTopologically(tableMap);

    // 2. Build Sequential Execution Chains using sub-sequences of the global order
    for (DataGeneratorTable childTable : tableMap.values()) {
      Set<DataGeneratorTable> uniqueParents = new HashSet<>();
      // Gather all recursive upstream ancestors (interleaving + foreign keys)
      collectAncestors(childTable, tableMap, uniqueParents);

      if (uniqueParents.isEmpty()) {
        continue; // Standalone table with no dependencies
      }

      Set<String> lineageNames =
          uniqueParents.stream().map(DataGeneratorTable::name).collect(Collectors.toSet());
      lineageNames.add(childTable.name());

      // Filter the master global order to keep only this table's specific lineage
      List<DataGeneratorTable> executionChain =
          globalOrder.stream()
              .filter(t -> lineageNames.contains(t.name()))
              .collect(Collectors.toList());

      // Link the sorted lineage chain together consecutively (P1 -> P2 -> ... -> Child)
      for (int i = 0; i < executionChain.size() - 1; i++) {
        String currentTable = executionChain.get(i).name();
        String nextTable = executionChain.get(i + 1).name();

        List<String> sequenceChildren =
            parentToSequenceChild.computeIfAbsent(currentTable, k -> new ArrayList<>());
        if (!sequenceChildren.contains(nextTable)) {
          sequenceChildren.add(nextTable);
        }
        hasSequenceParent.add(nextTable);
      }
    }

    // 3. Construct Final Table Definitions
    ImmutableMap.Builder<String, DataGeneratorTable> newTablesBuilder = ImmutableMap.builder();
    for (DataGeneratorTable table : tableMap.values()) {
      String tableName = table.name();
      List<String> sequenceChildren =
          parentToSequenceChild.getOrDefault(tableName, ImmutableList.of());
      // A table is a root if it is not triggered as a sequence child by any other table
      boolean isRoot = !hasSequenceParent.contains(tableName);

      // Prevent double deletion conflicts by overriding deleteQps to 0 on child tables when any
      // upstream
      // physical ancestor has deleteQps > 0. This ensures child tables do not randomly delete
      // themselves
      // independently. Instead, child tables explicitly adopt their parent's forced deletion
      // schedule
      // during cascading generation in DataGeneratorEngine.
      boolean hasAncestorDelete =
          hasPhysicalAncestorWithDeleteQps(table, tableMap, new HashSet<>());
      Integer finalDeleteQps = hasAncestorDelete ? Integer.valueOf(0) : table.deleteQps();

      newTablesBuilder.put(
          tableName,
          table.toBuilder()
              .childTables(ImmutableList.copyOf(sequenceChildren))
              .isRoot(isRoot)
              .deleteQps(finalDeleteQps)
              .build());

      if (isRoot) {
        LOG.info("Identified Root table: {}", tableName);
      }
      if (!sequenceChildren.isEmpty()) {
        LOG.info("Table {} triggers children: {}", tableName, sequenceChildren);
      }
    }

    return DataGeneratorSchema.builder().tables(newTablesBuilder.buildOrThrow()).build();
  }

  /**
   * Standard Kahn's Algorithm for Global Topological Sorting. Uses a PriorityQueue to ensure that
   * when multiple parent tables are unblocked, the one with the lowest insertQps is selected first.
   */
  private static List<DataGeneratorTable> sortGloballyTopologically(
      Map<String, DataGeneratorTable> tableMap) {
    Map<String, Integer> inDegree = new HashMap<>();
    Map<String, List<String>> adjacencyList = new HashMap<>();

    for (String tableName : tableMap.keySet()) {
      inDegree.put(tableName, 0);
      adjacencyList.put(tableName, new ArrayList<>());
    }

    // Map physical dependencies (interleaving and FKs) into graph directed edges
    for (DataGeneratorTable table : tableMap.values()) {
      String child = table.name();
      List<String> parents = new ArrayList<>();
      if (table.interleavedInTable() != null) {
        parents.add(table.interleavedInTable());
      }
      if (table.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : table.foreignKeys()) {
          parents.add(fk.referencedTable());
        }
      }

      for (String parent : parents) {
        if (tableMap.containsKey(parent)) {
          adjacencyList.get(parent).add(child);
          inDegree.put(child, inDegree.get(child) + 1);
        }
      }
    }

    // Priority Queue sorts prioritizing roots, then ascending by QPS, then alphabetically by name
    PriorityQueue<DataGeneratorTable> queue =
        new PriorityQueue<>(
            Comparator.comparing(
                    (DataGeneratorTable t) -> t.isRoot() != null && t.isRoot(),
                    Comparator.reverseOrder())
                .thenComparingInt(
                    (DataGeneratorTable t) -> t.insertQps() != null ? t.insertQps() : 0)
                .thenComparing(DataGeneratorTable::name));

    for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
      if (entry.getValue() == 0) {
        queue.add(tableMap.get(entry.getKey()));
      }
    }

    List<DataGeneratorTable> globalOrder = new ArrayList<>();
    while (!queue.isEmpty()) {
      DataGeneratorTable current = queue.poll();
      globalOrder.add(current);

      for (String neighbor : adjacencyList.get(current.name())) {
        int updatedInDegree = inDegree.get(neighbor) - 1;
        inDegree.put(neighbor, updatedInDegree);
        if (updatedInDegree == 0) {
          queue.add(tableMap.get(neighbor));
        }
      }
    }

    if (globalOrder.size() != tableMap.size()) {
      throw new IllegalStateException("Circular dependency detected in schema layout.");
    }

    return globalOrder;
  }

  /** Recursively collects all physical upstream ancestor tables (via interleaving or FKs). */
  private static void collectAncestors(
      DataGeneratorTable table,
      Map<String, DataGeneratorTable> tableMap,
      Set<DataGeneratorTable> uniqueParents) {
    if (table.interleavedInTable() != null) {
      DataGeneratorTable parent = tableMap.get(table.interleavedInTable());
      if (parent != null && uniqueParents.add(parent)) {
        collectAncestors(parent, tableMap, uniqueParents);
      }
    }
    if (table.foreignKeys() != null) {
      for (DataGeneratorForeignKey fk : table.foreignKeys()) {
        DataGeneratorTable parent = tableMap.get(fk.referencedTable());
        if (parent != null && uniqueParents.add(parent)) {
          collectAncestors(parent, tableMap, uniqueParents);
        }
      }
    }
  }

  /**
   * Checks if any physical ancestor (interleaved or foreign key) has delete QPS configured. Used to
   * override child table deleteQps to 0 to prevent double deletion conflicts across hierarchies.
   */
  private static boolean hasPhysicalAncestorWithDeleteQps(
      DataGeneratorTable table, Map<String, DataGeneratorTable> tableMap, Set<String> visited) {
    if (table == null || !visited.add(table.name())) {
      return false;
    }
    if (table.interleavedInTable() != null) {
      DataGeneratorTable p = tableMap.get(table.interleavedInTable());
      if (p != null) {
        if (p.deleteQps() != null && p.deleteQps() > 0) {
          return true;
        }
        if (hasPhysicalAncestorWithDeleteQps(p, tableMap, visited)) {
          return true;
        }
      }
    }
    if (table.foreignKeys() != null) {
      for (DataGeneratorForeignKey fk : table.foreignKeys()) {
        DataGeneratorTable p = tableMap.get(fk.referencedTable());
        if (p != null) {
          if (p.deleteQps() != null && p.deleteQps() > 0) {
            return true;
          }
          if (hasPhysicalAncestorWithDeleteQps(p, tableMap, visited)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /** Builds the global insertion order across all tables in the schema for pipeline execution. */
  public static List<String> buildInsertTopoOrder(DataGeneratorSchema schema) {
    List<DataGeneratorTable> sortedTables = sortGloballyTopologically(schema.tables());
    return sortedTables.stream().map(DataGeneratorTable::name).collect(Collectors.toList());
  }
}
