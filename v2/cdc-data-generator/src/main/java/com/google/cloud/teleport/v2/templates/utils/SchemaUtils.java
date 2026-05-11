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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for manipulating {@link DataGeneratorSchema}. */
public class SchemaUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

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

      // Sort Parents Topologically to respect physical FK relations
      List<DataGeneratorTable> sortedParents = sortTopologically(parents, tableMap);

      // Chain the Parents: P1 -> P2 -> ... -> Pn -> Child
      for (int i = 0; i < sortedParents.size() - 1; i++) {
        String currentParentName = sortedParents.get(i).name();
        String nextParentName = sortedParents.get(i + 1).name();
        // Avoid adding duplicate dependencies if a table is part of multiple chains
        List<String> currentChildren =
            parentToSequenceChild.computeIfAbsent(currentParentName, k -> new ArrayList<>());
        if (!currentChildren.contains(nextParentName)) {
          currentChildren.add(nextParentName);
        }
        hasSequenceParent.add(nextParentName);
      }

      // Link the last parent in the chain to the child table
      String lastParentName = sortedParents.get(sortedParents.size() - 1).name();
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

      boolean hasAncestorDelete =
          hasPhysicalAncestorWithDeleteQps(table, tableMap, new HashSet<>());
      Integer finalDeleteQps = hasAncestorDelete ? Integer.valueOf(0) : table.deleteQps();

      newTablesBuilder.put(
          tableName,
          table.toBuilder()
              .childTables(
                  ImmutableList.copyOf(
                      sequenceChildren)) // These are tables to generate data AFTER this one
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
   * Checks if any physical ancestor (via interleaving or foreign keys) has delete QPS configured.
   * Used to suppress child table delete generation to prevent double deletion conflicts.
   */
  private static boolean hasPhysicalAncestorWithDeleteQps(
      DataGeneratorTable table, Map<String, DataGeneratorTable> tableMap, Set<String> visited) {
    if (table == null || !visited.add(table.name())) {
      return false; // Cycle or null table reached
    }

    // 1. Check Interleaved Parent
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

    // 2. Check Foreign Key Parents
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

  /**
   * Sorts a collection of tables topologically, ensuring parent/referenced tables are listed before
   * their dependent child tables.
   */
  public static List<DataGeneratorTable> sortTopologically(
      Collection<DataGeneratorTable> tables, Map<String, DataGeneratorTable> allTables) {
    List<DataGeneratorTable> sortedInput = new ArrayList<>(tables);
    // Deterministic initial sort prioritizing roots and higher QPS tables
    sortedInput.sort(
        Comparator.comparing(
                (DataGeneratorTable t) -> t.isRoot() != null && t.isRoot(),
                Comparator.reverseOrder())
            .thenComparingInt(t -> t.insertQps() != null ? t.insertQps() : 0)
            .thenComparing(DataGeneratorTable::name));

    List<DataGeneratorTable> sorted = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    Set<String> visiting = new HashSet<>();

    for (DataGeneratorTable table : sortedInput) {
      if (!visited.contains(table.name())) {
        visitTopologically(table, allTables, visited, visiting, sorted, sortedInput);
      }
    }
    return sorted;
  }

  /**
   * Recursive depth-first search helper for topological sorting. Detects circular dependencies via
   * the 'visiting' tracking set.
   */
  private static void visitTopologically(
      DataGeneratorTable table,
      Map<String, DataGeneratorTable> allTables,
      Set<String> visited,
      Set<String> visiting,
      List<DataGeneratorTable> sorted,
      List<DataGeneratorTable> subset) {
    visiting.add(table.name());

    // Collect all physical dependencies (interleaved + FK parents)
    List<String> parentNames = new ArrayList<>();
    if (table.interleavedInTable() != null) {
      parentNames.add(table.interleavedInTable());
    }
    for (DataGeneratorForeignKey fk : table.foreignKeys()) {
      parentNames.add(fk.referencedTable());
    }

    // Recursively visit dependencies first
    for (String refTable : parentNames) {
      if (subset.stream().anyMatch(t -> t.name().equals(refTable))) {
        DataGeneratorTable parent = allTables.get(refTable);
        if (parent != null && !visited.contains(refTable)) {
          if (visiting.contains(refTable)) {
            throw new IllegalStateException(
                "Circular dependency detected in schema involving table: " + refTable);
          }
          visitTopologically(parent, allTables, visited, visiting, sorted, subset);
        }
      }
    }

    visiting.remove(table.name());
    visited.add(table.name());
    sorted.add(table);
  }

  /** Builds the global insertion order across all tables in the schema for pipeline execution. */
  public static List<String> buildInsertTopoOrder(DataGeneratorSchema schema) {
    List<DataGeneratorTable> sortedTables =
        sortTopologically(schema.tables().values(), schema.tables());
    return sortedTables.stream().map(DataGeneratorTable::name).collect(Collectors.toList());
  }
}
