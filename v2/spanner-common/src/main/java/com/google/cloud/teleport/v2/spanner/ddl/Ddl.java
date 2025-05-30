/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.spanner.ddl;

import com.google.cloud.spanner.Dialect;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Allows to build and introspect Cloud Spanner DDL with Java code. */
public class Ddl implements Serializable {

  private static final String ROOT = "#";
  private static final long serialVersionUID = -4153759448351855360L;

  private ImmutableSortedMap<String, Table> tables;
  private TreeMultimap<String, String> parents;

  private final Dialect dialect;

  private Ddl(
      ImmutableSortedMap<String, Table> tables,
      TreeMultimap<String, String> parents,
      Dialect dialect) {
    this.tables = tables;
    this.parents = parents;
    this.dialect = dialect;
  }

  public Dialect dialect() {
    return dialect;
  }

  public Collection<Table> allTables() {
    return tables.values();
  }

  public Collection<Table> rootTables() {
    return childTables(ROOT);
  }

  public Collection<Table> childTables(String table) {
    return Collections2.transform(
        childTableNames(table),
        new Function<String, Table>() {

          @Nullable
          @Override
          public Table apply(@Nullable String input) {
            return table(input);
          }
        });
  }

  /**
   * Return list of all the tables that this table refers to or depends on. This method only
   * provides the immediate references, not the whole tree.
   *
   * @param tableName
   * @return
   */
  public List<String> tablesReferenced(String tableName) {
    Set<String> tablesReferenced = new HashSet<>();
    Table table = tables.get(tableName.toLowerCase());
    // Add check if table not found
    if (table == null) {
      throw new IllegalStateException(
          "attempting to fetch table which does not exist in spanner:" + tableName);
    }
    if (table.interleavingParent() != null
        && !Objects.equals(table.interleaveType(), new String("IN"))) {
      tablesReferenced.add(table.interleavingParent());
    }
    Set<String> fkReferencedTables =
        table.foreignKeys().stream().map(f -> f.referencedTable()).collect(Collectors.toSet());
    tablesReferenced.addAll(fkReferencedTables);
    return new ArrayList<>(tablesReferenced);
  }

  private void orderedTablesByReferenceUtil(
      Set<String> visited,
      Set<String> processingTables,
      String tableName,
      List<String> orderedTables) {
    if (visited.contains(tableName.toLowerCase())) {
      return;
    }
    if (processingTables.contains(tableName.toLowerCase())) {
      throw new IllegalStateException(
          "Cyclic dependency detected! Involved tables: " + processingTables);
    }

    processingTables.add(tableName.toLowerCase());
    for (String parent : tablesReferenced(tableName)) {
      orderedTablesByReferenceUtil(visited, processingTables, parent, orderedTables);
    }
    orderedTables.add(tableName);
    visited.add(tableName.toLowerCase());
    processingTables.remove(tableName.toLowerCase());
  }

  public List<String> getTablesOrderedByReference() {
    List<String> orderedTables = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    Set<String> processingTables = new HashSet<>();
    for (Table table : allTables()) {
      orderedTablesByReferenceUtil(visited, processingTables, table.name(), orderedTables);
    }
    return orderedTables;
  }

  public List<String> getAllReferencedTables(String tableName) {
    Table table = tables.get(tableName.toLowerCase());
    if (!tables.containsKey(tableName.toLowerCase())) {
      throw new IllegalStateException(
          "cannot fetch referenced tables for table which does not exist"
              + " in spanner:"
              + tableName);
    }
    List<String> orderedTables = new LinkedList<>();
    Set<String> visited = new HashSet<>();
    Set<String> processingTables = new HashSet<>();
    orderedTablesByReferenceUtil(visited, processingTables, table.name(), orderedTables);
    orderedTables.remove(table.name()); // Remove reference of self from list
    return orderedTables;
  }

  private NavigableSet<String> childTableNames(String table) {
    return parents.get(table.toLowerCase());
  }

  public Table table(String tableName) {
    return tables.get(tableName.toLowerCase());
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    LinkedList<String> stack = Lists.newLinkedList();
    stack.addAll(childTableNames(ROOT));
    boolean first = true;
    Set<String> visited = Sets.newHashSet();
    // Print depth first.
    while (!stack.isEmpty()) {
      if (first) {
        first = false;
      } else {
        appendable.append("\n");
      }
      String tableName = stack.pollFirst();
      Table table = tables.get(tableName);
      if (visited.contains(tableName)) {
        throw new IllegalStateException("Cycle!");
      }
      visited.add(tableName);
      table.prettyPrint(appendable, true, true);
      NavigableSet<String> children = childTableNames(table.name());
      if (children != null) {
        for (String name : children.descendingSet()) {
          stack.addFirst(name);
        }
      }
    }
  }

  public List<String> statements() {
    return ImmutableList.<String>builder()
        .addAll(createTableStatements())
        .addAll(createIndexStatements())
        .addAll(addForeignKeyStatements())
        .build();
  }

  public List<String> createTableStatements() {
    List<String> result = new ArrayList<>();
    LinkedList<String> stack = Lists.newLinkedList();
    stack.addAll(childTableNames(ROOT));
    Set<String> visited = Sets.newHashSet();
    // Print depth first.
    while (!stack.isEmpty()) {
      String tableName = stack.pollFirst();
      Table table = tables.get(tableName);
      if (visited.contains(tableName)) {
        throw new IllegalStateException("Cycle!");
      }
      visited.add(tableName);
      StringBuilder statement = new StringBuilder();
      try {
        table.prettyPrint(statement, false, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      result.add(statement.toString());
      NavigableSet<String> children = childTableNames(table.name());
      if (children != null) {
        for (String name : children.descendingSet()) {
          stack.addFirst(name);
        }
      }
    }
    return result;
  }

  public List<String> createIndexStatements() {
    List<String> result = new ArrayList<>();
    for (Table table : allTables()) {
      result.addAll(table.indexes());
    }
    return result;
  }

  public List<String> addForeignKeyStatements() {
    List<String> result = new ArrayList<>();
    for (Table table : allTables()) {
      result.addAll(
          table.foreignKeys().stream().map(f -> f.prettyPrint()).collect(Collectors.toList()));
    }
    return result;
  }

  public HashMultimap<Integer, String> perLevelView() {
    HashMultimap<Integer, String> result = HashMultimap.create();
    LinkedList<String> currentLevel = Lists.newLinkedList();
    currentLevel.addAll(childTableNames(ROOT));
    int depth = 0;

    while (!currentLevel.isEmpty()) {
      LinkedList<String> nextLevel = Lists.newLinkedList();
      for (String tableName : currentLevel) {
        result.put(depth, tableName);
        nextLevel.addAll(childTableNames(tableName));
      }
      currentLevel = nextLevel;
      depth++;
    }

    return result;
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  public static Builder builder() {
    return new Builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new Builder(dialect);
  }

  /** A builder for {@link Ddl}. */
  public static class Builder {

    private Map<String, Table> tables = Maps.newLinkedHashMap();
    private TreeMultimap<String, String> parents = TreeMultimap.create();
    private Dialect dialect;

    public Builder(Dialect dialect) {
      this.dialect = dialect;
    }

    public Table getTable(String name) {
      return tables.get(name.toLowerCase());
    }

    public Table.Builder createTable(String name) {
      Table table = getTable(name);
      if (table == null) {
        return Table.builder(dialect).name(name).ddlBuilder(this);
      }
      return table.toBuilder().ddlBuilder(this);
    }

    public void addTable(Table table) {
      String name = table.name().toLowerCase();
      tables.put(name, table);
      String parent =
          table.interleavingParent() == null ? ROOT : table.interleavingParent().toLowerCase();
      parents.put(parent, name);
    }

    public Ddl build() {
      return new Ddl(ImmutableSortedMap.copyOf(tables), parents, dialect);
    }
  }

  public Builder toBuilder() {
    Builder builder = new Builder(dialect);
    builder.tables.putAll(tables);
    builder.parents.putAll(parents);
    return builder;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Ddl ddl = (Ddl) o;

    if (dialect != ddl.dialect) {
      return false;
    }
    if (tables != null ? !tables.equals(ddl.tables) : ddl.tables != null) {
      return false;
    }
    return parents != null ? parents.equals(ddl.parents) : ddl.parents == null;
  }

  @Override
  public int hashCode() {
    int result = dialect != null ? dialect.hashCode() : 0;
    result = 31 * result + (tables != null ? tables.hashCode() : 0);
    result = 31 * result + (parents != null ? parents.hashCode() : 0);
    return result;
  }
}
