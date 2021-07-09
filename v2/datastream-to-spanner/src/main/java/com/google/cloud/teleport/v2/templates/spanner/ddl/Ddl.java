/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.templates.spanner.ddl;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import javax.annotation.Nullable;

/** Allows to build and introspect Cloud Spanner DDL with Java code. */
public class Ddl implements Serializable {

  private static final String ROOT = "#";
  private static final long serialVersionUID = -4153759448351855360L;

  private ImmutableSortedMap<String, Table> tables;
  private TreeMultimap<String, String> parents;

  private Ddl(ImmutableSortedMap<String, Table> tables, TreeMultimap<String, String> parents) {
    this.tables = tables;
    this.parents = parents;
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
      result.addAll(table.foreignKeys());
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
    return new Builder();
  }

  /** A builder for {@link Ddl}. */
  public static class Builder {

    private Map<String, Table> tables = Maps.newLinkedHashMap();
    private TreeMultimap<String, String> parents = TreeMultimap.create();

    public Table.Builder createTable(String name) {
      Table table = tables.get(name.toLowerCase());
      if (table == null) {
        return Table.builder().name(name).ddlBuilder(this);
      }
      return table.toBuilder().ddlBuilder(this);
    }

    public void addTable(Table table) {
      String name = table.name().toLowerCase();
      tables.put(name, table);
      String parent =
          table.interleaveInParent() == null ? ROOT : table.interleaveInParent().toLowerCase();
      parents.put(parent, name);
    }

    public Ddl build() {
      return new Ddl(ImmutableSortedMap.copyOf(tables), parents);
    }
  }

  public Builder toBuilder() {
    Builder builder = new Builder();
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

    if (tables != null ? !tables.equals(ddl.tables) : ddl.tables != null) {
      return false;
    }
    return parents != null ? parents.equals(ddl.parents) : ddl.parents == null;
  }

  @Override
  public int hashCode() {
    int result = tables != null ? tables.hashCode() : 0;
    result = 31 * result + (parents != null ? parents.hashCode() : 0);
    return result;
  }
}
