/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import com.google.cloud.teleport.spanner.ExportProtos.Export;
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
  private ImmutableSortedMap<String, View> views;
  private TreeMultimap<String, String> parents;
  // This is only populated by InformationSchemaScanner and not while reading from AVRO files.
  private TreeMultimap<String, String> referencedTables;
  private final ImmutableList<Export.DatabaseOption> databaseOptions;

  private Ddl(
      ImmutableSortedMap<String, Table> tables,
      ImmutableSortedMap<String, View> views,
      TreeMultimap<String, String> parents,
      TreeMultimap<String, String> referencedTables,
      ImmutableList<Export.DatabaseOption> databaseOptions) {
    this.tables = tables;
    this.views = views;
    this.parents = parents;
    this.referencedTables = referencedTables;
    this.databaseOptions = databaseOptions;
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

  public Collection<Table> allReferencedTables(String table) {
    return Collections2.transform(
        referencedTableNames(table),
        new Function<String, Table>() {

          @Nullable
          @Override
          public Table apply(@Nullable String input) {
            return table(input);
          }
        });
  }

  private NavigableSet<String> referencedTableNames(String table) {
    return referencedTables.get(table.toLowerCase());
  }

  public void addNewReferencedTable(String table, String referencedTable) {
    referencedTables.put(table.toLowerCase(), referencedTable.toLowerCase());
  }


  public Table table(String tableName) {
    return tables.get(tableName.toLowerCase());
  }

  public Collection<View> views() {
    return views.values();
  }

  public View view(String viewName) {
    return views.get(viewName.toLowerCase());
  }

  public ImmutableList<Export.DatabaseOption> databaseOptions() {
    return databaseOptions;
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    for (Export.DatabaseOption databaseOption : databaseOptions()) {
      appendable.append(getDatabaseOptionsStatements(databaseOption, "%db_name%"));
      appendable.append("\n");
    }

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

    for (View view : views()) {
      appendable.append("\n");
      view.prettyPrint(appendable);
    }
  }

  public List<String> statements() {
    return ImmutableList.<String>builder()
        .addAll(createTableStatements())
        .addAll(createIndexStatements())
        .addAll(addForeignKeyStatements())
        .addAll(createViewStatements())
        .addAll(setOptionsStatements("%db_name%"))
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

  public List<String> createViewStatements() {
    List<String> result = new ArrayList<>(views.size());
    for (View view : views.values()) {
      result.add(view.prettyPrint());
    }
    return result;
  }

  public List<String> setOptionsStatements(String databaseId) {
    List<String> result = new ArrayList<>();
    for (Export.DatabaseOption databaseOption : databaseOptions()) {
      result.add(getDatabaseOptionsStatements(databaseOption, databaseId));
    }
    return result;
  }

  private static String getDatabaseOptionsStatements(
      Export.DatabaseOption databaseOption, String databaseId) {
    String formattedValue =
        databaseOption.getOptionType().equalsIgnoreCase("STRING")
            ? "\""
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(databaseOption.getOptionValue())
                + "\""
            : databaseOption.getOptionValue();

    String statement =
        String.format(
            "ALTER DATABASE `%s` SET OPTIONS ( %s = %s )",
            databaseId, databaseOption.getOptionName(), formattedValue);
    return statement;
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
    private Map<String, View> views = Maps.newLinkedHashMap();
    private TreeMultimap<String, String> parents = TreeMultimap.create();
    private TreeMultimap<String, String> referencedTables = TreeMultimap.create();
    private ImmutableList<Export.DatabaseOption> databaseOptions = ImmutableList.of();

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

    public void addReferencedTable(String table, String referencedTable) {
      referencedTables.put(table.toLowerCase(), referencedTable.toLowerCase());
    }

    public Collection<Table> tables() {
      return tables.values();
    }

    public View.Builder createView(String name) {
      View view = views.get(name.toLowerCase());
      if (view == null) {
        return View.builder().name(name).ddlBuilder(this);
      }
      return view.toBuilder().ddlBuilder(this);
    }

    public void addView(View view) {
      views.put(view.name().toLowerCase(), view);
    }

    public boolean hasView(String name) {
      return views.containsKey(name.toLowerCase());
    }

    public void mergeDatabaseOptions(List<Export.DatabaseOption> databaseOptions) {
      List<Export.DatabaseOption> allowedDatabaseOptions = new ArrayList<>();
      List<String> existingOptionNames = new ArrayList<>();
      for (Export.DatabaseOption databaseOption : databaseOptions) {
        if (!DatabaseOptionAllowlist.DATABASE_OPTION_ALLOWLIST.contains(
            databaseOption.getOptionName())) {
          continue;
        }
        allowedDatabaseOptions.add(databaseOption);
        existingOptionNames.add(databaseOption.getOptionName());
      }
      for (Export.DatabaseOption databaseOption : this.databaseOptions) {
        if (!existingOptionNames.contains(databaseOption.getOptionName())) {
          allowedDatabaseOptions.add(databaseOption);
        }
      }
      this.databaseOptions = ImmutableList.copyOf(allowedDatabaseOptions);
    }

    public Ddl build() {
      return new Ddl(
          ImmutableSortedMap.copyOf(tables),
          ImmutableSortedMap.copyOf(views),
          parents,
          referencedTables,
          databaseOptions);
    }
  }

  public Builder toBuilder() {
    Builder builder = new Builder();
    builder.tables.putAll(tables);
    builder.views.putAll(views);
    builder.parents.putAll(parents);
    builder.referencedTables.putAll(referencedTables);
    builder.databaseOptions = databaseOptions;
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
    if (parents != null ? !parents.equals(ddl.parents) : ddl.parents != null) {
      return false;
    }
    if (referencedTables != null ? !referencedTables.equals(ddl.referencedTables)
            : ddl.referencedTables != null) {
      return false;
    }
    if (views != null ? !views.equals(ddl.views) : ddl.views != null) {
      return false;
    }
    return databaseOptions.equals(ddl.databaseOptions);
  }

  @Override
  public int hashCode() {
    int result = tables != null ? tables.hashCode() : 0;
    result = 31 * result + (parents != null ? parents.hashCode() : 0);
    result = 31 * result + (referencedTables != null ? referencedTables.hashCode() : 0);
    result = 31 * result + (views != null ? views.hashCode() : 0);
    result = 31 * result + (databaseOptions != null ? databaseOptions.hashCode() : 0);
    return result;
  }
}
