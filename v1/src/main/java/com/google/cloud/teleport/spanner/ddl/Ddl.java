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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NameUtils;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
  private ImmutableSortedMap<String, Model> models;
  private ImmutableSortedMap<String, PropertyGraph> propertyGraphs;
  private ImmutableSortedMap<String, View> views;
  private ImmutableSortedMap<String, ChangeStream> changeStreams;
  private ImmutableSortedMap<String, Sequence> sequences;
  private ImmutableSortedMap<String, Placement> placements;
  private ImmutableSortedMap<String, NamedSchema> schemas;
  private TreeMultimap<String, String> parents;
  // This is only populated by InformationSchemaScanner and not while reading from AVRO files.
  private TreeMultimap<String, String> referencedTables;
  private final ImmutableList<Export.DatabaseOption> databaseOptions;
  private final ImmutableSet<String> protoBundle;
  private final FileDescriptorSet protoDescriptors;
  private final Dialect dialect;

  private Ddl(
      ImmutableSortedMap<String, Table> tables,
      ImmutableSortedMap<String, Model> models,
      ImmutableSortedMap<String, PropertyGraph> propertyGraphs,
      ImmutableSortedMap<String, View> views,
      ImmutableSortedMap<String, ChangeStream> changeStreams,
      ImmutableSortedMap<String, Sequence> sequences,
      ImmutableSortedMap<String, Placement> placements,
      ImmutableSortedMap<String, NamedSchema> schemas,
      TreeMultimap<String, String> parents,
      TreeMultimap<String, String> referencedTables,
      ImmutableList<Export.DatabaseOption> databaseOptions,
      ImmutableSet<String> protoBundle,
      FileDescriptorSet protoDescriptors,
      Dialect dialect) {
    this.tables = tables;
    this.models = models;
    this.propertyGraphs = propertyGraphs;
    this.views = views;
    this.changeStreams = changeStreams;
    this.sequences = sequences;
    this.placements = placements;
    this.schemas = schemas;
    this.parents = parents;
    this.referencedTables = referencedTables;
    this.databaseOptions = databaseOptions;
    this.protoBundle = protoBundle;
    this.protoDescriptors = protoDescriptors;
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

  public Collection<Model> models() {
    return models.values();
  }

  public Model model(String modelName) {
    return models.get(modelName.toLowerCase());
  }

  public Collection<PropertyGraph> propertyGraphs() {
    return propertyGraphs.values();
  }

  public PropertyGraph propertyGraph(String propertyGraphName) {
    return propertyGraphs.get(propertyGraphName.toLowerCase());
  }

  public Collection<View> views() {
    return views.values();
  }

  public View view(String viewName) {
    return views.get(viewName.toLowerCase());
  }

  public Collection<ChangeStream> changeStreams() {
    return changeStreams.values();
  }

  public ChangeStream changeStream(String changeStreamName) {
    return changeStreams.get(changeStreamName.toLowerCase());
  }

  public Collection<Sequence> sequences() {
    return sequences.values();
  }

  public Sequence sequence(String sequenceName) {
    return sequences.get(sequenceName.toLowerCase());
  }

  public Collection<Placement> placements() {
    return placements.values();
  }

  public Placement placement(String placementName) {
    return placements.get(placementName.toLowerCase());
  }

  public Collection<NamedSchema> schemas() {
    return schemas.values();
  }

  public NamedSchema schema(String schemaName) {
    return schemas.get(schemaName.toLowerCase());
  }

  public ImmutableList<Export.DatabaseOption> databaseOptions() {
    return databaseOptions;
  }

  public Collection<String> protoBundle() {
    return protoBundle;
  }

  public FileDescriptorSet protoDescriptors() {
    return protoDescriptors;
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    for (Export.DatabaseOption databaseOption : databaseOptions()) {
      appendable.append(getDatabaseOptionsStatements(databaseOption, "%db_name%", dialect));
      appendable.append("\n");
    }
    // Create Proto Bundle statements should be above all other statements
    appendable.append(createProtoBundleStatement());

    for (NamedSchema schema : schemas()) {
      appendable.append("\n");
      schema.prettyPrint(appendable);
    }

    for (Sequence sequence : sequences()) {
      appendable.append("\n");
      sequence.prettyPrint(appendable);
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

    for (Model model : models()) {
      appendable.append("\n");
      model.prettyPrint(appendable);
    }

    for (PropertyGraph graph : propertyGraphs()) {
      appendable.append("\n");
      graph.prettyPrint(appendable);
    }

    for (View view : views()) {
      appendable.append("\n");
      view.prettyPrint(appendable);
    }

    for (ChangeStream changeStream : changeStreams()) {
      appendable.append("\n");
      changeStream.prettyPrint(appendable);
    }

    for (Placement placement : placements()) {
      appendable.append("\n");
      placement.prettyPrint(appendable);
    }
  }

  public List<String> statements() {
    // CREATE SEQUENCE statements have to be before CREATE TABLE statements.
    ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
    // Create Proto Bundle statements should be above all other statements
    if (!protoBundle().isEmpty()) {
      builder.add(createProtoBundleStatement());
    }
    builder
        .addAll(createNamedSchemaStatements())
        .addAll(createSequenceStatements())
        .addAll(createTableStatements())
        .addAll(createIndexStatements())
        .addAll(addForeignKeyStatements())
        .addAll(createModelStatements())
        .addAll(createPropertyGraphStatements())
        .addAll(createViewStatements())
        .addAll(createChangeStreamStatements())
        .addAll(createPlacementStatements())
        .addAll(setOptionsStatements("%db_name%"));
    return builder.build();
  }

  public List<String> createNamedSchemaStatements() {
    List<String> result = new ArrayList<>();
    for (NamedSchema schema : schemas()) {
      result.add(schema.prettyPrint());
    }
    return result;
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

  public List<String> createModelStatements() {
    List<String> result = new ArrayList<>(models.size());
    for (Model model : models.values()) {
      result.add(model.prettyPrint());
    }
    return result;
  }

  public List<String> createPropertyGraphStatements() {
    List<String> result = new ArrayList<>(propertyGraphs.size());
    for (PropertyGraph propertyGraph : propertyGraphs.values()) {
      result.add(propertyGraph.prettyPrint());
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

  public List<String> createChangeStreamStatements() {
    List<String> result = new ArrayList<>(changeStreams.size());
    for (ChangeStream changeStream : changeStreams()) {
      result.add(changeStream.prettyPrint());
    }
    return result;
  }

  public List<String> createSequenceStatements() {
    List<String> result = new ArrayList<>(sequences.size());
    for (Sequence sequence : sequences()) {
      result.add(sequence.prettyPrint());
    }
    return result;
  }

  public List<String> createPlacementStatements() {
    List<String> result = new ArrayList<>(placements.size());
    for (Placement placement : placements()) {
      result.add(placement.prettyPrint());
    }
    return result;
  }

  public String createProtoBundleStatement() {
    StringBuilder appendable = new StringBuilder();
    String quote = NameUtils.identifierQuote(dialect);
    if (!protoBundle.isEmpty()) {
      appendable.append("CREATE PROTO BUNDLE (");
      for (String protoTypeFqn : protoBundle()) {
        appendable.append("\n\t");
        appendable.append(quote + protoTypeFqn + quote);
        appendable.append(",");
      }
      appendable.append(")");
    }
    return appendable.toString();
  }

  public List<String> setOptionsStatements(String databaseId) {
    List<String> result = new ArrayList<>();
    for (Export.DatabaseOption databaseOption : databaseOptions()) {
      result.add(getDatabaseOptionsStatements(databaseOption, databaseId, dialect));
    }
    return result;
  }

  private static String getDatabaseOptionsStatements(
      Export.DatabaseOption databaseOption, String databaseId, Dialect dialect) {
    String literalQuote = NameUtils.literalQuote(dialect);
    String optionType = databaseOption.getOptionType();
    String formattedValue =
        (optionType.equalsIgnoreCase("STRING") || optionType.equalsIgnoreCase("character varying"))
            ? literalQuote
                + NameUtils.OPTION_STRING_ESCAPER.escape(databaseOption.getOptionValue())
                + literalQuote
            : databaseOption.getOptionValue();
    String statement;
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            String.format(
                "ALTER DATABASE `%s` SET OPTIONS ( %s = %s )",
                databaseId, databaseOption.getOptionName(), formattedValue);
        break;
      case POSTGRESQL:
        statement =
            String.format(
                "ALTER DATABASE \"%s\" SET spanner.%s = %s",
                databaseId, databaseOption.getOptionName(), formattedValue);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s", dialect));
    }
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
    return new Builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new Builder(dialect);
  }

  /** A builder for {@link Ddl}. */
  public static class Builder {

    private Map<String, Table> tables = Maps.newLinkedHashMap();
    private Map<String, Model> models = Maps.newLinkedHashMap();
    private Map<String, PropertyGraph> propertyGraphs = Maps.newLinkedHashMap();
    private Map<String, View> views = Maps.newLinkedHashMap();
    private Map<String, ChangeStream> changeStreams = Maps.newLinkedHashMap();
    private Map<String, Sequence> sequences = Maps.newLinkedHashMap();
    private Map<String, Placement> placements = Maps.newLinkedHashMap();
    private Map<String, NamedSchema> schemas = Maps.newLinkedHashMap();
    private TreeMultimap<String, String> parents = TreeMultimap.create();
    private TreeMultimap<String, String> referencedTables = TreeMultimap.create();
    private ImmutableList<Export.DatabaseOption> databaseOptions = ImmutableList.of();
    private ImmutableSet<String> protoBundle = ImmutableSet.of();
    private FileDescriptorSet protoDescriptors;
    private Dialect dialect;

    public Builder(Dialect dialect) {
      this.dialect = dialect;
    }

    public Table.Builder createTable(String name) {
      Table table = tables.get(name.toLowerCase());
      if (table == null) {
        return Table.builder(dialect).name(name).ddlBuilder(this);
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

    public Model.Builder createModel(String name) {
      Model model = models.get(name.toLowerCase());
      if (model == null) {
        return Model.builder(dialect).name(name).ddlBuilder(this);
      }
      return model.toBuilder().ddlBuilder(this);
    }

    public void addModel(Model model) {
      models.put(model.name().toLowerCase(), model);
    }

    public boolean hasModel(String name) {
      return models.containsKey(name.toLowerCase());
    }

    public PropertyGraph.Builder createPropertyGraph(String name) {
      PropertyGraph graph = propertyGraphs.get(name.toLowerCase());
      if (graph == null) {
        return PropertyGraph.builder(dialect).name(name).ddlBuilder(this);
      }
      return graph.toBuilder().ddlBuilder(this);
    }

    public void addPropertyGraph(PropertyGraph graph) {
      propertyGraphs.put(graph.name().toLowerCase(), graph);
    }

    public boolean hasPropertyGraph(String name) {
      return propertyGraphs.containsKey(name.toLowerCase());
    }

    public Collection<PropertyGraph> propertyGraphs() {
      return propertyGraphs.values();
    }

    public View.Builder createView(String name) {
      View view = views.get(name.toLowerCase());
      if (view == null) {
        return View.builder(dialect).name(name).ddlBuilder(this);
      }
      return view.toBuilder().ddlBuilder(this);
    }

    public void addView(View view) {
      views.put(view.name().toLowerCase(), view);
    }

    public boolean hasView(String name) {
      return views.containsKey(name.toLowerCase());
    }

    public ChangeStream.Builder createChangeStream(String name) {
      ChangeStream changeStream = changeStreams.get(name.toLowerCase());
      if (changeStream == null) {
        return ChangeStream.builder(dialect).name(name).ddlBuilder(this);
      }
      return changeStream.toBuilder().ddlBuilder(this);
    }

    public void addChangeStream(ChangeStream changeStream) {
      changeStreams.put(changeStream.name().toLowerCase(), changeStream);
    }

    public boolean hasChangeStream(String name) {
      return changeStreams.containsKey(name.toLowerCase());
    }

    public Sequence.Builder createSequence(String name) {
      Sequence sequence = sequences.get(name.toLowerCase());
      if (sequence == null) {
        return Sequence.builder(dialect).name(name).ddlBuilder(this);
      }
      return sequence.toBuilder().ddlBuilder(this);
    }

    public void addSequence(Sequence sequence) {
      sequences.put(sequence.name().toLowerCase(), sequence);
    }

    public Placement.Builder createPlacement(String name) {
      Placement placement = placements.get(name.toLowerCase());
      if (placement == null) {
        return Placement.builder(dialect).name(name).ddlBuilder(this);
      }
      return placement.toBuilder().ddlBuilder(this);
    }

    public void addPlacement(Placement placement) {
      placements.put(placement.name().toLowerCase(), placement);
    }

    public boolean hasPlacement(String name) {
      return placements.containsKey(name.toLowerCase());
    }

    public NamedSchema.Builder createSchema(String name) {
      NamedSchema schema = schemas.get(name.toLowerCase());
      if (schema == null) {
        return NamedSchema.builder(dialect).name(name).ddlBuilder(this);
      }
      return schema.toBuilder().ddlBuilder(this);
    }

    public void addSchema(NamedSchema schema) {
      schemas.put(schema.name().toLowerCase(), schema);
    }

    public boolean hasSequence(String name) {
      return sequences.containsKey(name.toLowerCase());
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

    public void mergeProtoBundle(Set<String> protoBundle) {
      Set<String> newProtoBundle = new HashSet<>();
      newProtoBundle.addAll(this.protoBundle);
      newProtoBundle.addAll(protoBundle);
      this.protoBundle = ImmutableSet.copyOf(newProtoBundle);
    }

    public void mergeProtoDescriptors(FileDescriptorSet protoDescriptors) {
      FileDescriptorSet.Builder newProtoDescriptors = FileDescriptorSet.newBuilder();
      if (this.protoDescriptors != null) {
        newProtoDescriptors.addAllFile(this.protoDescriptors.getFileList());
      }
      newProtoDescriptors.addAllFile(protoDescriptors.getFileList());
      this.protoDescriptors = newProtoDescriptors.build();
    }

    public Ddl build() {
      return new Ddl(
          ImmutableSortedMap.copyOf(tables),
          ImmutableSortedMap.copyOf(models),
          ImmutableSortedMap.copyOf(propertyGraphs),
          ImmutableSortedMap.copyOf(views),
          ImmutableSortedMap.copyOf(changeStreams),
          ImmutableSortedMap.copyOf(sequences),
          ImmutableSortedMap.copyOf(placements),
          ImmutableSortedMap.copyOf(schemas),
          parents,
          referencedTables,
          databaseOptions,
          protoBundle,
          protoDescriptors,
          dialect);
    }
  }

  public Builder toBuilder() {
    Builder builder = new Builder(dialect);
    builder.schemas.putAll(schemas);
    builder.tables.putAll(tables);
    builder.models.putAll(models);
    builder.propertyGraphs.putAll(propertyGraphs);
    builder.views.putAll(views);
    builder.changeStreams.putAll(changeStreams);
    builder.sequences.putAll(sequences);
    builder.placements.putAll(placements);
    builder.parents.putAll(parents);
    builder.referencedTables.putAll(referencedTables);
    builder.databaseOptions = databaseOptions;
    builder.protoBundle = protoBundle;
    builder.protoDescriptors = protoDescriptors;
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
    if (parents != null ? !parents.equals(ddl.parents) : ddl.parents != null) {
      return false;
    }
    if (referencedTables != null
        ? !referencedTables.equals(ddl.referencedTables)
        : ddl.referencedTables != null) {
      return false;
    }
    if (models != null ? !models.equals(ddl.models) : ddl.models != null) {
      return false;
    }
    if (propertyGraphs != null
        ? !propertyGraphs.equals(ddl.propertyGraphs)
        : ddl.propertyGraphs != null) {
      return false;
    }
    if (views != null ? !views.equals(ddl.views) : ddl.views != null) {
      return false;
    }
    if (changeStreams != null
        ? !changeStreams.equals(ddl.changeStreams)
        : ddl.changeStreams != null) {
      return false;
    }
    if (sequences != null ? !sequences.equals(ddl.sequences) : ddl.sequences != null) {
      return false;
    }
    if (placements != null ? !placements.equals(ddl.placements) : ddl.placements != null) {
      return false;
    }
    if (protoDescriptors != null
        ? !protoDescriptors.equals(ddl.protoDescriptors)
        : ddl.protoDescriptors != null) {
      return false;
    }
    return databaseOptions.equals(ddl.databaseOptions) && protoBundle.equals(ddl.protoBundle);
  }

  @Override
  public int hashCode() {
    int result = dialect != null ? dialect.hashCode() : 0;
    result = 31 * result + (tables != null ? tables.hashCode() : 0);
    result = 31 * result + (parents != null ? parents.hashCode() : 0);
    result = 31 * result + (referencedTables != null ? referencedTables.hashCode() : 0);
    result = 31 * result + (models != null ? models.hashCode() : 0);
    result = 31 * result + (propertyGraphs != null ? propertyGraphs.hashCode() : 0);
    result = 31 * result + (views != null ? views.hashCode() : 0);
    result = 31 * result + (changeStreams != null ? changeStreams.hashCode() : 0);
    result = 31 * result + (sequences != null ? sequences.hashCode() : 0);
    result = 31 * result + (placements != null ? placements.hashCode() : 0);
    result = 31 * result + (databaseOptions != null ? databaseOptions.hashCode() : 0);
    result = 31 * result + (protoBundle != null ? protoBundle.hashCode() : 0);
    result = 31 * result + (protoDescriptors != null ? protoDescriptors.hashCode() : 0);
    return result;
  }
}
