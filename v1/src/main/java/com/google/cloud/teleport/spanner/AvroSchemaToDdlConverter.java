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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.AvroUtil.DEFAULT_EXPRESSION;
import static com.google.cloud.teleport.spanner.AvroUtil.GENERATION_EXPRESSION;
import static com.google.cloud.teleport.spanner.AvroUtil.HIDDEN;
import static com.google.cloud.teleport.spanner.AvroUtil.INPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.NOT_NULL;
import static com.google.cloud.teleport.spanner.AvroUtil.OUTPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHANGE_STREAM_FOR_CLAUSE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHECK_CONSTRAINT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_EDGE_TABLE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY_MODEL;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY_PLACEMENT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY_PROPERTY_GRAPH;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_FOREIGN_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_INDEX;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_LABEL;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_NAMED_SCHEMA;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_NODE_TABLE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ON_DELETE_ACTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_OPTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PARENT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PLACEMENT_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PRIMARY_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PROPERTY_DECLARATION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_REMOTE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_COUNTER_START;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_KIND;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_OPTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_SKIP_RANGE_MAX;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_SKIP_RANGE_MIN;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_QUERY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_SECURITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SQL_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.STORED;
import static com.google.cloud.teleport.spanner.AvroUtil.getSpannerObjectName;
import static com.google.cloud.teleport.spanner.AvroUtil.unpackNullable;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.ChangeStream;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.GraphElementTable;
import com.google.cloud.teleport.spanner.ddl.GraphElementTable.Kind;
import com.google.cloud.teleport.spanner.ddl.Model;
import com.google.cloud.teleport.spanner.ddl.NamedSchema;
import com.google.cloud.teleport.spanner.ddl.Placement;
import com.google.cloud.teleport.spanner.ddl.PropertyGraph;
import com.google.cloud.teleport.spanner.ddl.PropertyGraph.PropertyDeclaration;
import com.google.cloud.teleport.spanner.ddl.Sequence;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.ddl.View;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Allows to convert a set of Avro schemas to {@link Ddl}. */
public class AvroSchemaToDdlConverter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaToDdlConverter.class);
  private final Dialect dialect;

  public AvroSchemaToDdlConverter() {
    this.dialect = Dialect.GOOGLE_STANDARD_SQL;
  }

  public AvroSchemaToDdlConverter(Dialect dialect) {
    this.dialect = dialect;
  }

  public Ddl toDdl(Collection<Schema> avroSchemas) {
    Ddl.Builder builder = Ddl.builder(dialect);
    for (Schema schema : avroSchemas) {
      if (schema.getProp(SPANNER_VIEW_QUERY) != null) {
        builder.addView(toView(null, schema));
      } else if (SPANNER_ENTITY_MODEL.equals(schema.getProp(SPANNER_ENTITY))) {
        builder.addModel(toModel(null, schema));
      } else if (schema.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE) != null) {
        builder.addChangeStream(toChangeStream(null, schema));
      } else if (schema.getProp(SPANNER_SEQUENCE_OPTION + "0") != null
          || schema.getProp(SPANNER_SEQUENCE_KIND) != null) {
        // Cloud Sequence always requires at least one option,
        // `sequence_kind='bit_reversed_positive`, so `sequenceOption_0` must
        // always be valid.
        builder.addSequence(toSequence(null, schema));
      } else if (SPANNER_NAMED_SCHEMA.equals(schema.getProp(SPANNER_ENTITY))) {
        builder.addSchema(toSchema(null, schema));
      } else if (SPANNER_ENTITY_PLACEMENT.equals(schema.getProp(SPANNER_ENTITY))) {
        builder.addPlacement(toPlacement(null, schema));
      } else if (SPANNER_ENTITY_PROPERTY_GRAPH.equals(schema.getProp(SPANNER_ENTITY))) {
        builder.addPropertyGraph(toPropertyGraph(null, schema));
      } else {
        builder.addTable(toTable(null, schema));
      }
    }
    return builder.build();
  }

  public NamedSchema toSchema(String schemaName, Schema schema) {
    if (schemaName == null) {
      schemaName = schema.getName();
    }
    NamedSchema.Builder builder = NamedSchema.builder().dialect(dialect).name(schemaName);
    return builder.build();
  }

  public View toView(String viewName, Schema schema) {
    if (viewName == null) {
      viewName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl viewName {}", viewName);

    View.Builder builder =
        View.builder(dialect).name(viewName).query(schema.getProp(SPANNER_VIEW_QUERY));
    if (schema.getProp(SPANNER_VIEW_SECURITY) != null) {
      builder.security(View.SqlSecurity.valueOf(schema.getProp(SPANNER_VIEW_SECURITY)));
    }
    return builder.build();
  }

  // TODO: Modularize long function implementation using helpers
  public PropertyGraph toPropertyGraph(String propertyGraphName, Schema schema) {
    PropertyGraph.Builder propertyGraphBuilder = PropertyGraph.builder();
    if (propertyGraphName == null) {
      propertyGraphName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl propertyGraphName {}", propertyGraphName);

    propertyGraphBuilder.name(propertyGraphName);

    // Deserialize nodeTables
    int nodeTableCount = 0;
    while (schema.getProp(SPANNER_NODE_TABLE + "_" + nodeTableCount + "_NAME") != null) {
      GraphElementTable.Builder nodeTableBuilder = GraphElementTable.builder();
      nodeTableBuilder.name(schema.getProp(SPANNER_NODE_TABLE + "_" + nodeTableCount + "_NAME"));
      nodeTableBuilder.baseTableName(
          schema.getProp(SPANNER_NODE_TABLE + "_" + nodeTableCount + "_BASE_TABLE_NAME"));
      nodeTableBuilder.kind(Kind.NODE);

      // Deserialize keyColumns
      String[] keyColumns =
          schema
              .getProp(SPANNER_NODE_TABLE + "_" + nodeTableCount + "_KEY_COLUMNS")
              .trim()
              .split(",");
      nodeTableBuilder.keyColumns(ImmutableList.copyOf(keyColumns));

      // Deserialize labelToPropertyDefinitions
      int labelCount = 0;
      ImmutableList.Builder<GraphElementTable.LabelToPropertyDefinitions> labelsBuilder =
          ImmutableList.builder();
      while (schema.getProp(
              SPANNER_NODE_TABLE + "_" + nodeTableCount + "_LABEL_" + labelCount + "_NAME")
          != null) {
        String labelName =
            schema.getProp(
                SPANNER_NODE_TABLE + "_" + nodeTableCount + "_LABEL_" + labelCount + "_NAME");
        ImmutableList.Builder<GraphElementTable.PropertyDefinition> propertyDefinitionsBuilder =
            ImmutableList.builder();
        int propertyCount = 0;
        while (schema.getProp(
                SPANNER_NODE_TABLE
                    + "_"
                    + nodeTableCount
                    + "_LABEL_"
                    + labelCount
                    + "_PROPERTY_"
                    + propertyCount
                    + "_NAME")
            != null) {
          String propertyName =
              schema.getProp(
                  SPANNER_NODE_TABLE
                      + "_"
                      + nodeTableCount
                      + "_LABEL_"
                      + labelCount
                      + "_PROPERTY_"
                      + propertyCount
                      + "_NAME");
          String propertyValue =
              schema.getProp(
                  SPANNER_NODE_TABLE
                      + "_"
                      + nodeTableCount
                      + "_LABEL_"
                      + labelCount
                      + "_PROPERTY_"
                      + propertyCount
                      + "_VALUE");
          propertyDefinitionsBuilder.add(
              new GraphElementTable.PropertyDefinition(propertyName, propertyValue));
          propertyCount++;
        }
        labelsBuilder.add(
            new GraphElementTable.LabelToPropertyDefinitions(
                labelName, propertyDefinitionsBuilder.build()));
        labelCount++;
      }
      nodeTableBuilder.labelToPropertyDefinitions(labelsBuilder.build());

      propertyGraphBuilder.addNodeTable(nodeTableBuilder.autoBuild());
      nodeTableCount++;
    }

    // Deserialize edgeTables (similar logic to nodeTables)
    int edgeTableCount = 0;
    while (schema.getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_NAME") != null) {
      GraphElementTable.Builder edgeTableBuilder = GraphElementTable.builder();
      edgeTableBuilder.name(schema.getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_NAME"));
      edgeTableBuilder.baseTableName(
          schema.getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_BASE_TABLE_NAME"));
      edgeTableBuilder.kind(Kind.EDGE);

      // Deserialize keyColumns
      String[] keyColumns =
          schema
              .getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_KEY_COLUMNS")
              .trim()
              .split(",");
      edgeTableBuilder.keyColumns(ImmutableList.copyOf(keyColumns));

      // Deserialize labelToPropertyDefinitions
      int labelCount = 0;
      ImmutableList.Builder<GraphElementTable.LabelToPropertyDefinitions> labelsBuilder =
          ImmutableList.builder();
      while (schema.getProp(
              SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_LABEL_" + labelCount + "_NAME")
          != null) {
        String labelName =
            schema.getProp(
                SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_LABEL_" + labelCount + "_NAME");
        ImmutableList.Builder<GraphElementTable.PropertyDefinition> propertyDefinitionsBuilder =
            ImmutableList.builder();
        int propertyCount = 0;
        while (schema.getProp(
                SPANNER_EDGE_TABLE
                    + "_"
                    + edgeTableCount
                    + "_LABEL_"
                    + labelCount
                    + "_PROPERTY_"
                    + propertyCount
                    + "_NAME")
            != null) {
          String propertyName =
              schema.getProp(
                  SPANNER_EDGE_TABLE
                      + "_"
                      + edgeTableCount
                      + "_LABEL_"
                      + labelCount
                      + "_PROPERTY_"
                      + propertyCount
                      + "_NAME");
          String propertyValue =
              schema.getProp(
                  SPANNER_EDGE_TABLE
                      + "_"
                      + edgeTableCount
                      + "_LABEL_"
                      + labelCount
                      + "_PROPERTY_"
                      + propertyCount
                      + "_VALUE");
          propertyDefinitionsBuilder.add(
              new GraphElementTable.PropertyDefinition(propertyName, propertyValue));
          propertyCount++;
        }
        labelsBuilder.add(
            new GraphElementTable.LabelToPropertyDefinitions(
                labelName, propertyDefinitionsBuilder.build()));
        labelCount++;
      }
      edgeTableBuilder.labelToPropertyDefinitions(labelsBuilder.build());

      // Deserialize sourceNodeTable and targetNodeTable (always present for edges)
      String sourceNodeTableName =
          schema.getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_SOURCE_NODE_TABLE_NAME");
      String[] sourceNodeKeyColumns =
          schema
              .getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_SOURCE_NODE_KEY_COLUMNS")
              .trim()
              .split(",");
      String[] sourceEdgeKeyColumns =
          schema
              .getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_SOURCE_EDGE_KEY_COLUMNS")
              .trim()
              .split(",");
      edgeTableBuilder.sourceNodeTable(
          new GraphElementTable.GraphNodeTableReference(
              sourceNodeTableName,
              ImmutableList.copyOf(sourceNodeKeyColumns),
              ImmutableList.copyOf(sourceEdgeKeyColumns)));

      String targetNodeTableName =
          schema.getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_TARGET_NODE_TABLE_NAME");
      String[] targetNodeKeyColumns =
          schema
              .getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_TARGET_NODE_KEY_COLUMNS")
              .trim()
              .split(",");
      String[] targetEdgeKeyColumns =
          schema
              .getProp(SPANNER_EDGE_TABLE + "_" + edgeTableCount + "_TARGET_EDGE_KEY_COLUMNS")
              .trim()
              .split(",");
      edgeTableBuilder.targetNodeTable(
          new GraphElementTable.GraphNodeTableReference(
              targetNodeTableName,
              ImmutableList.copyOf(targetNodeKeyColumns),
              ImmutableList.copyOf(targetEdgeKeyColumns)));

      propertyGraphBuilder.addEdgeTable(edgeTableBuilder.autoBuild());
      edgeTableCount++;
    }

    // Deserialize propertyDeclarations
    int propertyDeclCount = 0;
    ImmutableList.Builder<PropertyGraph.PropertyDeclaration> propertyDeclsBuilder =
        ImmutableList.builder();
    while (schema.getProp(SPANNER_PROPERTY_DECLARATION + "_" + propertyDeclCount + "_NAME")
        != null) {
      String propertyName =
          schema.getProp(SPANNER_PROPERTY_DECLARATION + "_" + propertyDeclCount + "_NAME");
      String propertyType =
          schema.getProp(SPANNER_PROPERTY_DECLARATION + "_" + propertyDeclCount + "_TYPE");
      propertyGraphBuilder.addPropertyDeclaration(
          new PropertyDeclaration(propertyName, propertyType));
      propertyDeclCount++;
    }

    // Deserialize labels
    int labelCount = 0;
    ImmutableList.Builder<PropertyGraph.GraphElementLabel> labelsBuilder = ImmutableList.builder();
    while (schema.getProp(SPANNER_LABEL + "_" + labelCount + "_NAME") != null) {
      String labelName = schema.getProp(SPANNER_LABEL + "_" + labelCount + "_NAME");
      ImmutableList.Builder<String> labelPropertiesBuilder = ImmutableList.builder();
      int propertyCount = 0;
      while (schema.getProp(SPANNER_LABEL + "_" + labelCount + "_PROPERTY_" + propertyCount)
          != null) {
        labelPropertiesBuilder.add(
            schema.getProp(SPANNER_LABEL + "_" + labelCount + "_PROPERTY_" + propertyCount));
        propertyCount++;
      }
      propertyGraphBuilder.addLabel(
          new PropertyGraph.GraphElementLabel(labelName, labelPropertiesBuilder.build()));
      labelCount++;
    }

    return propertyGraphBuilder.build();
  }

  public Model toModel(String modelName, Schema schema) {
    if (modelName == null) {
      modelName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl modelName {}", modelName);

    Model.Builder builder = Model.builder(dialect);
    builder.name(modelName);
    builder.remote(Boolean.parseBoolean(schema.getProp(SPANNER_REMOTE)));
    builder.options(toOptionsList(schema));

    for (Schema.Field f : schema.getFields()) {
      if (f.name().equals(INPUT)) {
        for (Schema.Field c : f.schema().getFields()) {
          builder
              .inputColumn(c.name())
              .parseType(c.getProp(SQL_TYPE))
              .columnOptions(toOptionsList(c))
              .endInputColumn();
        }
      } else if (f.name().equals(OUTPUT)) {
        for (Schema.Field c : f.schema().getFields()) {
          builder
              .outputColumn(c.name())
              .parseType(c.getProp(SQL_TYPE))
              .columnOptions(toOptionsList(c))
              .endOutputColumn();
        }
      } else {
        throw new IllegalArgumentException("Unexpected model field " + f.name());
      }
    }

    return builder.build();
  }

  private ImmutableList<String> toOptionsList(JsonProperties json) {
    ImmutableList.Builder<String> columnOptions = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String spannerOption = json.getProp(SPANNER_OPTION + i);
      if (spannerOption == null) {
        break;
      }
      columnOptions.add(spannerOption);
    }
    return columnOptions.build();
  }

  public ChangeStream toChangeStream(String changeStreamName, Schema schema) {
    if (changeStreamName == null) {
      changeStreamName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl changeStreamName {}", changeStreamName);

    ChangeStream.Builder builder =
        ChangeStream.builder(dialect)
            .name(changeStreamName)
            .forClause(schema.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE));

    ImmutableList.Builder<String> changeStreamOptions = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String spannerOption = schema.getProp(SPANNER_OPTION + i);
      if (spannerOption == null) {
        break;
      }
      changeStreamOptions.add(spannerOption);
    }
    builder.options(changeStreamOptions.build());

    return builder.build();
  }

  public Sequence toSequence(String sequenceName, Schema schema) {
    if (sequenceName == null) {
      sequenceName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl sequenceName {}", sequenceName);
    Sequence.Builder builder = Sequence.builder(dialect).name(sequenceName);

    if (schema.getProp(SPANNER_SEQUENCE_KIND) != null) {
      builder.sequenceKind(schema.getProp(SPANNER_SEQUENCE_KIND));
    }
    if (schema.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MIN) != null
        && schema.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MAX) != null) {
      builder
          .skipRangeMin(Long.valueOf(schema.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MIN)))
          .skipRangeMax(Long.valueOf(schema.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MAX)));
    }
    if (schema.getProp(SPANNER_SEQUENCE_COUNTER_START) != null) {
      builder.counterStartValue(Long.valueOf(schema.getProp(SPANNER_SEQUENCE_COUNTER_START)));
    }

    ImmutableList.Builder<String> sequenceOptions = ImmutableList.builder();
    for (int i = 0; schema.getProp(SPANNER_SEQUENCE_OPTION + i) != null; i++) {
      sequenceOptions.add(schema.getProp(SPANNER_SEQUENCE_OPTION + i));
    }
    builder.options(sequenceOptions.build());

    return builder.build();
  }

  public Placement toPlacement(String placementName, Schema schema) {
    if (placementName == null) {
      placementName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl placementName {}", placementName);

    Placement.Builder builder = Placement.builder(dialect).name(placementName);

    ImmutableList.Builder<String> placementOptions = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String spannerOption = schema.getProp(SPANNER_OPTION + i);
      if (spannerOption == null) {
        break;
      }
      placementOptions.add(spannerOption);
    }
    builder.options(placementOptions.build());

    return builder.build();
  }

  public Table toTable(String tableName, Schema schema) {
    if (tableName == null) {
      tableName = getSpannerObjectName(schema);
    }
    LOG.debug("Converting to Ddl tableName {}", tableName);

    Table.Builder table = Table.builder(dialect);
    table.name(tableName);
    for (Schema.Field f : schema.getFields()) {
      Column.Builder column = table.column(f.name());
      String sqlType = f.getProp(SQL_TYPE);
      String expression = f.getProp(GENERATION_EXPRESSION);
      if (expression != null) {
        // This is a generated column.
        if (Strings.isNullOrEmpty(sqlType)) {
          throw new IllegalArgumentException(
              "Property sqlType is missing for generated column " + f.name());
        }
        String notNull = f.getProp(NOT_NULL);
        if (notNull == null) {
          throw new IllegalArgumentException(
              "Property notNull is missing for generated column " + f.name());
        }
        column.parseType(sqlType).notNull(Boolean.parseBoolean(notNull)).generatedAs(expression);
        String stored = f.getProp(STORED);
        if (stored == null) {
          throw new IllegalArgumentException(
              "Property stored is missing for generated column " + f.name());
        }
        if (Boolean.parseBoolean(stored)) {
          column.stored();
        }
      } else {
        boolean nullable = false;
        Schema avroType = f.schema();
        if (avroType.getType() == Schema.Type.UNION) {
          Schema unpacked = unpackNullable(avroType);
          nullable = unpacked != null;
          if (nullable) {
            avroType = unpacked;
          }
        }
        if (Strings.isNullOrEmpty(sqlType)) {
          Type spannerType = inferType(avroType, true);
          sqlType = toString(spannerType, true);
        }
        String defaultExpression = f.getProp(DEFAULT_EXPRESSION);
        column.parseType(sqlType).notNull(!nullable).defaultExpression(defaultExpression);
      }
      String hidden = f.getProp(HIDDEN);
      if (Boolean.parseBoolean(hidden)) {
        column.isHidden(true);
      }
      String placementKey = f.getProp(SPANNER_PLACEMENT_KEY);
      if (placementKey != null) {
        column.isPlacementKey(Boolean.parseBoolean(placementKey));
      }
      ImmutableList.Builder<String> columnOptions = ImmutableList.builder();
      for (int i = 0; ; i++) {
        String spannerOption = f.getProp(SPANNER_OPTION + i);
        if (spannerOption == null) {
          break;
        }
        columnOptions.add(spannerOption);
      }
      column.columnOptions(columnOptions.build());
      column.endColumn();
    }

    for (int i = 0; ; i++) {
      String spannerPrimaryKey = schema.getProp(SPANNER_PRIMARY_KEY + "_" + i);
      if (spannerPrimaryKey == null) {
        break;
      }
      if (spannerPrimaryKey.endsWith(" ASC")) {
        String name = spannerPrimaryKey.substring(0, spannerPrimaryKey.length() - 4);
        table.primaryKey().asc(unescape(name, dialect)).end();
      } else if (spannerPrimaryKey.endsWith(" DESC")) {
        String name = spannerPrimaryKey.substring(0, spannerPrimaryKey.length() - 5);
        table.primaryKey().desc(unescape(name, dialect)).end();
      } else {
        throw new IllegalArgumentException("Cannot parse spannerPrimaryKey " + spannerPrimaryKey);
      }
    }

    table.indexes(getNumberedPropsWithPrefix(schema, SPANNER_INDEX));

    table.foreignKeys(getNumberedPropsWithPrefix(schema, SPANNER_FOREIGN_KEY));

    table.checkConstraints(getNumberedPropsWithPrefix(schema, SPANNER_CHECK_CONSTRAINT));

    // Table parent options.
    String spannerParent = schema.getProp(SPANNER_PARENT);
    if (!Strings.isNullOrEmpty(spannerParent)) {
      table.interleaveInParent(spannerParent);

      // Process the on delete action.
      String onDeleteAction = schema.getProp(SPANNER_ON_DELETE_ACTION);
      if (onDeleteAction == null) {
        // Preserve behavior for old versions of exporter that did not provide the
        // spannerOnDeleteAction property.
        onDeleteAction = "no action";
      }
      if (onDeleteAction.equals("cascade")) {
        table.onDeleteCascade();
      } else if (!onDeleteAction.equals("no action")) {
        // This is an unknown on delete action.
        throw new IllegalArgumentException("Unsupported ON DELETE action " + onDeleteAction);
      }
    }

    return table.build();
  }

  // TODO: maybe encapsulate in the Ddl library.
  private static String unescape(String name, Dialect dialect) {
    if ((dialect == Dialect.GOOGLE_STANDARD_SQL && name.startsWith("`") && name.endsWith("`"))
        || (dialect == Dialect.POSTGRESQL && name.startsWith("\"") && name.endsWith("\""))) {
      return name.substring(1, name.length() - 1);
    }
    return name;
  }

  private static ImmutableList<String> getNumberedPropsWithPrefix(Schema schema, String prefix) {
    ImmutableList.Builder<String> props = ImmutableList.builder();
    for (int i = 0; ; i++) {
      String prop = schema.getProp(prefix + i);
      if (prop == null) {
        break;
      }
      props.add(prop);
    }
    return props.build();
  }

  @VisibleForTesting
  com.google.cloud.teleport.spanner.common.Type inferType(Schema f, boolean supportArrays) {
    Schema.Type type = f.getType();
    LogicalType logicalType = LogicalTypes.fromSchema(f);

    switch (type) {
      case BOOLEAN:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.bool()
            : com.google.cloud.teleport.spanner.common.Type.pgBool();
      case INT:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.int64()
            : com.google.cloud.teleport.spanner.common.Type.pgInt8();
      case LONG:
        if (LogicalTypes.timestampMillis().equals(logicalType)
            || LogicalTypes.timestampMicros().equals(logicalType)) {
          return (dialect == Dialect.GOOGLE_STANDARD_SQL)
              ? com.google.cloud.teleport.spanner.common.Type.timestamp()
              : com.google.cloud.teleport.spanner.common.Type.pgTimestamptz();
        }
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.int64()
            : com.google.cloud.teleport.spanner.common.Type.pgInt8();
      case FLOAT:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.float32()
            : com.google.cloud.teleport.spanner.common.Type.pgFloat4();
      case DOUBLE:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.float64()
            : com.google.cloud.teleport.spanner.common.Type.pgFloat8();
      case STRING:
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.string()
            : com.google.cloud.teleport.spanner.common.Type.pgVarchar();
      case BYTES:
        if (LogicalTypes.decimal(NumericUtils.PRECISION, NumericUtils.SCALE).equals(logicalType)
            && dialect == Dialect.GOOGLE_STANDARD_SQL) {
          return com.google.cloud.teleport.spanner.common.Type.numeric();
        }
        if (LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
                .equals(logicalType)
            && dialect == Dialect.POSTGRESQL) {
          return com.google.cloud.teleport.spanner.common.Type.pgNumeric();
        }
        return (dialect == Dialect.GOOGLE_STANDARD_SQL)
            ? com.google.cloud.teleport.spanner.common.Type.bytes()
            : com.google.cloud.teleport.spanner.common.Type.pgBytea();
      case ARRAY:
        {
          if (supportArrays) {
            Schema element = f.getElementType();
            if (element.getType() == Schema.Type.UNION) {
              Schema unpacked = unpackNullable(element);
              if (unpacked == null) {
                throw new IllegalArgumentException("Cannot infer a type " + f);
              }
              element = unpacked;
            }
            try {
              return (dialect == Dialect.GOOGLE_STANDARD_SQL)
                  ? com.google.cloud.teleport.spanner.common.Type.array(inferType(element, false))
                  : com.google.cloud.teleport.spanner.common.Type.pgArray(
                      inferType(element, false));
            } catch (IllegalArgumentException e) {
              throw new IllegalArgumentException("Cannot infer array type for field " + f);
            }
          }
          // Throw exception.
          break;
        }
    }
    throw new IllegalArgumentException("Cannot infer a type " + f);
  }

  private String toString(
      com.google.cloud.teleport.spanner.common.Type spannerType, boolean supportArray) {
    switch (spannerType.getCode()) {
      case BOOL:
        return "BOOL";
      case PG_BOOL:
        return "boolean";
      case INT64:
        return "INT64";
      case PG_INT8:
        return "bigint";
      case FLOAT32:
        return "FLOAT32";
      case PG_FLOAT4:
        return "real";
      case FLOAT64:
        return "FLOAT64";
      case PG_FLOAT8:
        return "double precision";
      case STRING:
        return "STRING(MAX)";
      case PG_TEXT:
        return "text";
      case PG_VARCHAR:
        return "character varying";
      case BYTES:
        return "BYTES(MAX)";
      case PG_BYTEA:
        return "bytea";
      case TIMESTAMP:
        return "TIMESTAMP";
      case PG_TIMESTAMPTZ:
        return "timestamp with time zone";
      case PG_SPANNER_COMMIT_TIMESTAMP:
        return "spanner.commit_timestamp";
      case DATE:
        return "DATE";
      case PG_DATE:
        return "date";
      case NUMERIC:
        return "NUMERIC";
      case PG_NUMERIC:
        return "numeric";
      case JSON:
        return "JSON";
      case PROTO:
        return "PROTO<" + spannerType.getProtoTypeFqn() + ">";
      case ENUM:
        return "ENUM<" + spannerType.getProtoTypeFqn() + ">";
      case ARRAY:
        {
          if (supportArray) {
            com.google.cloud.teleport.spanner.common.Type element =
                spannerType.getArrayElementType();
            String elementStr = toString(element, false);
            return "ARRAY<" + elementStr + ">";
          }
          // otherwise fall through and throw an error.
          break;
        }
      case PG_ARRAY:
        {
          if (supportArray) {
            com.google.cloud.teleport.spanner.common.Type element =
                spannerType.getArrayElementType();
            String elementStr = toString(element, false);
            return elementStr + "[]";
          }
          // otherwise fall through and throw an error.
          break;
        }
    }
    throw new IllegalArgumentException("Cannot to string the type " + spannerType);
  }
}
