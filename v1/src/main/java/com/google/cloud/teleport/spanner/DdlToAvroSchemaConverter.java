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
import static com.google.cloud.teleport.spanner.AvroUtil.GOOGLE_FORMAT_VERSION;
import static com.google.cloud.teleport.spanner.AvroUtil.GOOGLE_STORAGE;
import static com.google.cloud.teleport.spanner.AvroUtil.HIDDEN;
import static com.google.cloud.teleport.spanner.AvroUtil.IDENTITY_COLUMN;
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
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_INTERLEAVE_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_LABEL;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_NAME;
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
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF_DEFINITION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF_NAME;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF_PARAMETER;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF_SECURITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_UDF_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_QUERY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_SECURITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SQL_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.STORED;
import static com.google.cloud.teleport.spanner.AvroUtil.generateAvroSchemaName;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.ddl.ChangeStream;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.GraphElementTable;
import com.google.cloud.teleport.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.spanner.ddl.Model;
import com.google.cloud.teleport.spanner.ddl.ModelColumn;
import com.google.cloud.teleport.spanner.ddl.NamedSchema;
import com.google.cloud.teleport.spanner.ddl.Placement;
import com.google.cloud.teleport.spanner.ddl.PropertyGraph;
import com.google.cloud.teleport.spanner.ddl.Sequence;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.ddl.Table.InterleaveType;
import com.google.cloud.teleport.spanner.ddl.Udf;
import com.google.cloud.teleport.spanner.ddl.UdfParameter;
import com.google.cloud.teleport.spanner.ddl.View;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Converts a Spanner {@link Ddl} to Avro {@link Schema}. */
public class DdlToAvroSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(DdlToAvroSchemaConverter.class);
  private final String namespace;
  private final String version;
  private final Boolean shouldExportTimestampAsLogicalType;

  public DdlToAvroSchemaConverter(
      String namespace, String version, Boolean shouldExportTimestampAsLogicalType) {
    this.namespace = namespace;
    this.version = version;
    this.shouldExportTimestampAsLogicalType = shouldExportTimestampAsLogicalType;
  }

  public Collection<Schema> convert(Ddl ddl) {
    Collection<Schema> schemas = new ArrayList<>();

    for (NamedSchema schema : ddl.schemas()) {
      LOG.info("DdlToAvo Schema {}", schema.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(schema.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, schema.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      // Indicate that this is a "CREATE SCHEMA", not a table or a view.
      recordBuilder.prop(SPANNER_ENTITY, SPANNER_NAMED_SCHEMA);
      schemas.add(recordBuilder.fields().endRecord());
    }
    for (Udf udf : ddl.udfs()) {
      LOG.info("DdlToAvo Udf {}", udf.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(udf.specificName()))
              .namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, udf.specificName());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      // Indicate that this is a "CREATE FUNCTION", not a table or a view.
      recordBuilder.prop(SPANNER_ENTITY, SPANNER_UDF);
      recordBuilder.prop(SPANNER_UDF_NAME, udf.name());
      recordBuilder.prop(SPANNER_UDF_DEFINITION, udf.definition());
      if (udf.type() != null) {
        recordBuilder.prop(SPANNER_UDF_TYPE, udf.type());
      }
      if (udf.security() != null) {
        recordBuilder.prop(SPANNER_UDF_SECURITY, udf.security().toString());
      }
      int i = 0;
      for (UdfParameter udfParameter : udf.parameters()) {
        recordBuilder.prop(SPANNER_UDF_PARAMETER + i++, udfParameter.prettyPrint());
      }
      schemas.add(recordBuilder.fields().endRecord());
    }
    for (Table table : ddl.allTables()) {
      LOG.info("DdlToAvo Table {}", table.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(table.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, table.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      if (table.interleaveInParent() != null) {
        recordBuilder.prop(SPANNER_PARENT, table.interleaveInParent());
        recordBuilder.prop(
            SPANNER_INTERLEAVE_TYPE,
            table.interleaveType() == InterleaveType.IN ? "IN" : "IN PARENT");
        recordBuilder.prop(
            SPANNER_ON_DELETE_ACTION, table.onDeleteCascade() ? "cascade" : "no action");
      }
      if (table.dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        if (table.primaryKeys() != null) {
          String encodedPk =
              table.primaryKeys().stream()
                  .map(IndexColumn::prettyPrint)
                  .collect(Collectors.joining(","));
          recordBuilder.prop(SPANNER_PRIMARY_KEY, encodedPk);
        }
        for (int i = 0; i < table.primaryKeys().size(); i++) {
          recordBuilder.prop(
              SPANNER_PRIMARY_KEY + "_" + i, table.primaryKeys().get(i).prettyPrint());
        }
      } else if (table.dialect() == Dialect.POSTGRESQL) {
        if (table.primaryKeys() != null) {
          String encodedPk =
              table.primaryKeys().stream()
                  .map(c -> "\"" + c.name() + "\"")
                  .collect(Collectors.joining(", "));
          recordBuilder.prop(SPANNER_PRIMARY_KEY, encodedPk);
        }
        for (int i = 0; i < table.primaryKeys().size(); i++) {
          IndexColumn pk = table.primaryKeys().get(i);
          recordBuilder.prop(SPANNER_PRIMARY_KEY + "_" + i, "\"" + pk.name() + "\" ASC");
        }
      }
      for (int i = 0; i < table.indexes().size(); i++) {
        recordBuilder.prop(SPANNER_INDEX + i, table.indexes().get(i));
      }
      for (int i = 0; i < table.foreignKeys().size(); i++) {
        recordBuilder.prop(SPANNER_FOREIGN_KEY + i, table.foreignKeys().get(i));
      }
      for (int i = 0; i < table.checkConstraints().size(); i++) {
        recordBuilder.prop(SPANNER_CHECK_CONSTRAINT + i, table.checkConstraints().get(i));
      }
      SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = recordBuilder.fields();
      int columnOrdinal = 0;
      for (Column cm : table.columns()) {
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder = fieldsAssembler.name(cm.name());
        fieldBuilder.prop(SQL_TYPE, cm.typeString());
        fieldBuilder.prop(HIDDEN, Boolean.toString(cm.isHidden()));
        for (int i = 0; i < cm.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, cm.columnOptions().get(i));
        }
        if (cm.isPlacementKey()) {
          fieldBuilder.prop(SPANNER_PLACEMENT_KEY, Boolean.toString(cm.isPlacementKey()));
        }
        if (cm.isGenerated()) {
          fieldBuilder.prop(NOT_NULL, Boolean.toString(cm.notNull()));
          fieldBuilder.prop(GENERATION_EXPRESSION, cm.generationExpression());
          fieldBuilder.prop(STORED, Boolean.toString(cm.isStored()));
          // Make the type null to allow us not export the generated column values,
          // which are semantically logical entities.
          fieldBuilder.type(SchemaBuilder.builder().nullType()).withDefault(null);
        } else {
          if (cm.isIdentityColumn()) {
            fieldBuilder.prop(IDENTITY_COLUMN, Boolean.toString(cm.isIdentityColumn()));
            if (cm.sequenceKind() != null) {
              fieldBuilder.prop(SPANNER_SEQUENCE_KIND, cm.sequenceKind());
            }
            if (cm.counterStartValue() != null) {
              fieldBuilder.prop(
                  SPANNER_SEQUENCE_COUNTER_START, String.valueOf(cm.counterStartValue()));
            }
            if (cm.skipRangeMin() != null) {
              fieldBuilder.prop(SPANNER_SEQUENCE_SKIP_RANGE_MIN, String.valueOf(cm.skipRangeMin()));
            }
            if (cm.skipRangeMax() != null) {
              fieldBuilder.prop(SPANNER_SEQUENCE_SKIP_RANGE_MAX, String.valueOf(cm.skipRangeMax()));
            }
          } else if (cm.defaultExpression() != null) {
            fieldBuilder.prop(DEFAULT_EXPRESSION, cm.defaultExpression());
          }
          Schema avroType = avroType(cm.type(), table.name() + "_" + columnOrdinal++);
          if (!cm.notNull()) {
            avroType = wrapAsNullable(avroType);
          }
          fieldBuilder.type(avroType).noDefault();
        }
      }
      Schema schema = fieldsAssembler.endRecord();
      schemas.add(schema);
    }

    for (Model model : ddl.models()) {
      LOG.info("DdlToAvo model {}", model.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(model.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, model.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(SPANNER_ENTITY, SPANNER_ENTITY_MODEL);
      recordBuilder.prop(SPANNER_REMOTE, Boolean.toString(model.remote()));
      if (model.options() != null) {
        for (int i = 0; i < model.options().size(); i++) {
          recordBuilder.prop(SPANNER_OPTION + i, model.options().get(i));
        }
      }

      SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = recordBuilder.fields();

      SchemaBuilder.FieldAssembler<RecordDefault<Schema>> inputBuilder =
          fieldsAssembler
              .name(INPUT)
              .type()
              .record(model.name() + "_" + INPUT)
              .namespace(this.namespace)
              .fields();
      int inputColumnOrdinal = 0;
      for (ModelColumn c : model.inputColumns()) {
        FieldBuilder<RecordDefault<Schema>> fieldBuilder = inputBuilder.name(c.name());
        fieldBuilder.prop(SQL_TYPE, c.typeString());
        for (int i = 0; i < c.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, c.columnOptions().get(i));
        }
        Schema avroType = avroType(c.type(), model.name() + "_input_" + inputColumnOrdinal++);
        fieldBuilder.type(avroType).noDefault();
      }
      inputBuilder.endRecord().noDefault();

      SchemaBuilder.FieldAssembler<RecordDefault<Schema>> outputBuilder =
          fieldsAssembler
              .name(OUTPUT)
              .type()
              .record(model.name() + "_" + OUTPUT)
              .namespace(this.namespace)
              .fields();
      int outputColumnOrdinal = 0;
      for (ModelColumn c : model.outputColumns()) {
        FieldBuilder<RecordDefault<Schema>> fieldBuilder = outputBuilder.name(c.name());
        fieldBuilder.prop(SQL_TYPE, c.typeString());
        for (int i = 0; i < c.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, c.columnOptions().get(i));
        }
        Schema avroType = avroType(c.type(), model.name() + "_output_" + outputColumnOrdinal++);
        fieldBuilder.type(avroType).noDefault();
      }
      outputBuilder.endRecord().noDefault();

      Schema schema = fieldsAssembler.endRecord();
      schemas.add(schema);
    }

    for (PropertyGraph propertyGraph : ddl.propertyGraphs()) {
      LOG.info("DdlToAvro PropertyGraph {}", propertyGraph.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(propertyGraph.name()))
              .namespace(this.namespace);

      recordBuilder.prop(SPANNER_NAME, propertyGraph.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(SPANNER_ENTITY, SPANNER_ENTITY_PROPERTY_GRAPH);

      // Encode nodeTables
      for (int i = 0; i < propertyGraph.nodeTables().size(); i++) {
        encodeNodeTable(recordBuilder, propertyGraph.nodeTables().get(i), i);
      }

      // Encode edgeTables
      for (int i = 0; i < propertyGraph.edgeTables().size(); i++) {
        encodeEdgeTable(recordBuilder, propertyGraph.edgeTables().get(i), i);
      }

      // Encode propertyDeclarations
      encodePropertyDeclarations(recordBuilder, propertyGraph.propertyDeclarations());

      // Encode labels
      encodeLabels(recordBuilder, propertyGraph.labels());

      Schema schema = recordBuilder.fields().endRecord();
      schemas.add(schema);
    }

    for (View view : ddl.views()) {
      LOG.info("DdlToAvo view {}", view.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(view.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, view.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(SPANNER_VIEW_QUERY, view.query());
      if (view.security() != null) {
        recordBuilder.prop(SPANNER_VIEW_SECURITY, view.security().toString());
      }
      schemas.add(recordBuilder.fields().endRecord());
    }

    for (ChangeStream changeStream : ddl.changeStreams()) {
      LOG.info("DdlToAvo changestream {}", changeStream.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(changeStream.name()))
              .namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, changeStream.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(
          SPANNER_CHANGE_STREAM_FOR_CLAUSE,
          changeStream.forClause() == null ? "" : changeStream.forClause());
      if (changeStream.options() != null) {
        for (int i = 0; i < changeStream.options().size(); i++) {
          recordBuilder.prop(SPANNER_OPTION + i, changeStream.options().get(i));
        }
      }
      schemas.add(recordBuilder.fields().endRecord());
    }

    for (Sequence sequence : ddl.sequences()) {
      LOG.info("DdlToAvo sequence {}", sequence.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(sequence.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, sequence.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      if (sequence.options() != null) {
        for (int i = 0; i < sequence.options().size(); i++) {
          recordBuilder.prop(SPANNER_SEQUENCE_OPTION + i, sequence.options().get(i));
        }
      }
      if (sequence.sequenceKind() != null) {
        recordBuilder.prop(SPANNER_SEQUENCE_KIND, sequence.sequenceKind());
      }
      if (sequence.counterStartValue() != null) {
        recordBuilder.prop(
            SPANNER_SEQUENCE_COUNTER_START, String.valueOf(sequence.counterStartValue()));
      }
      if (sequence.skipRangeMin() != null) {
        recordBuilder.prop(
            SPANNER_SEQUENCE_SKIP_RANGE_MIN, String.valueOf(sequence.skipRangeMin()));
      }
      if (sequence.skipRangeMax() != null) {
        recordBuilder.prop(
            SPANNER_SEQUENCE_SKIP_RANGE_MAX, String.valueOf(sequence.skipRangeMax()));
      }
      schemas.add(recordBuilder.fields().endRecord());
    }

    for (Placement placement : ddl.placements()) {
      LOG.info("DdlToAvro placement {}", placement.name());
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(generateAvroSchemaName(placement.name())).namespace(this.namespace);
      recordBuilder.prop(SPANNER_NAME, placement.name());
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(SPANNER_ENTITY, SPANNER_ENTITY_PLACEMENT);
      if (placement.options() != null) {
        for (int i = 0; i < placement.options().size(); i++) {
          recordBuilder.prop(SPANNER_OPTION + i, placement.options().get(i));
        }
      }
      schemas.add(recordBuilder.fields().endRecord());
    }
    return schemas;
  }

  private void encodeNodeTable(
      SchemaBuilder.RecordBuilder<Schema> recordBuilder, GraphElementTable nodeTable, int i) {
    encodeElementTable(recordBuilder, nodeTable, i, SPANNER_NODE_TABLE);
  }

  private void encodeEdgeTable(
      SchemaBuilder.RecordBuilder<Schema> recordBuilder, GraphElementTable edgeTable, int i) {
    encodeElementTable(recordBuilder, edgeTable, i, SPANNER_EDGE_TABLE);

    // Encode sourceNodeTable and targetNodeTable (always present for edges)
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_SOURCE_NODE_TABLE_NAME",
        edgeTable.sourceNodeTable().nodeTableName);
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_SOURCE_NODE_KEY_COLUMNS",
        String.join(", ", edgeTable.sourceNodeTable().nodeKeyColumns));
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_SOURCE_EDGE_KEY_COLUMNS",
        String.join(", ", edgeTable.sourceNodeTable().edgeKeyColumns));
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_TARGET_NODE_TABLE_NAME",
        edgeTable.targetNodeTable().nodeTableName);
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_TARGET_NODE_KEY_COLUMNS",
        String.join(", ", edgeTable.targetNodeTable().nodeKeyColumns));
    recordBuilder.prop(
        SPANNER_EDGE_TABLE + "_" + i + "_TARGET_EDGE_KEY_COLUMNS",
        String.join(", ", edgeTable.targetNodeTable().edgeKeyColumns));
  }

  private void encodeElementTable(
      SchemaBuilder.RecordBuilder<Schema> recordBuilder,
      GraphElementTable elementTable,
      int i,
      String prefix) {
    recordBuilder.prop(prefix + "_" + i + "_NAME", elementTable.name());
    recordBuilder.prop(prefix + "_" + i + "_BASE_TABLE_NAME", elementTable.baseTableName());
    recordBuilder.prop(prefix + "_" + i + "_KIND", elementTable.kind().toString());
    // Encode keyColumns
    recordBuilder.prop(
        prefix + "_" + i + "_KEY_COLUMNS", String.join(", ", elementTable.keyColumns()));

    // Encode labelToPropertyDefinitions
    for (int j = 0; j < elementTable.labelToPropertyDefinitions().size(); j++) {
      GraphElementTable.LabelToPropertyDefinitions labelToPropertyDef =
          elementTable.labelToPropertyDefinitions().get(j);
      recordBuilder.prop(prefix + "_" + i + "_LABEL_" + j + "_NAME", labelToPropertyDef.labelName);
      // Encode propertyDefinitions
      for (int k = 0; k < labelToPropertyDef.propertyDefinitions.size(); k++) {
        GraphElementTable.PropertyDefinition propertyDef =
            labelToPropertyDef.propertyDefinitions.get(k);
        recordBuilder.prop(
            prefix + "_" + i + "_LABEL_" + j + "_PROPERTY_" + k + "_NAME", propertyDef.name);
        recordBuilder.prop(
            prefix + "_" + i + "_LABEL_" + j + "_PROPERTY_" + k + "_VALUE",
            propertyDef.valueExpressionString);
      }
    }
  }

  private void encodePropertyDeclarations(
      SchemaBuilder.RecordBuilder<Schema> recordBuilder,
      ImmutableList<PropertyGraph.PropertyDeclaration> declarations) {
    for (int i = 0; i < declarations.size(); i++) {
      PropertyGraph.PropertyDeclaration declaration = declarations.get(i);
      recordBuilder.prop(SPANNER_PROPERTY_DECLARATION + "_" + i + "_NAME", declaration.name);
      recordBuilder.prop(SPANNER_PROPERTY_DECLARATION + "_" + i + "_TYPE", declaration.type);
    }
  }

  private void encodeLabels(
      SchemaBuilder.RecordBuilder<Schema> recordBuilder,
      ImmutableList<PropertyGraph.GraphElementLabel> labels) {
    for (int i = 0; i < labels.size(); i++) {
      PropertyGraph.GraphElementLabel label = labels.get(i);
      recordBuilder.prop(SPANNER_LABEL + "_" + i + "_NAME", label.name);
      // Encode properties
      for (int j = 0; j < label.properties.size(); j++) {
        recordBuilder.prop(SPANNER_LABEL + "_" + i + "_PROPERTY_" + j, label.properties.get(j));
      }
    }
  }

  /**
   * Converts a Spanner type into Avro type.
   *
   * <p>All generated record types must have unique names per Avro schema requirements. In order to
   * ensure that, structSuffix should contain a unique schema entity name (e.g. table name) and a
   * column ordinal number. The former prevents conflicts across different entities in the schema,
   * while the latter generates unique names for each column.
   */
  private Schema avroType(
      com.google.cloud.teleport.spanner.common.Type spannerType, String structSuffix) {
    LOG.info("avroType type={} suffix={}", spannerType.toString(), structSuffix);
    switch (spannerType.getCode()) {
      case BOOL:
      case PG_BOOL:
        return SchemaBuilder.builder().booleanType();
      case INT64:
      case PG_INT8:
      case ENUM:
        return SchemaBuilder.builder().longType();
      case FLOAT32:
      case PG_FLOAT4:
        return SchemaBuilder.builder().floatType();
      case FLOAT64:
      case PG_FLOAT8:
        return SchemaBuilder.builder().doubleType();
      case PG_TEXT:
      case PG_VARCHAR:
      case STRING:
      case DATE:
      case PG_DATE:
      case JSON:
      case PG_JSONB:
        return SchemaBuilder.builder().stringType();
      case UUID:
      case PG_UUID:
        return LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType());
      case BYTES:
      case PG_BYTEA:
      case PROTO:
      case TOKENLIST:
      case PG_SPANNER_TOKENLIST:
        return SchemaBuilder.builder().bytesType();
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_SPANNER_COMMIT_TIMESTAMP:
        return shouldExportTimestampAsLogicalType
            ? LogicalTypes.timestampMicros().addToSchema(SchemaBuilder.builder().longType())
            : SchemaBuilder.builder().stringType();
      case NUMERIC:
        return LogicalTypes.decimal(NumericUtils.PRECISION, NumericUtils.SCALE)
            .addToSchema(SchemaBuilder.builder().bytesType());
      case PG_NUMERIC:
        return LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
            .addToSchema(SchemaBuilder.builder().bytesType());
      case ARRAY:
      case PG_ARRAY:
        Schema avroItemsType = avroType(spannerType.getArrayElementType(), structSuffix);
        return SchemaBuilder.builder().array().items().type(wrapAsNullable(avroItemsType));
      case STRUCT:
        SchemaBuilder.FieldAssembler<Schema> fields =
            SchemaBuilder.builder().record("struct_" + structSuffix).fields();
        int fieldCounter = 0;
        for (com.google.cloud.teleport.spanner.common.Type.StructField structField :
            spannerType.getStructFields()) {
          fields
              .name(structField.getName())
              .type(avroType(structField.getType(), structSuffix + "_" + fieldCounter++))
              .noDefault();
        }
        return fields.endRecord();
      default:
        throw new IllegalArgumentException("Unknown spanner type " + spannerType);
    }
  }

  private Schema wrapAsNullable(Schema avroType) {
    return SchemaBuilder.builder().unionOf().nullType().and().type(avroType).endUnion();
  }
}
