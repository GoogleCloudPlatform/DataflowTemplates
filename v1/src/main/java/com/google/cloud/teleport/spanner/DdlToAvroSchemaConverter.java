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
import static com.google.cloud.teleport.spanner.AvroUtil.INPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.NOT_NULL;
import static com.google.cloud.teleport.spanner.AvroUtil.OUTPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHANGE_STREAM_FOR_CLAUSE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHECK_CONSTRAINT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY_MODEL;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_FOREIGN_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_INDEX;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ON_DELETE_ACTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_OPTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PARENT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PRIMARY_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_REMOTE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_QUERY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_SECURITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SQL_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.STORED;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.ddl.ChangeStream;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.spanner.ddl.Model;
import com.google.cloud.teleport.spanner.ddl.ModelColumn;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.cloud.teleport.spanner.ddl.View;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordDefault;

/** Converts a Spanner {@link Ddl} to Avro {@link Schema}. */
public class DdlToAvroSchemaConverter {
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

    for (Table table : ddl.allTables()) {
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(table.name()).namespace(this.namespace);
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      if (table.interleaveInParent() != null) {
        recordBuilder.prop(SPANNER_PARENT, table.interleaveInParent());
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
      for (Column cm : table.columns()) {
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder = fieldsAssembler.name(cm.name());
        fieldBuilder.prop(SQL_TYPE, cm.typeString());
        for (int i = 0; i < cm.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, cm.columnOptions().get(i));
        }
        if (cm.isGenerated()) {
          fieldBuilder.prop(NOT_NULL, Boolean.toString(cm.notNull()));
          fieldBuilder.prop(GENERATION_EXPRESSION, cm.generationExpression());
          fieldBuilder.prop(STORED, Boolean.toString(cm.isStored()));
          // Make the type null to allow us not export the generated column values,
          // which are semantically logical entities.
          fieldBuilder.type(SchemaBuilder.builder().nullType()).withDefault(null);
        } else {
          if (cm.defaultExpression() != null) {
            fieldBuilder.prop(DEFAULT_EXPRESSION, cm.defaultExpression());
          }
          Schema avroType = avroType(cm.type());
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
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(model.name()).namespace(this.namespace);
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
      for (ModelColumn c : model.inputColumns()) {
        FieldBuilder<RecordDefault<Schema>> fieldBuilder = inputBuilder.name(c.name());
        fieldBuilder.prop(SQL_TYPE, c.typeString());
        for (int i = 0; i < c.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, c.columnOptions().get(i));
        }
        Schema avroType = avroType(c.type());
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
      for (ModelColumn c : model.outputColumns()) {
        FieldBuilder<RecordDefault<Schema>> fieldBuilder = outputBuilder.name(c.name());
        fieldBuilder.prop(SQL_TYPE, c.typeString());
        for (int i = 0; i < c.columnOptions().size(); i++) {
          fieldBuilder.prop(SPANNER_OPTION + i, c.columnOptions().get(i));
        }
        Schema avroType = avroType(c.type());
        fieldBuilder.type(avroType).noDefault();
      }
      outputBuilder.endRecord().noDefault();

      Schema schema = fieldsAssembler.endRecord();
      schemas.add(schema);
    }

    for (View view : ddl.views()) {
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(view.name()).namespace(this.namespace);
      recordBuilder.prop(GOOGLE_FORMAT_VERSION, version);
      recordBuilder.prop(GOOGLE_STORAGE, "CloudSpanner");
      recordBuilder.prop(SPANNER_VIEW_QUERY, view.query());
      if (view.security() != null) {
        recordBuilder.prop(SPANNER_VIEW_SECURITY, view.security().toString());
      }
      schemas.add(recordBuilder.fields().endRecord());
    }

    for (ChangeStream changeStream : ddl.changeStreams()) {
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(changeStream.name()).namespace(this.namespace);
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

    return schemas;
  }

  private Schema avroType(com.google.cloud.teleport.spanner.common.Type spannerType) {
    switch (spannerType.getCode()) {
      case BOOL:
      case PG_BOOL:
        return SchemaBuilder.builder().booleanType();
      case INT64:
      case PG_INT8:
        return SchemaBuilder.builder().longType();
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
      case BYTES:
      case PG_BYTEA:
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
        Schema avroItemsType = avroType(spannerType.getArrayElementType());
        return SchemaBuilder.builder().array().items().type(wrapAsNullable(avroItemsType));
      default:
        throw new IllegalArgumentException("Unknown spanner type " + spannerType);
    }
  }

  private Schema wrapAsNullable(Schema avroType) {
    return SchemaBuilder.builder().unionOf().nullType().and().type(avroType).endUnion();
  }
}
