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

package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.spanner.ddl.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/** Converts a Spanner {@link Ddl} to Avro {@link Schema}. */
public class DdlToAvroSchemaConverter {
  private final String namespace;
  private final String version;

  public DdlToAvroSchemaConverter(String namespace, String version) {
    this.namespace = namespace;
    this.version = version;
  }

  public Collection<Schema> convert(Ddl ddl) {
    Collection<Schema> schemas = new ArrayList<>();

    for (Table table : ddl.allTables()) {
      SchemaBuilder.RecordBuilder<Schema> recordBuilder =
          SchemaBuilder.record(table.name()).namespace(this.namespace);
      recordBuilder.prop("googleFormatVersion", version);
      recordBuilder.prop("googleStorage", "CloudSpanner");
      if (table.interleaveInParent() != null) {
        recordBuilder.prop("spannerParent", table.interleaveInParent());
        recordBuilder.prop(
            "spannerOnDeleteAction", table.onDeleteCascade() ? "cascade" : "no action");
      }
      if (table.primaryKeys() != null) {
        String encodedPk =
            table.primaryKeys().stream()
                .map(IndexColumn::prettyPrint)
                .collect(Collectors.joining(","));
        recordBuilder.prop("spannerPrimaryKey", encodedPk);
      }
      for (int i = 0; i < table.primaryKeys().size(); i++) {
        recordBuilder.prop("spannerPrimaryKey_" + i, table.primaryKeys().get(i).prettyPrint());
      }
      for (int i = 0; i < table.indexes().size(); i++) {
        recordBuilder.prop("spannerIndex_" + i, table.indexes().get(i));
      }
      for (int i = 0; i < table.foreignKeys().size(); i++) {
        recordBuilder.prop("spannerForeignKey_" + i, table.foreignKeys().get(i));
      }
      SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = recordBuilder.fields();
      for (Column cm : table.columns()) {
        SchemaBuilder.FieldBuilder<Schema> fieldBuilder = fieldsAssembler.name(cm.name());
        fieldBuilder.prop("sqlType", cm.typeString());
        for (int i = 0; i < cm.columnOptions().size(); i++) {
          fieldBuilder.prop("spannerOption_" + i, cm.columnOptions().get(i));
        }
        if (cm.isGenerated()) {
          fieldBuilder.prop("notNull", Boolean.toString(cm.notNull()));
          fieldBuilder.prop("generationExpression", cm.generationExpression());
          fieldBuilder.prop("stored", Boolean.toString(cm.isStored()));
          // Make the type null to allow us not export the generated column values,
          // which are semantically logical entities.
          fieldBuilder.type(SchemaBuilder.builder().nullType()).withDefault(null);
        } else {
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

    return schemas;
  }

  private Schema avroType(Type spannerType) {
    switch (spannerType.getCode()) {
      case BOOL:
        return SchemaBuilder.builder().booleanType();
      case INT64:
        return SchemaBuilder.builder().longType();
      case FLOAT64:
        return SchemaBuilder.builder().doubleType();
      case STRING:
        return SchemaBuilder.builder().stringType();
      case BYTES:
        return SchemaBuilder.builder().bytesType();
      case TIMESTAMP:
        return SchemaBuilder.builder().stringType();
      case DATE:
        return SchemaBuilder.builder().stringType();
      case ARRAY:
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
