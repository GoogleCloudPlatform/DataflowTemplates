/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.common;

import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Set of utilities to convert Beam Schemas to Dataplex schema aspects. */
public class SchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  private static final java.util.Map<String, TypeName> DATAPLEX_TO_BEAM_TYPE =
      com.google.common.collect.ImmutableMap.<String, TypeName>builder()
          .put("boolean", TypeName.BOOLEAN)
          .put("bytes", TypeName.BYTES)
          .put("double", TypeName.DOUBLE)
          .put("decimal", TypeName.DECIMAL)
          .put("int16", TypeName.INT16)
          .put("int32", TypeName.INT32)
          .put("int64", TypeName.INT64)
          .put("integer", TypeName.INT32)
          .put("long", TypeName.INT64)
          .put("string", TypeName.STRING)
          .put("timestamp", TypeName.DATETIME)
          .build();

  private static final java.util.Map<TypeName, String> BEAM_TO_DATAPLEX_TYPE =
      com.google.common.collect.ImmutableMap.<TypeName, String>builder()
          .put(TypeName.BOOLEAN, "BOOLEAN")
          .put(TypeName.BYTES, "BYTES")
          .put(TypeName.DOUBLE, "DOUBLE")
          .put(TypeName.DECIMAL, "DECIMAL")
          .put(TypeName.INT16, "INT16")
          .put(TypeName.INT32, "INT32")
          .put(TypeName.INT64, "INT64")
          .put(TypeName.STRING, "STRING")
          .put(TypeName.DATETIME, "TIMESTAMP")
          .build();

  private static final ImmutableBiMap<String, FieldType> LOGICAL_FIELD_TYPES =
      ImmutableBiMap.<String, FieldType>builder()
          .put("date", FieldType.logicalType(SqlTypes.DATE))
          .put("time", FieldType.logicalType(SqlTypes.TIME))
          .put("map", FieldType.map(FieldType.STRING, FieldType.STRING))
          .build();

  static Schema toBeamSchema(Struct schemaAspectData) {
    if (schemaAspectData == null || !schemaAspectData.containsFields("fields")) {
      LOG.warn("Schema is empty");
      return Schema.builder().build();
    }
    ListValue fieldsList = schemaAspectData.getFieldsMap().get("fields").getListValue();
    List<Struct> cols =
        fieldsList.getValuesList().stream().map(Value::getStructValue).collect(Collectors.toList());
    return beamSchemaFromColumnList(cols);
  }

  private static Schema beamSchemaFromColumnList(List<Struct> cols) {
    Schema.Builder schemaBuilder = Schema.builder();
    cols.forEach(col -> schemaBuilder.addField(toBeamField(col)));
    return schemaBuilder.build();
  }

  private static Field toBeamField(Struct columnSchema) {
    String name =
        columnSchema.containsFields("name")
            ? columnSchema.getFieldsMap().get("name").getStringValue()
            : "";
    FieldType beamFieldType = getBeamFieldType(columnSchema);

    Field field = Field.of(name, beamFieldType);

    String mode = "NULLABLE";
    if (columnSchema.containsFields("mode")) {
      mode = columnSchema.getFieldsMap().get("mode").getStringValue();
    }

    if ("NULLABLE".equals(mode)) {
      field = field.withNullable(true);
    } else if ("REQUIRED".equals(mode)) {
      field = field.withNullable(false);
    } else if ("REPEATED".equals(mode)) {
      field = Field.of(name, FieldType.array(beamFieldType));
    } else {
      throw new UnsupportedOperationException(
          "Field mode '" + mode + "' is not supported (field '" + name + "')");
    }

    return field;
  }

  private static FieldType getBeamFieldType(Struct column) {
    String dcFieldType =
        column.containsFields("dataType")
            ? column.getFieldsMap().get("dataType").getStringValue().toLowerCase()
            : "";

    if (LOGICAL_FIELD_TYPES.containsKey(dcFieldType)) {
      return LOGICAL_FIELD_TYPES.get(dcFieldType);
    } else if (DATAPLEX_TO_BEAM_TYPE.containsKey(dcFieldType)) {
      return FieldType.of(DATAPLEX_TO_BEAM_TYPE.get(dcFieldType));
    }

    if ("record".equals(dcFieldType) || "struct".equals(dcFieldType)) {
      if (column.containsFields("fields")) {
        List<Struct> subcols =
            column.getFieldsMap().get("fields").getListValue().getValuesList().stream()
                .map(Value::getStructValue)
                .collect(Collectors.toList());
        return FieldType.row(beamSchemaFromColumnList(subcols));
      } else {
        return FieldType.row(Schema.builder().build());
      }
    }

    String fieldName =
        column.containsFields("name") ? column.getFieldsMap().get("name").getStringValue() : "";
    String errorMessage =
        "Field type '" + dcFieldType + "' is not supported (field '" + fieldName + "')";
    LOG.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
  }

  static Struct fromBeamSchema(Schema beamSchema) {
    List<Value> catalogColumns =
        beamSchema.getFields().stream()
            .map(SchemaUtils::fromBeamField)
            .collect(Collectors.toList());
    return Struct.newBuilder()
        .putFields(
            "fields",
            Value.newBuilder()
                .setListValue(ListValue.newBuilder().addAllValues(catalogColumns).build())
                .build())
        .build();
  }

  private static String getMetadataType(FieldType type) {
    switch (type.getTypeName()) {
      case BOOLEAN:
        return "BOOLEAN";
      case BYTES:
        return "BYTES";
      case DATETIME:
        return "TIMESTAMP";
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INT16:
      case INT32:
      case INT64:
        return "NUMBER";
      case STRING:
        return "STRING";
      case ROW:
        return "STRUCT";
      default:
        return "OTHER";
    }
  }

  private static Value fromBeamField(Field beamField) {
    Struct.Builder columnBuilder = Struct.newBuilder();
    columnBuilder.putFields("name", Value.newBuilder().setStringValue(beamField.getName()).build());

    if (beamField.getType().getNullable()) {
      columnBuilder.putFields("mode", Value.newBuilder().setStringValue("NULLABLE").build());
    } else {
      columnBuilder.putFields("mode", Value.newBuilder().setStringValue("REQUIRED").build());
    }

    String metadataType = getMetadataType(beamField.getType());
    columnBuilder.putFields(
        "metadataType", Value.newBuilder().setStringValue(metadataType).build());

    // In Dataplex v1, dataType often mirrors metadataType or must be valid standard SQL types
    if (beamField.getType().getTypeName() == TypeName.ROW) {
      columnBuilder.putFields("dataType", Value.newBuilder().setStringValue("STRUCT").build());
      Struct subSchema = fromBeamSchema(beamField.getType().getRowSchema());
      if (subSchema.containsFields("fields")) {
        columnBuilder.putFields("fields", subSchema.getFieldsMap().get("fields"));
      }
    } else if (LOGICAL_FIELD_TYPES.inverse().containsKey(beamField.getType())) {
      String columnType = LOGICAL_FIELD_TYPES.inverse().get(beamField.getType()).toUpperCase();
      columnBuilder.putFields("dataType", Value.newBuilder().setStringValue(columnType).build());
    } else {
      String columnType = BEAM_TO_DATAPLEX_TYPE.get(beamField.getType().getTypeName());
      if (columnType == null) {
        columnType = "STRING"; // fallback
      }
      columnBuilder.putFields("dataType", Value.newBuilder().setStringValue(columnType).build());
    }

    return Value.newBuilder().setStructValue(columnBuilder.build()).build();
  }
}
