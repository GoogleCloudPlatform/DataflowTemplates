/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.logicaltypes.IsoDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;

/** Utilities for organizing Bean rows and schema. */
public class BeamUtils {

  public static Schema toBeamSchema(
      Target target, NodeTarget startNodeTarget, NodeTarget endNodeTarget) {
    TargetType targetType = target.getTargetType();
    if (targetType == TargetType.QUERY) {
      return new Schema(new ArrayList<>());
    }
    if (targetType != TargetType.NODE && targetType != TargetType.RELATIONSHIP) {
      throw new IllegalArgumentException(
          String.format("Expected relationship or node target, got %s", targetType));
    }
    EntityTarget entityTarget = (EntityTarget) target;

    List<Schema.Field> fields = new ArrayList<>();
    for (PropertyMapping mapping :
        ModelUtils.allPropertyMappings(entityTarget, startNodeTarget, endNodeTarget)) {
      // map source column names to order
      String field = mapping.getSourceField();
      if (StringUtils.isEmpty(field)) {
        throw new RuntimeException(
            "Could not find field name or constant in target: " + target.getName());
      }
      Schema.Field schemaField;
      PropertyType propertyType = mapping.getTargetPropertyType();
      if (propertyType == null) {
        fields.add(defaultFieldSchema(field));
        continue;
      }
      switch (propertyType) {
        case BOOLEAN:
          schemaField = Schema.Field.nullable(field, FieldType.BOOLEAN);
          break;
        case BOOLEAN_ARRAY:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.array(FieldType.BOOLEAN));
          break;
        case BYTE_ARRAY:
          schemaField = Schema.Field.nullable(field, FieldType.BYTES);
          break;
        case DATE:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.logicalType(new Date()));
          break;
        case DATE_ARRAY:
          schemaField =
              Schema.Field.nullable(
                  field, Schema.FieldType.array(Schema.FieldType.logicalType(new Date())));
          break;
        case DURATION:
          schemaField =
              Schema.Field.nullable(field, Schema.FieldType.logicalType(new NanosDuration()));
          break;
        case DURATION_ARRAY:
          schemaField =
              Schema.Field.nullable(
                  field, Schema.FieldType.array(Schema.FieldType.logicalType(new NanosDuration())));
          break;
        case FLOAT:
          schemaField = Schema.Field.nullable(field, FieldType.DOUBLE);
          break;
        case FLOAT_ARRAY:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.array(FieldType.DOUBLE));
          break;
        case INTEGER:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.INT64);
          break;
        case INTEGER_ARRAY:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.array(FieldType.INT64));
          break;
        case LOCAL_DATETIME:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.logicalType(new DateTime()));
          break;
        case LOCAL_DATETIME_ARRAY:
          schemaField =
              Schema.Field.nullable(
                  field, Schema.FieldType.array(Schema.FieldType.logicalType(new DateTime())));
          break;
        case LOCAL_TIME:
        case ZONED_TIME:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.logicalType(new Time()));
          break;
        case LOCAL_TIME_ARRAY:
        case ZONED_TIME_ARRAY:
          schemaField =
              Schema.Field.nullable(
                  field, Schema.FieldType.array(Schema.FieldType.logicalType(new Time())));
          break;
        case POINT:
        case STRING:
          schemaField = Schema.Field.nullable(field, FieldType.STRING);
          break;
        case POINT_ARRAY:
        case STRING_ARRAY:
          schemaField = Schema.Field.nullable(field, Schema.FieldType.array(FieldType.STRING));
          break;
        case ZONED_DATETIME:
          schemaField =
              Schema.Field.nullable(field, Schema.FieldType.logicalType(new IsoDateTime()));
          break;
        case ZONED_DATETIME_ARRAY:
          schemaField =
              Schema.Field.nullable(
                  field, Schema.FieldType.array(Schema.FieldType.logicalType(new IsoDateTime())));
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported property type: %s", propertyType));
      }
      fields.add(schemaField);
    }
    return new Schema(fields);
  }

  public static Schema textToBeamSchema(List<String> fields) {
    return new Schema(
        fields.stream()
            .map(field -> Schema.Field.of(field, FieldType.STRING).withNullable(true))
            .collect(Collectors.toList()));
  }

  private static Field defaultFieldSchema(String field) {
    return Field.nullable(field, FieldType.STRING);
  }
}
