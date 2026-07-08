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
        ModelUtils.getAllPropertyMappings(entityTarget, startNodeTarget, endNodeTarget)) {
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
      schemaField =
          switch (propertyType.getName()) {
            case BOOLEAN -> Field.nullable(field, FieldType.BOOLEAN);
            case BOOLEAN_ARRAY -> Field.nullable(field, FieldType.array(FieldType.BOOLEAN));
            case BYTE_ARRAY -> Field.nullable(field, FieldType.BYTES);
            case DATE -> Field.nullable(field, FieldType.logicalType(new Date()));
            case DATE_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.logicalType(new Date())));
            case DURATION -> Field.nullable(field, FieldType.logicalType(new NanosDuration()));
            case DURATION_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.logicalType(new NanosDuration())));
            case FLOAT -> Field.nullable(field, FieldType.DOUBLE);
            case FLOAT_ARRAY -> Field.nullable(field, FieldType.array(FieldType.DOUBLE));
            case INTEGER -> Field.nullable(field, FieldType.INT64);
            case INTEGER_ARRAY -> Field.nullable(field, FieldType.array(FieldType.INT64));
            case LOCAL_DATETIME -> Field.nullable(field, FieldType.logicalType(new DateTime()));
            case LOCAL_DATETIME_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.logicalType(new DateTime())));
            case LOCAL_TIME, ZONED_TIME -> Field.nullable(field, FieldType.logicalType(new Time()));
            case LOCAL_TIME_ARRAY, ZONED_TIME_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.logicalType(new Time())));
            case POINT, STRING -> Field.nullable(field, FieldType.STRING);
            case POINT_ARRAY, STRING_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.STRING));
            case ZONED_DATETIME -> Field.nullable(field, FieldType.logicalType(new IsoDateTime()));
            case ZONED_DATETIME_ARRAY -> Field.nullable(
                field, FieldType.array(FieldType.logicalType(new IsoDateTime())));
            default -> throw new IllegalArgumentException(
                String.format("Unsupported property type: %s", propertyType));
          };
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
