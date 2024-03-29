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
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.commons.lang3.StringUtils;

/** Utilities for organizing Bean rows and schema. */
public class BeamUtils {

  public static Schema toBeamSchema(Target target) {

    // map source column names to order
    List<Schema.Field> fields = new ArrayList<>();
    // Map these fields to a schema in a row
    for (int i = 0; i < target.getMappings().size(); i++) {
      Mapping mapping = target.getMappings().get(i);
      if (mapping.getRole() == RoleType.type || mapping.getRole() == RoleType.label) {
        continue;
      }

      String fieldName = "";
      if (StringUtils.isNotBlank(mapping.getField())) {
        fieldName = mapping.getField();
      } else if (StringUtils.isNotBlank(mapping.getName())) {
        fieldName = mapping.getName();
      } else if (StringUtils.isNotBlank(mapping.getConstant())) {
        fieldName = mapping.getConstant();
      }

      if (StringUtils.isEmpty(fieldName)) {
        throw new RuntimeException(
            "Could not find field name or constant in target: " + target.getName());
      }
      Schema.Field schemaField;
      if (mapping.getType() == PropertyType.Integer || mapping.getType() == PropertyType.Long) {
        // avoid truncation by making it long
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.INT64);
      } else if (mapping.getType() == PropertyType.Double) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DOUBLE);
      } else if (mapping.getType() == PropertyType.Float) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DOUBLE);
      } else if (mapping.getType() == PropertyType.Boolean) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BOOLEAN);
      } else if (mapping.getType() == PropertyType.ByteArray) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BYTES);
      } else if (mapping.getType() == PropertyType.Point) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
      } else if (mapping.getType() == PropertyType.Duration) {
        schemaField =
            Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new NanosDuration()));
      } else if (mapping.getType() == PropertyType.Date) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new Date()));
      } else if (mapping.getType() == PropertyType.LocalDateTime) {
        schemaField =
            Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new DateTime()));
      } else if (mapping.getType() == PropertyType.DateTime) {
        schemaField =
            Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new IsoDateTime()));
      } else if (mapping.getType() == PropertyType.LocalTime) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new Time()));
      } else if (mapping.getType() == PropertyType.Time) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.logicalType(new Time()));
      } else {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
      }
      fields.add(schemaField);
    }
    return new Schema(fields);
  }

  public static Schema textToBeamSchema(String[] fieldNames) {
    // map source column names to order
    List<Schema.Field> fields = new ArrayList<>();
    // Map these fields to a schema in a row
    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      Schema.Field schemaField = Schema.Field.of(fieldName, Schema.FieldType.STRING);
      fields.add(schemaField);
    }
    return new Schema(fields);
  }
}
