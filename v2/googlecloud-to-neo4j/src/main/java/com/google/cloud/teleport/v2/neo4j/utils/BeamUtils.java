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

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for organizing Bean rows and schema. */
public class BeamUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BeamUtils.class);

  public static String getSchemaFieldNameCsv(Schema schema) {
    return StringUtils.join(schema.getFields(), ",");
  }

  public static Schema toBeamSchema(com.google.cloud.bigquery.Schema bqSchema) {
    List<Schema.Field> schemaFieldList = new ArrayList<>();
    for (int i = 0; i < bqSchema.getFields().size(); i++) {
      com.google.cloud.bigquery.Field field = bqSchema.getFields().get(i);
      Schema.Field schemaField =
          Schema.Field.nullable(field.getName(), bigQueryToBeamFieldType(field));
      schemaFieldList.add(schemaField);
    }
    return new Schema(schemaFieldList);
  }

  public static Schema.FieldType bigQueryToBeamFieldType(com.google.cloud.bigquery.Field field) {

    LegacySQLTypeName legacySQLTypeName = field.getType();
    if (LegacySQLTypeName.STRING.equals(legacySQLTypeName)) {
      return Schema.FieldType.STRING;
    } else if (LegacySQLTypeName.TIMESTAMP.equals(legacySQLTypeName)) {
      return Schema.FieldType.DATETIME;
    } else if (LegacySQLTypeName.DATE.equals(legacySQLTypeName)) {
      return Schema.FieldType.DATETIME;
    } else if (LegacySQLTypeName.BYTES.equals(legacySQLTypeName)) {
      return Schema.FieldType.BYTES;
    } else if (LegacySQLTypeName.BOOLEAN.equals(legacySQLTypeName)) {
      return Schema.FieldType.BOOLEAN;
    } else if (LegacySQLTypeName.NUMERIC.equals(legacySQLTypeName)) {
      return Schema.FieldType.DOUBLE;
    } else if (LegacySQLTypeName.FLOAT.equals(legacySQLTypeName)) {
      return Schema.FieldType.FLOAT;
    } else if (LegacySQLTypeName.INTEGER.equals(legacySQLTypeName)) {
      return Schema.FieldType.INT64;
    }
    throw new UnsupportedOperationException(
        "LegacySQL type " + legacySQLTypeName.getStandardType() + " not supported.");
  }

  public static Schema toBeamSchema(Target target) {

    // map source column names to order
    List<Schema.Field> fields = new ArrayList<>();
    // Map these fields to a schema in a row
    for (int i = 0; i < target.getMappings().size(); i++) {
      Mapping mapping = target.getMappings().get(i);
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
      } else if (mapping.getType() == PropertyType.BigDecimal) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DECIMAL);
      } else if (mapping.getType() == PropertyType.Float) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.FLOAT);
      } else if (mapping.getType() == PropertyType.Boolean) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BOOLEAN);
      } else if (mapping.getType() == PropertyType.ByteArray) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.BYTES);
      } else if (mapping.getType() == PropertyType.Point) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
      } else if (mapping.getType() == PropertyType.Duration) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DECIMAL);
      } else if (mapping.getType() == PropertyType.Date) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
      } else if (mapping.getType() == PropertyType.LocalDateTime) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
      } else if (mapping.getType() == PropertyType.DateTime) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
      } else if (mapping.getType() == PropertyType.LocalTime) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.DATETIME);
      } else if (mapping.getType() == PropertyType.Time) {
        schemaField = Schema.Field.nullable(fieldName, Schema.FieldType.STRING);
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
