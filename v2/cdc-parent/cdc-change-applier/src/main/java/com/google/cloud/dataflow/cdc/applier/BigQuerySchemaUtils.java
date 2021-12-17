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
package com.google.cloud.dataflow.cdc.applier;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.ArrayList;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/** Utilities to convert between Beam and BigQuery schemas. */
public class BigQuerySchemaUtils {

  public static Schema beamSchemaToBigQueryClientSchema(
      org.apache.beam.sdk.schemas.Schema tableSchema) {
    ArrayList<Field> bqFields = new ArrayList<>(tableSchema.getFieldCount());

    for (org.apache.beam.sdk.schemas.Schema.Field f : tableSchema.getFields()) {
      bqFields.add(beamFieldToBigQueryClientField(f));
    }
    return Schema.of(bqFields);
  }

  private static Field beamFieldToBigQueryClientField(
      org.apache.beam.sdk.schemas.Schema.Field beamField) {
    TypeName beamTypeName = beamField.getType().getTypeName();
    StandardSQLTypeName bigQueryTypeName;
    switch (beamTypeName) {
      case BYTES:
      case BYTE:
        bigQueryTypeName = StandardSQLTypeName.BYTES;
        break;
      case FLOAT:
      case DOUBLE:
        bigQueryTypeName = StandardSQLTypeName.FLOAT64;
        break;
      case INT16:
      case INT32:
      case INT64:
        bigQueryTypeName = StandardSQLTypeName.INT64;
        break;
      case STRING:
        bigQueryTypeName = StandardSQLTypeName.STRING;
        break;
      case DATETIME:
        bigQueryTypeName = StandardSQLTypeName.DATETIME;
        break;
      case BOOLEAN:
        bigQueryTypeName = StandardSQLTypeName.BOOL;
        break;
      case DECIMAL:
        bigQueryTypeName = StandardSQLTypeName.NUMERIC;
        break;
      case MAP:
      case ROW:
      case ARRAY:
      case LOGICAL_TYPE:
      default:
        // In the first version of this solution, we will not support sub-fields of type
        // MAP/ROW/ARRAY/LOGICAL_TYPE. These may become necessary as the solution expands to other
        // databases.
        throw new IllegalArgumentException(
            String.format("Unsupported field type: %s", beamTypeName));
    }
    return Field.newBuilder(beamField.getName(), bigQueryTypeName).build();
  }
}
