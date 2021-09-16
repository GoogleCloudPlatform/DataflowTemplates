/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.SchemaBuilder.TypeBuilder;

/** Utility methods for Dataplex and Avro schemas. */
public final class Schemas {

  /** Convert Dataplex schema to a correpsonding Avro schema. */
  public static Schema dataplexSchemaToAvro(GoogleCloudDataplexV1Schema dataplexSchema)
      throws SchemaConversionException {
    return dataplexFieldsToAvro(dataplexSchema.getFields(), SchemaBuilder.record("Schema"));
  }

  private static Schema dataplexFieldsToAvro(
      List<GoogleCloudDataplexV1SchemaSchemaField> dataplexFields,
      RecordBuilder<Schema> avroRecordBuilder)
      throws SchemaConversionException {
    FieldAssembler<Schema> avroFieldAssembler = avroRecordBuilder.fields();
    for (GoogleCloudDataplexV1SchemaSchemaField dataplexField : dataplexFields) {
      TypeBuilder<Schema> fieldTypeBuilder = SchemaBuilder.builder();
      BaseTypeBuilder<Schema> fieldTypeBuilderWithMode;
      switch (dataplexField.getMode()) {
        case "NULLABLE ":
          fieldTypeBuilderWithMode = fieldTypeBuilder.nullable();
          break;
        case "REQUIRED":
          fieldTypeBuilderWithMode = fieldTypeBuilder;
          break;
        case "REPEATED":
          fieldTypeBuilderWithMode = fieldTypeBuilder.array().items();
          break;
        default:
          throw new SchemaConversionException(
              "Unsupported Dataplex Schema Mode: " + dataplexField.getMode());
      }

      Schema fieldType;
      switch (dataplexField.getType()) {
        case "BOOLEAN":
          fieldType = fieldTypeBuilderWithMode.booleanType();
          break;
        case "BYTE":
        case "INT16":
        case "INT32":
          // Avro only has 32 and 64 bit integers
          fieldType = fieldTypeBuilderWithMode.intType();
          break;
        case "INT64":
          fieldType = fieldTypeBuilderWithMode.longType();
          break;
        case "FLOAT":
          fieldType = fieldTypeBuilderWithMode.floatType();
          break;
        case "DECIMAL":
        case "DOUBLE":
          // Avro only has float and double
          fieldType = fieldTypeBuilderWithMode.doubleType();
          break;
        case "TIMESTAMP":
        case "DATE":
        case "TIME":
        case "STRING":
          // Ideally the date and time related types would translate to numerical Avro types with
          // logical types: "date", "time-*", "timestamp-*", "local-timestamp-*", however the input
          // JSON and CSV files represent dates and times as strings and it's not clear if the
          // format will always be consistent.
          // TODO(olegsa) figure out if Dataplex parses the dates and times from JSONs and CSVs;
          // and if so where the formats are stored, or how they are derived
          fieldType = fieldTypeBuilderWithMode.stringType();
          break;
        case "BINARY":
          fieldType = fieldTypeBuilderWithMode.bytesType();
          break;
        case "RECORD":
          fieldType =
              dataplexFieldsToAvro(
                  dataplexField.getFields(),
                  fieldTypeBuilderWithMode.record(dataplexField.getName() + ".Record"));
          break;
        default:
          throw new SchemaConversionException(
              "Unsupported Dataplex schema field type: " + dataplexField.getType());
      }

      avroFieldAssembler =
          avroFieldAssembler.name(dataplexField.getName()).type(fieldType).noDefault();
    }
    return avroFieldAssembler.endRecord();
  }
}
