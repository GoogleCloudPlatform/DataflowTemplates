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
package com.google.cloud.dataflow.cdc.connector;

import com.google.cloud.dataflow.cdc.common.DataflowCdcRowFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

/**
 * Translate a Debezium SourceRecord into a Beam {@class Row} object to be pushed to PubSub.
 *
 * <p>Information on the Debezium schema:
 * https://debezium.io/docs/connectors/mysql/#change-events-value
 */
public class DebeziumSourceRecordToDataflowCdcFormatTranslator {

  /** The type of operation being performed in a database. */
  public static final class Operation {
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String READ = "READ";
  }

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DebeziumSourceRecordToDataflowCdcFormatTranslator.class);

  private final Map<String, org.apache.beam.sdk.schemas.Schema> knownSchemas = new HashMap<>();

  public Row translate(SourceRecord record) {
    LOG.debug("Source Record from Debezium: {}", record);

    String qualifiedTableName = record.topic();

    Struct recordValue = (Struct) record.value();
    if (recordValue == null) {
      return null;
    }

    // TODO: Consider including before value in the Row.
    Struct afterValue = recordValue.getStruct("after");
    Row afterValueRow = afterValue == null ? null : handleValue(afterValue.schema(), afterValue);
    LOG.debug("Beam Row is {}", afterValueRow);

    Row primaryKey = null;
    boolean hasPK = true;
    if (record.key() == null) {
      hasPK = false;
    } else {
      primaryKey = handleValue(record.keySchema(), record.key());
      LOG.debug("Key Schema: {} | Key Value: {}", primaryKey.getSchema(), primaryKey);
    }

    String sourceRecordOp = recordValue.getString("op");
    String operation = translateOperation(sourceRecordOp);
    if (operation == null) {
      return null;
    }

    Long timestampMs = recordValue.getInt64("ts_ms");

    if (!knownSchemas.containsKey(qualifiedTableName)) {
      org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder =
          org.apache.beam.sdk.schemas.Schema.builder()
              .addStringField(DataflowCdcRowFormat.OPERATION)
              .addStringField(DataflowCdcRowFormat.TABLE_NAME)
              .addField(
                  org.apache.beam.sdk.schemas.Schema.Field.nullable(
                      DataflowCdcRowFormat.FULL_RECORD, FieldType.row(afterValueRow.getSchema())))
              .addInt64Field(DataflowCdcRowFormat.TIMESTAMP_MS);

      if (hasPK) {
        schemaBuilder.addRowField(DataflowCdcRowFormat.PRIMARY_KEY, primaryKey.getSchema());
      }
      knownSchemas.put(qualifiedTableName, schemaBuilder.build());
    }
    org.apache.beam.sdk.schemas.Schema finalBeamSchema = knownSchemas.get(qualifiedTableName);

    Row.Builder beamRowBuilder =
        Row.withSchema(finalBeamSchema)
            .addValue(operation)
            .addValue(qualifiedTableName)
            .addValue(afterValueRow)
            .addValue(timestampMs);

    if (hasPK) {
      beamRowBuilder.addValue(primaryKey);
    }

    return beamRowBuilder.build();
  }

  private static String translateOperation(String op) {
    String res;

    switch (op) {
      case "c":
        res = Operation.INSERT;
        break;
      case "u":
        res = Operation.UPDATE;
        break;
      case "d":
        res = Operation.DELETE;
        break;
      case "r":
        res = Operation.READ;
        break;
      default:
        res = null;
        break;
    }
    return res;
  }

  private static org.apache.beam.sdk.schemas.Schema kafkaSchemaToBeamRowSchema(
      org.apache.kafka.connect.data.Schema schema) {
    assert schema.type() == Schema.Type.STRUCT;

    org.apache.beam.sdk.schemas.Schema.Builder beamSchemaBuilder =
        org.apache.beam.sdk.schemas.Schema.builder();
    for (Field f : schema.fields()) {
      Schema.Type t = f.schema().type();
      org.apache.beam.sdk.schemas.Schema.Field beamField;
      switch (t) {
        case INT8:
        case INT16:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT16);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT16);
          }
          break;
        case INT32:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT32);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT32);
          }
          break;
        case INT64:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT64);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT64);
          }
          break;
        case FLOAT32:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.FLOAT);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.FLOAT);
          }
          break;
        case FLOAT64:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.DOUBLE);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.DOUBLE);
          }
          break;
        case BOOLEAN:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.BOOLEAN);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.BOOLEAN);
          }
          break;
        case STRING:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.STRING);
          } else {
            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.STRING);
          }
          break;
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(f.schema().name())) {
            if (f.schema().isOptional()) {
              beamField =
                  org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.DECIMAL);
            } else {
              beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.DECIMAL);
            }
          } else {
            if (f.schema().isOptional()) {
              beamField =
                  org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.BYTES);
            } else {
              beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.BYTES);
            }
          }
          break;
        case STRUCT:
          if (f.schema().isOptional()) {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.nullable(
                    f.name(), FieldType.row(kafkaSchemaToBeamRowSchema(f.schema())));
          } else {
            beamField =
                org.apache.beam.sdk.schemas.Schema.Field.of(
                    f.name(), FieldType.row(kafkaSchemaToBeamRowSchema(f.schema())));
          }
          break;
        case MAP:
          throw new DataException("Map types are not supported.");
        case ARRAY:
          throw new DataException("Array types are not supported.");
        default:
          throw new DataException(String.format("Unsupported data type: {}", t));
      }
      if (f.schema().name() != null && !f.schema().name().isEmpty()) {
        FieldType fieldType = beamField.getType();
        fieldType = fieldType.withMetadata("logicalType", f.schema().name());
        beamField = beamField.withType(fieldType).withDescription(f.schema().name());
      }
      beamSchemaBuilder.addField(beamField);
    }
    return beamSchemaBuilder.build();
  }

  private static Row kafkaSourceRecordToBeamRow(Struct value, Row.Builder rowBuilder) {
    org.apache.beam.sdk.schemas.Schema beamSchema = rowBuilder.getSchema();
    for (org.apache.beam.sdk.schemas.Schema.Field f : beamSchema.getFields()) {
      switch (f.getType().getTypeName()) {
        case INT16:
          rowBuilder.addValue(value.getInt16(f.getName()));
          break;
        case INT32:
          rowBuilder.addValue(value.getInt32(f.getName()));
          break;
        case INT64:
          rowBuilder.addValue(value.getInt64(f.getName()));
          break;
        case FLOAT:
          rowBuilder.addValue(value.getFloat32(f.getName()));
          break;
        case DOUBLE:
          rowBuilder.addValue(value.getFloat64(f.getName()));
          break;
        case BOOLEAN:
          rowBuilder.addValue(value.getBoolean(f.getName()));
          break;
        case STRING:
          rowBuilder.addValue(value.getString(f.getName()));
          break;
        case DECIMAL:
          rowBuilder.addValue(value.get(f.getName()));
          break;
        case BYTES:
          rowBuilder.addValue(value.getBytes(f.getName()));
          break;
        case ROW:
          Row.Builder nestedRowBuilder = Row.withSchema(f.getType().getRowSchema());
          rowBuilder.addValue(
              kafkaSourceRecordToBeamRow(value.getStruct(f.getName()), nestedRowBuilder));
          break;
        case MAP:
          throw new DataException("Map types are not supported.");
        case ARRAY:
          throw new DataException("Array types are not supported.");
        default:
          throw new DataException(
              String.format("Unsupported data type: {}", f.getType().getTypeName()));
      }
    }
    return rowBuilder.build();
  }

  private static Row handleValue(org.apache.kafka.connect.data.Schema schema, Object value) {
    org.apache.beam.sdk.schemas.Schema beamSchema = kafkaSchemaToBeamRowSchema(schema);
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    return kafkaSourceRecordToBeamRow((Struct) value, rowBuilder);
  }
}
