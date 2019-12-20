/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.dataflow.cdc.connector;

import com.google.cloud.dataflow.cdc.common.DataflowCdcRowFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

/**
 * Translate a Debezium SourceRecord into a Beam {@class Row} object to be pushed to PubSub.
 *
 * Information on the Debezium schema:
 * https://debezium.io/docs/connectors/mysql/#change-events-value
 */
public class DebeziumSourceRecordToDataflowCdcFormatTranslator {

  /**
   * The type of operation being performed in a database.
   */
  public static final class Operation {
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String READ = "READ";
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      DebeziumSourceRecordToDataflowCdcFormatTranslator.class);

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
    if (record.key() == null) {
      return null;
    }

    primaryKey = handleValue(record.keySchema(), record.key());
    LOG.debug("Key Schema: {} | Key Value: {}", primaryKey.getSchema(), primaryKey);

    String sourceRecordOp = recordValue.getString("op");
    String operation = translateOperation(sourceRecordOp);
    if (operation == null) {
      return null;
    }

    Long timestampMs = recordValue.getInt64("ts_ms");

    if (!knownSchemas.containsKey(qualifiedTableName)) {
      knownSchemas.put(
          qualifiedTableName,
          org.apache.beam.sdk.schemas.Schema
              .builder()
              .addStringField(DataflowCdcRowFormat.OPERATION)
              .addStringField(DataflowCdcRowFormat.TABLE_NAME)
              .addRowField(DataflowCdcRowFormat.PRIMARY_KEY, primaryKey.getSchema())
              .addField(org.apache.beam.sdk.schemas.Schema.Field.nullable(
                  DataflowCdcRowFormat.FULL_RECORD, FieldType.row(afterValueRow.getSchema())))
              .addInt64Field(DataflowCdcRowFormat.TIMESTAMP_MS)
              .build());
    }
    org.apache.beam.sdk.schemas.Schema finalBeamSchema = knownSchemas.get(qualifiedTableName);

    return Row.withSchema(finalBeamSchema)
        .addValue(operation)
        .addValue(qualifiedTableName)
        .addValue(primaryKey)
        .addValue(afterValueRow)
        .addValue(timestampMs)
        .build();
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
      switch (t) {
        case INT8:
        case INT16:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.INT16);
          } else {
            beamSchemaBuilder.addInt16Field(f.name());
          }
          break;
        case INT32:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.INT32);
          } else {
            beamSchemaBuilder.addInt32Field(f.name());
          }
          break;
        case INT64:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.INT64);
          } else {
            beamSchemaBuilder.addInt64Field(f.name());
          }
          break;
        case FLOAT32:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.FLOAT);
          } else {
            beamSchemaBuilder.addFloatField(f.name());
          }
          break;
        case FLOAT64:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.DOUBLE);
          } else {
            beamSchemaBuilder.addDoubleField(f.name());
          }
          break;
        case BOOLEAN:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.BOOLEAN);
          } else {
            beamSchemaBuilder.addBooleanField(f.name());
          }
          break;
        case STRING:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.STRING);
          } else {
            beamSchemaBuilder.addStringField(f.name());
          }
          break;
        case BYTES:
          if (f.schema().isOptional()) {
            beamSchemaBuilder.addNullableField(f.name(), FieldType.BYTES);
          } else {
            beamSchemaBuilder.addByteArrayField(f.name());
          }
          break;
        case STRUCT:
          beamSchemaBuilder.addRowField(f.name(), kafkaSchemaToBeamRowSchema(f.schema()));
          break;
        case MAP:
          throw new DataException("Map types are not supported.");
        case ARRAY:
          throw new DataException("Array types are not supported.");
        default:
          throw new DataException(String.format("Unsupported data type: {}", t));
      }
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
    org.apache.beam.sdk.schemas.Schema beamSchema =  kafkaSchemaToBeamRowSchema(schema);
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    return kafkaSourceRecordToBeamRow((Struct) value, rowBuilder);
  }
}
