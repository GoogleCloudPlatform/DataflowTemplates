/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;

/** Transforms Avro GenericRecords into Spanner Structs. */
public class AvroToStructFn extends SimpleFunction<GenericRecord, Struct> {

  public static AvroToStructFn create() {
    return new AvroToStructFn();
  }

  @Override
  public Struct apply(GenericRecord record) {
    return GenericRecordConverter.of(record).toStruct();
  }

  private static class GenericRecordConverter {

    private final GenericRecord record;

    public GenericRecordConverter(GenericRecord record) {
      this.record = record;
    }

    public static GenericRecordConverter of(GenericRecord record) {
      return new GenericRecordConverter(record);
    }

    public Struct toStruct() {
      var structBuilder = Struct.newBuilder();
      Schema avroSchema = checkNotNull(record.getSchema(), "Input file Avro Schema is null.");
      avroSchema
          .getFields()
          .forEach(field -> structBuilder.set(field.name()).to(getFieldValue(field)));
      return structBuilder.build();
    }

    private Value getFieldValue(Field field) {
      Schema.Type fieldType = field.schema().getType();
      Object fieldValue = record.get(field.name());
      switch (fieldType) {
        default:
        case ARRAY:
        case ENUM:
        case MAP:
        case NULL:
        case RECORD:
          throw new UnsupportedOperationException(
              String.format("Avro field type %s is not supported.", fieldType));
        case BOOLEAN:
          return Value.bool(fieldValue == null ? NullTypes.NULL_BOOLEAN : (Boolean) fieldValue);
        case BYTES:
        case FIXED:
          // TODO: Implement FIXED and BYTES including LogicalTypes.
          throw new UnsupportedOperationException(
              String.format("Support for Avro field type %s is not implemented yet.", fieldType));
        case DOUBLE:
          return Value.float64(fieldValue == null ? NullTypes.NULL_FLOAT64 : (Double) fieldValue);
        case FLOAT:
          return Value.float32(fieldValue == null ? NullTypes.NULL_FLOAT32 : (Float) fieldValue);
        case INT:
          return Value.int64(
              fieldValue == null ? NullTypes.NULL_INT64 : new Long((Integer) fieldValue));
        case LONG:
          // TODO: Implement Logical Type for Long timestamp
          return Value.int64(fieldValue == null ? NullTypes.NULL_INT64 : (Long) fieldValue);
        case STRING:
          return Value.string(fieldValue == null ? NullTypes.NULL_STRING : fieldValue.toString());
        case UNION:
          return getUnionFieldValue(field);
      }
    }

    private Value getUnionFieldValue(Field field) {
      List<Schema> unionTypes = field.schema().getTypes();
      if (unionTypes.size() != 2) {
        throw new UnsupportedOperationException(
            String.format(
                "UNION is only supported for nullable fields. Got: %s.", unionTypes.toString()));
      }

      // It is not possible to have UNION of same type (e.g. NULL, NULL).
      if (unionTypes.get(0).getType() == Schema.Type.NULL) {
        return getFieldValue(new Field(field.name(), Schema.create(unionTypes.get(1).getType())));
      }
      if (unionTypes.get(1).getType() == Schema.Type.NULL) {
        return getFieldValue(new Field(field.name(), Schema.create(unionTypes.get(0).getType())));
      }

      throw new UnsupportedOperationException(
          String.format(
              "UNION is only supported for nullable fields. Got: %s.", unionTypes.toString()));
    }
  }
}
