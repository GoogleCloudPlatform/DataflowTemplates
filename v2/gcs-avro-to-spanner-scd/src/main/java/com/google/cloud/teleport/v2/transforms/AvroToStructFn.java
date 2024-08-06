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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.ValueBinder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;

/** Transforms Avro GenericRecords into Spanner Structs. */
@AutoValue
public abstract class AvroToStructFn extends SimpleFunction<GenericRecord, Struct> {

  public static AvroToStructFn create() {
    return new AutoValue_AvroToStructFn();
  }

  @Override
  public Struct apply(GenericRecord record) {
    Schema avroSchema = checkNotNull(record.getSchema(), "Input file Avro Schema is null.");

    Struct.Builder structBuilder = Struct.newBuilder();
    for (Field field : avroSchema.getFields()) {
      ValueBinder<Struct.Builder> fieldSetter = structBuilder.set(field.name());

      Schema.Type fieldType = field.schema().getType();
      switch (fieldType) {
        default:
        case ARRAY:
        case ENUM:
        case MAP:
        case NULL:
        case RECORD:
        case UNION:
          // TODO: implement support for Union when it's type + null.
          throw new UnsupportedOperationException(
              String.format("Avro field type %s is not supported.", fieldType));
        case BOOLEAN:
          fieldSetter.to(Value.bool((boolean) record.get(field.name())));
          break;
        case BYTES:
        case FIXED:
          // TODO: Implement FIXED and BYTES including LogicalTypes.
          throw new UnsupportedOperationException(
              String.format("Support for Avro field type %s is not implemented yet.", fieldType));
        case DOUBLE:
          fieldSetter.to(Value.float64((double) record.get(field.name())));
          break;
        case FLOAT:
          fieldSetter.to(Value.float32((float) record.get(field.name())));
          break;
        case INT:
        case LONG:
          // TODO: Implement Logical Type for Long timestamp
          fieldSetter.to(Value.int64((long) record.get(field.name())));
          break;
        case STRING:
          fieldSetter.to(Value.string(record.get(field.name()).toString()));
          break;
      }
    }

    return structBuilder.build();
  }
}
