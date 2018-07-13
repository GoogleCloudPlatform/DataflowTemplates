/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Table;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Converts {@link GenericRecord} to {@link Mutation}. */
public class AvroRecordConverter implements SerializableFunction<GenericRecord, Mutation> {

  private final Table table;

  public AvroRecordConverter(Table table) {
    this.table = table;
  }

  public Mutation apply(GenericRecord record) {
    Schema schema = record.getSchema();
    List<Schema.Field> fields = schema.getFields();
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());

    for (Schema.Field field : fields) {
      String fieldName = field.name();

      Column column = table.column(fieldName);
      if (column == null) {
        throw new IllegalArgumentException(
            "Cannot find a corresponding column for field "
                + fieldName
                + " in table "
                + table.prettyPrint()
                + " schema "
                + schema.toString(true));
      }

      Schema avroFieldSchema = field.schema();
      if (avroFieldSchema.getType() == Schema.Type.UNION) {
        Schema unpacked = AvroUtil.unpackNullable(avroFieldSchema);
        if (unpacked != null) {
          avroFieldSchema = unpacked;
        }
      }
      LogicalType logicalType = LogicalTypes.fromSchema(avroFieldSchema);
      Schema.Type avroType = avroFieldSchema.getType();

      switch (column.type().getCode()) {
        case BOOL:
          builder.set(column.name()).to(readBool(record, avroType, fieldName).orElse(null));
          break;
        case INT64:
          builder.set(column.name()).to(readInt64(record, avroType, fieldName).orElse(null));
          break;
        case FLOAT64:
          builder.set(column.name()).to(readFloat64(record, avroType, fieldName).orElse(null));
          break;
        case STRING:
          builder.set(column.name()).to(readString(record, avroType, fieldName).orElse(null));
          break;
        case BYTES:
          builder.set(column.name()).to(readBytes(record, avroType, fieldName).orElse(null));
          break;
        case TIMESTAMP:
          builder
              .set(column.name())
              .to(readTimestamp(record, avroType, logicalType, fieldName).orElse(null));
          break;
        case DATE:
          builder.set(column.name()).to(readDate(record, avroType, fieldName).orElse(null));
          break;
        case ARRAY:
          {
            Schema arraySchema = avroFieldSchema.getElementType();
            if (arraySchema.getType() == Schema.Type.UNION) {
              Schema unpacked = AvroUtil.unpackNullable(arraySchema);
              if (unpacked != null) {
                arraySchema = unpacked;
              }
            }
            LogicalType arrayLogicalType = LogicalTypes.fromSchema(arraySchema);
            Schema.Type arrayType = arraySchema.getType();
            switch (column.type().getArrayElementType().getCode()) {
              case BOOL:
                builder
                    .set(column.name())
                    .toBoolArray(readBoolArray(record, arrayType, fieldName).orElse(null));
                break;
              case INT64:
                builder
                    .set(column.name())
                    .toInt64Array(readInt64Array(record, arrayType, fieldName).orElse(null));
                break;
              case FLOAT64:
                builder
                    .set(column.name())
                    .toFloat64Array(readFloat64Array(record, arrayType, fieldName).orElse(null));
                break;
              case STRING:
                builder
                    .set(column.name())
                    .toStringArray(readStringArray(record, arrayType, fieldName).orElse(null));
                break;
              case BYTES:
                builder
                    .set(column.name())
                    .toBytesArray(readBytesArray(record, arrayType, fieldName).orElse(null));
                break;
              case TIMESTAMP:
                builder
                    .set(column.name())
                    .toTimestampArray(
                        readTimestampArray(record, arrayType, arrayLogicalType, fieldName)
                            .orElse(null));
                break;
              case DATE:
                builder
                    .set(column.name())
                    .toDateArray(readDateArray(record, arrayType, fieldName).orElse(null));
                break;
              default:
                throw new IllegalArgumentException(
                    "Cannot convert a field "
                        + fieldName
                        + " in schema "
                        + schema.toString(true)
                        + " table "
                        + table.prettyPrint());
            }
            break;
          }
        default:
          throw new IllegalArgumentException(
              "Cannot convert a field "
                  + fieldName
                  + " in schema "
                  + " schema "
                  + schema.toString(true)
                  + " table "
                  + table.prettyPrint());
      }
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private Optional<List<ByteArray>> readBytesArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : ByteArray.copyFrom(x.getBytes()))
                  .collect(Collectors.toList()));
        }
      case BYTES:
        {
          List<ByteBuffer> value = (List<ByteBuffer>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : ByteArray.copyFrom(x))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BYTES");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<Date>> readDateArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : Date.parseDate(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as DATE");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<Timestamp>> readTimestampArray(
      GenericRecord record, Schema.Type avroType, LogicalType logicalType, String fieldName) {
    switch (avroType) {
      case LONG:
        {
          List<Long> value = (List<Long>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          if (LogicalTypes.timestampMillis().equals(logicalType)) {
            return Optional.of(
                value
                    .stream()
                    .map(x -> x == null ? null : Timestamp.ofTimeMicroseconds(1000L * x))
                    .collect(Collectors.toList()));
          }
          if (LogicalTypes.timestampMicros().equals(logicalType)) {
            return Optional.of(
                value
                    .stream()
                    .map(x -> x == null ? null : Timestamp.ofTimeMicroseconds(x))
                    .collect(Collectors.toList()));
          }
          // Default to microseconds
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : Timestamp.ofTimeMicroseconds(x))
                  .collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : Timestamp.parseTimestamp(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as TIMESTAMP");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<String>> readStringArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {

    switch (avroType) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case LONG:
      case INT:
        {
          List<Object> value = (List<Object>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : String.valueOf(x))
                  .collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : x.toString())
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as STRING");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<Double>> readFloat64Array(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case FLOAT:
      case DOUBLE:
      case LONG:
      case INT:
        {
          // Here we rely on java type casting.
          List<Object> value = (List) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value.stream().map(x -> x == null ? null : (double) x).collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : Double.parseDouble(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as FLOAT64");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<Long>> readInt64Array(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case LONG:
      case INT:
        {
          List<Object> value = (List<Object>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value.stream().map(x -> x == null ? null : (long) x).collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(
              value
                  .stream()
                  .map(x -> x == null ? null : Long.parseLong(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as INT64");
    }
  }

  @SuppressWarnings("unchecked")
  private Optional<List<Boolean>> readBoolArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case BOOLEAN:
        return Optional.ofNullable((List<Boolean>) record.get(fieldName));
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          if (value == null) {
            return Optional.empty();
          }
          List<Boolean> result =
              value
                  .stream()
                  .map(x -> x == null ? null : Boolean.parseBoolean(x.toString()))
                  .collect(Collectors.toList());
          return Optional.of(result);
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BOOL");
    }
  }

  private Optional<Date> readDate(GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Date::parseDate);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as DATE");
    }
  }

  private Optional<Timestamp> readTimestamp(
      GenericRecord record, Schema.Type avroType, LogicalType logicalType, String fieldName) {
    switch (avroType) {
      case LONG:
        if (LogicalTypes.timestampMillis().equals(logicalType)) {
          return Optional.ofNullable((Long) record.get(fieldName))
              .map(x -> Timestamp.ofTimeMicroseconds(1000L * x));
        }
        if (LogicalTypes.timestampMicros().equals(logicalType)) {
          return Optional.ofNullable((Long) record.get(fieldName))
              .map(Timestamp::ofTimeMicroseconds);
        }
        // Default to micro-seconds.
        return Optional.ofNullable((Long) record.get(fieldName)).map(Timestamp::ofTimeMicroseconds);
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Timestamp::parseTimestamp);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as TIMESTAMP");
    }
  }

  private static Optional<ByteArray> readBytes(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case BYTES:
        return Optional.ofNullable((ByteBuffer) record.get(fieldName)).map(ByteArray::copyFrom);
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(ByteArray::copyFrom);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BYTES");
    }
  }

  private static Optional<Boolean> readBool(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case BOOLEAN:
        return Optional.ofNullable((Boolean) record.get(fieldName));
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Boolean::parseBoolean);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BOOL");
    }
  }

  private static Optional<Double> readFloat64(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case INT:
        return Optional.ofNullable((Integer) record.get(fieldName)).map(x -> (double) x);
      case LONG:
        return Optional.ofNullable((Long) record.get(fieldName)).map(x -> (double) x);
      case FLOAT:
        return Optional.ofNullable((Float) record.get(fieldName)).map(x -> (double) x);
      case DOUBLE:
        return Optional.ofNullable((Double) record.get(fieldName));
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Double::valueOf);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as FLOAT64");
    }
  }

  private static Optional<Long> readInt64(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case INT:
        return Optional.ofNullable((Integer) record.get(fieldName)).map(x -> (long) x);
      case LONG:
        return Optional.ofNullable((Long) record.get(fieldName));
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Long::valueOf);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as INT64");
    }
  }

  private static Optional<String> readString(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case INT:
        return Optional.ofNullable((Integer) record.get(fieldName)).map(String::valueOf);
      case LONG:
        return Optional.ofNullable((Long) record.get(fieldName)).map(String::valueOf);
      case FLOAT:
        return Optional.ofNullable((Float) record.get(fieldName)).map(String::valueOf);
      case DOUBLE:
        return Optional.ofNullable((Double) record.get(fieldName)).map(String::valueOf);
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName)).map(Utf8::toString);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as STRING");
    }
  }
}
