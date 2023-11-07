/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.ddl.Column;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.annotations.VisibleForTesting;
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
            String.format(
                "Cannot find corresponding column for field %s in table %s schema %s",
                fieldName, table.prettyPrint(), schema.toString(true)));
      }

      if (column.isGenerated()) {
        // Spanner will compute generated column values automatically.
        continue;
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
        case PG_BOOL:
          builder.set(column.name()).to(readBool(record, avroType, fieldName).orElse(null));
          break;
        case INT64:
        case PG_INT8:
          builder.set(column.name()).to(readInt64(record, avroType, fieldName).orElse(null));
          break;
        case FLOAT64:
        case PG_FLOAT8:
          builder.set(column.name()).to(readFloat64(record, avroType, fieldName).orElse(null));
          break;
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
        case JSON:
        case PG_JSONB:
          builder.set(column.name()).to(readString(record, avroType, fieldName).orElse(null));
          break;
        case BYTES:
        case PG_BYTEA:
          builder.set(column.name()).to(readBytes(record, avroType, fieldName).orElse(null));
          break;
        case TIMESTAMP:
        case PG_TIMESTAMPTZ:
        case PG_SPANNER_COMMIT_TIMESTAMP:
          builder
              .set(column.name())
              .to(readTimestamp(record, avroType, logicalType, fieldName).orElse(null));
          break;
        case DATE:
        case PG_DATE:
          builder
              .set(column.name())
              .to(readDate(record, avroType, logicalType, fieldName).orElse(null));
          break;
        case NUMERIC:
          builder.set(column.name()).to(readNumeric(record, avroType, fieldName).orElse(null));
          break;
        case PG_NUMERIC:
          builder
              .set(column.name())
              .to(Value.pgNumeric(readPgNumeric(record, avroType, fieldName).orElse(null)));
          break;
        case ARRAY:
        case PG_ARRAY:
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
              case PG_BOOL:
                builder
                    .set(column.name())
                    .toBoolArray(readBoolArray(record, arrayType, fieldName).orElse(null));
                break;
              case INT64:
              case PG_INT8:
                builder
                    .set(column.name())
                    .toInt64Array(readInt64Array(record, arrayType, fieldName).orElse(null));
                break;
              case FLOAT64:
              case PG_FLOAT8:
                builder
                    .set(column.name())
                    .toFloat64Array(readFloat64Array(record, arrayType, fieldName).orElse(null));
                break;
              case STRING:
              case PG_VARCHAR:
              case PG_TEXT:
              case JSON:
                builder
                    .set(column.name())
                    .toStringArray(readStringArray(record, arrayType, fieldName).orElse(null));
                break;
              case PG_JSONB:
                builder
                    .set(column.name())
                    .toPgJsonbArray(readStringArray(record, arrayType, fieldName).orElse(null));
                break;
              case BYTES:
              case PG_BYTEA:
                builder
                    .set(column.name())
                    .toBytesArray(readBytesArray(record, arrayType, fieldName).orElse(null));
                break;
              case TIMESTAMP:
              case PG_TIMESTAMPTZ:
              case PG_SPANNER_COMMIT_TIMESTAMP:
                builder
                    .set(column.name())
                    .toTimestampArray(
                        readTimestampArray(record, arrayType, arrayLogicalType, fieldName)
                            .orElse(null));
                break;
              case DATE:
              case PG_DATE:
                builder
                    .set(column.name())
                    .toDateArray(readDateArray(record, arrayType, fieldName).orElse(null));
                break;
              case NUMERIC:
                builder
                    .set(column.name())
                    .toStringArray(readNumericArray(record, arrayType, fieldName).orElse(null));
                break;
              case PG_NUMERIC:
                builder
                    .set(column.name())
                    .toPgNumericArray(
                        readPgNumericArray(record, arrayType, fieldName).orElse(null));
                break;
              default:
                throw new IllegalArgumentException(
                    String.format(
                        "Cannot convert field %s in schema %s table %s",
                        fieldName, schema.toString(true), table.prettyPrint()));
            }
            break;
          }
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Cannot convert field %s in schema %s table %s",
                  fieldName, schema.toString(true), table.prettyPrint()));
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
              value.stream()
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
              value.stream()
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
              value.stream()
                  .map(x -> x == null ? null : Date.parseDate(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as DATE");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<String>> readNumericArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    Object fieldValue = record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
      case BYTES:
        List<ByteBuffer> values = (List<ByteBuffer>) record.get(fieldName);
        return Optional.of(
            values.stream()
                .map(x -> x == null ? null : NumericUtils.bytesToString(x.array()))
                .collect(Collectors.toList()));
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BYTES");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<String>> readPgNumericArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    Object fieldValue = record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
      case BYTES:
        List<ByteBuffer> values = (List<ByteBuffer>) record.get(fieldName);
        return Optional.of(
            values.stream()
                .map(x -> x == null ? null : NumericUtils.pgBytesToString(x.array()))
                .collect(Collectors.toList()));
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as NUMERIC");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<Timestamp>> readTimestampArray(
      GenericRecord record, Schema.Type avroType, LogicalType logicalType, String fieldName) {
    Object fieldValue = record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
      case LONG:
        {
          List<Long> value = (List<Long>) fieldValue;
          // Default to microseconds
          if (logicalType == null || LogicalTypes.timestampMicros().equals(logicalType)) {
            return Optional.of(
                value.stream()
                    .map(x -> x == null ? null : Timestamp.ofTimeMicroseconds(x))
                    .collect(Collectors.toList()));
          } else if (LogicalTypes.timestampMillis().equals(logicalType)) {
            return Optional.of(
                value.stream()
                    .map(x -> x == null ? null : Timestamp.ofTimeMicroseconds(1000L * x))
                    .collect(Collectors.toList()));
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot interpret Avrotype LONG LogicalType %s as TIMESTAMP", logicalType));
          }
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) fieldValue;
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Timestamp.parseTimestamp(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as TIMESTAMP");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<String>> readStringArray(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    List<Object> fieldValue = (List<Object>) record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case LONG:
      case INT:
      case STRING:
        // This relies on the .toString() method present in all classes.
        // It is not necessary to know the exact type of x for that.
        return Optional.of(
            fieldValue.stream()
                .map(x -> x == null ? null : String.valueOf(x))
                .collect(Collectors.toList()));
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as STRING");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<Double>> readFloat64Array(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    Object fieldValue = record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
        // For type check at compile time, the type of x has to be specified (as cast) so that
        // convertability to double can be verified.
      case DOUBLE:
        return Optional.of((List<Double>) fieldValue);
      case FLOAT:
        {
          List<Float> value = (List<Float>) fieldValue;
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Double.valueOf(x))
                  .collect(Collectors.toList()));
        }
      case INT:
        {
          List<Integer> value = (List<Integer>) fieldValue;
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Double.valueOf(x))
                  .collect(Collectors.toList()));
        }
      case LONG:
        {
          List<Long> value = (List<Long>) record.get(fieldName);
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Double.valueOf(x))
                  .collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) record.get(fieldName);
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Double.valueOf(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as FLOAT64");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<Long>> readInt64Array(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    Object fieldValue = record.get(fieldName);
    if (fieldValue == null) {
      return Optional.empty();
    }
    switch (avroType) {
        // For type check at compile time, the type of x has to be specified (as cast) so that
        // convertability to long can be verified.
      case LONG:
        return Optional.of((List<Long>) fieldValue);
      case INT:
        {
          List<Integer> value = (List<Integer>) fieldValue;
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Long.valueOf(x))
                  .collect(Collectors.toList()));
        }
      case STRING:
        {
          List<Utf8> value = (List<Utf8>) fieldValue;
          return Optional.of(
              value.stream()
                  .map(x -> x == null ? null : Long.valueOf(x.toString()))
                  .collect(Collectors.toList()));
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as INT64");
    }
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Optional<List<Boolean>> readBoolArray(
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
              value.stream()
                  .map(x -> x == null ? null : Boolean.valueOf(x.toString()))
                  .collect(Collectors.toList());
          return Optional.of(result);
        }
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BOOL");
    }
  }

  private Optional<Date> readDate(
      GenericRecord record, Schema.Type avroType, LogicalType logicalType, String fieldName) {
    switch (avroType) {
      case INT:
        if (logicalType == null || !LogicalTypes.date().equals(logicalType)) {
          throw new IllegalArgumentException(
              "Cannot interpret Avrotype INT Logicaltype " + logicalType + " as DATE");
        }
        // Avro Date is number of days since Jan 1, 1970.
        // Have to convert to Java Date first before creating google.cloud.core.Date
        return Optional.ofNullable((Integer) record.get(fieldName))
            .map(x -> new java.util.Date((long) x * 24L * 3600L * 1000L))
            .map(Date::fromJavaUtilDate);
      case STRING:
        return Optional.ofNullable((Utf8) record.get(fieldName))
            .map(Utf8::toString)
            .map(Date::parseDate);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as DATE");
    }
  }

  private Optional<String> readNumeric(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case BYTES:
        return Optional.ofNullable((ByteBuffer) record.get(fieldName))
            .map(ByteBuffer::array)
            .map(NumericUtils::bytesToString);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as BYTES");
    }
  }

  static Optional<String> readPgNumeric(
      GenericRecord record, Schema.Type avroType, String fieldName) {
    switch (avroType) {
      case BYTES:
        return Optional.ofNullable((ByteBuffer) record.get(fieldName))
            .map(ByteBuffer::array)
            .map(NumericUtils::pgBytesToString);
      default:
        throw new IllegalArgumentException("Cannot interpret " + avroType + " as NUMERIC");
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
