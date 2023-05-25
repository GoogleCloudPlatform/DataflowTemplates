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

import static com.google.cloud.teleport.spanner.AvroUtil.SQL_TYPE;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.common.base.Strings;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** Converts {@link Struct} to Avro record of specified {@link Schema}. */
public class SpannerRecordConverter {
  private static final Pattern STRING_PATTERN = Pattern.compile("STRING\\((?:MAX|[0-9]+)\\)");
  private static final Pattern ARRAY_PATTERN = Pattern.compile("ARRAY<STRING\\((?:MAX|[0-9]+)\\)>");
  private static final Pattern VARCHAR_PATTERN =
      Pattern.compile("character varying(\\([0-9]+\\))?");
  private static final Pattern VARCHAR_ARRAY_PATTERN =
      Pattern.compile("character varying(\\([0-9]+\\))?\\[\\]");
  private final Schema schema;
  private final Dialect dialect;

  public SpannerRecordConverter(Schema schema, Dialect dialect) {
    this.schema = schema;
    this.dialect = dialect;
  }

  public SpannerRecordConverter(Schema schema) {
    this.schema = schema;
    this.dialect = Dialect.GOOGLE_STANDARD_SQL;
  }

  public GenericRecord convert(Struct row) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      if (field.getProp("generationExpression") != null) {
        // Generated column values are not exported.
        continue;
      }
      String fieldName = field.name();
      Schema type = field.schema();
      // Empty string to avoid null checks.
      String spannerType = Strings.nullToEmpty(field.getProp(SQL_TYPE));
      boolean nullable = false;
      if (type.getType() == Schema.Type.UNION) {
        Schema unpack = AvroUtil.unpackNullable(field.schema());
        if (unpack == null) {
          throw new IllegalArgumentException("Unsupported type" + type);
        }
        type = unpack;
        nullable = true;
      }

      boolean nullValue = row.isNull(fieldName);
      if (nullValue && !nullable) {
        throw new IllegalArgumentException("Unexpected null value for field " + fieldName);
      }
      switch (type.getType()) {
        case BOOLEAN:
          builder.set(field, nullValue ? null : row.getBoolean(fieldName));
          break;
        case LONG:
          if ((dialect == Dialect.GOOGLE_STANDARD_SQL && spannerType.equals("TIMESTAMP"))
              || (dialect == Dialect.POSTGRESQL
                  && (spannerType.equals("timestamp with time zone")
                      || spannerType.equals("spanner.commit_timestamp")))) {
            long microSeconds = 0L;
            if (!nullValue) {
              Timestamp ts = row.getTimestamp(fieldName);
              microSeconds =
                  TimeUnit.SECONDS.toMicros(ts.getSeconds())
                      + TimeUnit.NANOSECONDS.toMicros(ts.getNanos());
            }
            builder.set(field, nullValue ? null : microSeconds);
          } else {
            builder.set(field, nullValue ? null : row.getLong(fieldName));
          }
          break;
        case DOUBLE:
          builder.set(field, nullValue ? null : row.getDouble(fieldName));
          break;
        case BYTES:
          if (dialect == Dialect.GOOGLE_STANDARD_SQL && spannerType.equals("NUMERIC")) {
            // TODO: uses row.getNumeric() once teleport uses new spanner library.
            builder.set(
                field,
                nullValue
                    ? null
                    : ByteBuffer.wrap(
                        NumericUtils.stringToBytes(row.getBigDecimal(fieldName).toString())));
            break;
          }
          if (dialect == Dialect.POSTGRESQL && spannerType.equals("numeric")) {
            builder.set(
                field,
                nullValue
                    ? null
                    : ByteBuffer.wrap(NumericUtils.pgStringToBytes(row.getString(fieldName))));
            break;
          }
          builder.set(
              field, nullValue ? null : ByteBuffer.wrap(row.getBytes(fieldName).toByteArray()));
          break;
        case STRING:
          if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
            if (STRING_PATTERN.matcher(spannerType).matches() || spannerType.equals("JSON")) {
              builder.set(field, nullValue ? null : row.getString(fieldName));
            } else if (spannerType.equals("TIMESTAMP")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldName).toString());
            } else if (spannerType.equals("DATE")) {
              builder.set(field, nullValue ? null : row.getDate(fieldName).toString());
            }
          } else if (dialect == Dialect.POSTGRESQL) {
            if (spannerType.equals("jsonb")) {
              builder.set(field, nullValue ? null : row.getPgJsonb(fieldName));
            } else if (VARCHAR_PATTERN.matcher(spannerType).matches()
                || spannerType.equals("text")) {
              builder.set(field, nullValue ? null : row.getString(fieldName));
            } else if (spannerType.equals("timestamp with time zone")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldName).toString());
            } else if (spannerType.equals("spanner.commit_timestamp")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldName).toString());
            } else if (spannerType.equals("date")) {
              builder.set(field, nullValue ? null : row.getDate(fieldName).toString());
            }
          }
          break;
        case ARRAY:
          {
            Schema arrayType = type.getElementType();
            boolean arrayElementNullable = arrayType.getType() == Schema.Type.UNION;
            if (!arrayElementNullable) {
              throw new IllegalArgumentException(
                  "Unsupported type for field "
                      + fieldName
                      + ". Cloud Spanner only supports nullable array values");
            }
            arrayType = AvroUtil.unpackNullable(arrayType);
            if (arrayType == null) {
              throw new IllegalArgumentException("Unsupported type for field " + fieldName);
            }
            switch (arrayType.getType()) {
              case BOOLEAN:
                builder.set(field, nullValue ? null : row.getBooleanList(fieldName));
                break;
              case LONG:
                if ((dialect == Dialect.GOOGLE_STANDARD_SQL
                        && spannerType.equals("ARRAY<TIMESTAMP>"))
                    || (dialect == Dialect.POSTGRESQL
                        && spannerType.equals("timestamp with time zone[]"))) {
                  List<Long> values =
                      row.getTimestampList(fieldName).stream()
                          .map(
                              timestamp ->
                                  timestamp == null
                                      ? null
                                      : (TimeUnit.SECONDS.toMicros(timestamp.getSeconds())
                                          + TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos())))
                          .collect(Collectors.toList());
                  builder.set(field, nullValue ? null : values);
                } else {
                  builder.set(field, nullValue ? null : row.getLongList(fieldName));
                }
                break;
              case DOUBLE:
                {
                  builder.set(field, nullValue ? null : row.getDoubleList(fieldName));
                  break;
                }
              case BYTES:
                {
                  if (dialect == Dialect.GOOGLE_STANDARD_SQL
                      && spannerType.equals("ARRAY<NUMERIC>")) {
                    if (nullValue) {
                      builder.set(field, null);
                      break;
                    }
                    List<ByteBuffer> numericValues = null;
                    numericValues =
                        row.getStringList(fieldName).stream()
                            .map(
                                numeric ->
                                    numeric == null
                                        ? null
                                        : ByteBuffer.wrap(NumericUtils.stringToBytes(numeric)))
                            .collect(Collectors.toList());
                    builder.set(field, numericValues);
                    break;
                  }
                  if (dialect == Dialect.POSTGRESQL && spannerType.equals("numeric[]")) {
                    if (nullValue) {
                      builder.set(field, null);
                      break;
                    }
                    List<ByteBuffer> numericValues = null;
                    numericValues =
                        row.getStringList(fieldName).stream()
                            .map(
                                numeric ->
                                    numeric == null
                                        ? null
                                        : ByteBuffer.wrap(NumericUtils.pgStringToBytes(numeric)))
                            .collect(Collectors.toList());
                    builder.set(field, numericValues);
                    break;
                  }
                  List<ByteBuffer> value = null;
                  if (!nullValue) {
                    value =
                        row.getBytesList(fieldName).stream()
                            .map(
                                bytes ->
                                    bytes == null ? null : ByteBuffer.wrap(bytes.toByteArray()))
                            .collect(Collectors.toList());
                  }
                  builder.set(field, value);
                  break;
                }
              case STRING:
                {
                  if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
                    if (ARRAY_PATTERN.matcher(spannerType).matches()
                        || spannerType.equals("ARRAY<JSON>")) {
                      builder.set(field, nullValue ? null : row.getStringList(fieldName));
                    } else if (spannerType.equals("ARRAY<TIMESTAMP>")) {
                      setTimestampArray(row, builder, field, fieldName, nullValue);
                    } else if (spannerType.equals("ARRAY<DATE>")) {
                      setDateArray(row, builder, field, fieldName, nullValue);
                    }
                  }
                  if (dialect == Dialect.POSTGRESQL) {
                    if (spannerType.equals("jsonb[]")) {
                      builder.set(field, nullValue ? null : row.getPgJsonbList(fieldName));
                    } else if (VARCHAR_ARRAY_PATTERN.matcher(spannerType).matches()
                        || spannerType.equals("text[]")) {
                      builder.set(field, nullValue ? null : row.getStringList(fieldName));
                    } else if (spannerType.equals("timestamp with time zone[]")) {
                      setTimestampArray(row, builder, field, fieldName, nullValue);
                    } else if (spannerType.equals("date[]")) {
                      setDateArray(row, builder, field, fieldName, nullValue);
                    }
                  }
                  break;
                }
              default:
                {
                  throw new IllegalArgumentException("Unsupported array type " + arrayType);
                }
            }
            break;
          }
        default:
          {
            throw new IllegalArgumentException("Unsupported type" + type);
          }
      }
    }
    return builder.build();
  }

  private static void setTimestampArray(
      Struct row,
      GenericRecordBuilder builder,
      Schema.Field field,
      String fieldName,
      boolean nullValue) {
    if (nullValue) {
      builder.set(field, null);
    } else {
      List<String> values =
          row.getTimestampList(fieldName).stream()
              .map(timestamp -> timestamp == null ? null : timestamp.toString())
              .collect(Collectors.toList());
      builder.set(field, values);
    }
  }

  private static void setDateArray(
      Struct row,
      GenericRecordBuilder builder,
      Schema.Field field,
      String fieldName,
      boolean nullValue) {
    if (nullValue) {
      builder.set(field, null);
    } else {
      List<String> values =
          row.getDateList(fieldName).stream()
              .map(date -> date == null ? null : date.toString())
              .collect(Collectors.toList());
      builder.set(field, values);
    }
  }
}
