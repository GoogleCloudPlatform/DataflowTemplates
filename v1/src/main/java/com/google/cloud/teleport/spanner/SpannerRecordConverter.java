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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.common.base.Strings;
import java.nio.ByteBuffer;
import java.text.DecimalFormatSymbols;
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
  private final List<FieldInfo> fields;
  private static final char ZERO_DIGIT = (new DecimalFormatSymbols()).getZeroDigit();

  private static class FieldInfo {
    private final Schema.Field field;
    private final boolean generated;
    private final String name;
    private final String spannerType;
    private final Schema type;
    private final boolean supportedType;
    private final boolean nullable;

    private final boolean matchesString;
    private final boolean matchesArray;
    private final boolean matchesVarchar;
    private final boolean matchesVarcharArray;

    public FieldInfo(Schema.Field field) {
      this.field = field;
      this.generated = field.getProp("generationExpression") != null;
      this.name = field.name();
      this.spannerType = Strings.nullToEmpty(field.getProp(SQL_TYPE));

      this.matchesString = STRING_PATTERN.matcher(this.spannerType).matches();
      this.matchesArray = ARRAY_PATTERN.matcher(this.spannerType).matches();
      this.matchesVarchar = VARCHAR_PATTERN.matcher(this.spannerType).matches();
      this.matchesVarcharArray = VARCHAR_ARRAY_PATTERN.matcher(this.spannerType).matches();

      Schema fieldType = field.schema();
      if (fieldType.getType() == Schema.Type.UNION) {
        Schema unpack = AvroUtil.unpackNullable(field.schema());
        if (unpack == null) {
          this.type = fieldType;
          this.supportedType = false;
          this.nullable = false;
        } else {
          this.type = unpack;
          this.supportedType = true;
          this.nullable = true;
        }
      } else {
        this.type = fieldType;
        this.supportedType = true;
        this.nullable = false;
      }
    }

    public void checkSupported() {
      if (!supportedType) {
        throw new IllegalArgumentException("Unsupported type" + type);
      }
    }

    public Schema.Field getField() {
      return field;
    }

    public boolean isGenerated() {
      return generated;
    }

    public String getName() {
      return name;
    }

    public Schema getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }

    public String getSpannerType() {
      return spannerType;
    }

    public boolean matchesStringPattern() {
      return matchesString;
    }

    public boolean matchesArrayPattern() {
      return matchesArray;
    }

    public boolean matchesVarcharPattern() {
      return matchesVarchar;
    }

    public boolean matchesVarcharArrayPattern() {
      return matchesVarcharArray;
    }
  }

  public SpannerRecordConverter(Schema schema, Dialect dialect) {
    this.schema = schema;
    this.dialect = dialect;

    this.fields = processFields();
  }

  public SpannerRecordConverter(Schema schema) {
    this(schema, Dialect.GOOGLE_STANDARD_SQL);
  }

  private List<FieldInfo> processFields() {
    return schema.getFields().stream().map(FieldInfo::new).collect(Collectors.toList());
  }

  public GenericRecord convert(Struct row) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (FieldInfo fieldInfo : this.fields) {
      if (fieldInfo.isGenerated()) {
        // Generated column values are not exported.
        continue;
      }

      fieldInfo.checkSupported();

      Schema.Field field = fieldInfo.getField();
      String fieldName = fieldInfo.getName();
      Schema type = fieldInfo.getType();
      String spannerType = fieldInfo.getSpannerType();

      int fieldIndex = row.getColumnIndex(fieldName);

      boolean nullValue = row.isNull(fieldIndex);
      if (nullValue && !fieldInfo.isNullable()) {
        throw new IllegalArgumentException("Unexpected null value for field " + fieldName);
      }
      switch (type.getType()) {
        case BOOLEAN:
          builder.set(field, nullValue ? null : row.getBoolean(fieldIndex));
          break;
        case LONG:
          if ((dialect == Dialect.GOOGLE_STANDARD_SQL && spannerType.equals("TIMESTAMP"))
              || (dialect == Dialect.POSTGRESQL
                  && (spannerType.equals("timestamp with time zone")
                      || spannerType.equals("spanner.commit_timestamp")))) {
            long microSeconds = 0L;
            if (!nullValue) {
              Timestamp ts = row.getTimestamp(fieldIndex);
              microSeconds =
                  TimeUnit.SECONDS.toMicros(ts.getSeconds())
                      + TimeUnit.NANOSECONDS.toMicros(ts.getNanos());
            }
            builder.set(field, nullValue ? null : microSeconds);
          } else {
            builder.set(field, nullValue ? null : row.getLong(fieldIndex));
          }
          break;
        case DOUBLE:
          builder.set(field, nullValue ? null : row.getDouble(fieldIndex));
          break;
        case BYTES:
          if (dialect == Dialect.GOOGLE_STANDARD_SQL && spannerType.equals("NUMERIC")) {
            // TODO: uses row.getNumeric() once teleport uses new spanner library.
            builder.set(
                field,
                nullValue
                    ? null
                    : ByteBuffer.wrap(
                        NumericUtils.stringToBytes(row.getBigDecimal(fieldIndex).toString())));
            break;
          }
          if (dialect == Dialect.POSTGRESQL && spannerType.equals("numeric")) {
            builder.set(
                field,
                nullValue
                    ? null
                    : ByteBuffer.wrap(NumericUtils.pgStringToBytes(row.getString(fieldIndex))));
            break;
          }
          builder.set(
              field, nullValue ? null : ByteBuffer.wrap(row.getBytes(fieldIndex).toByteArray()));
          break;
        case STRING:
          if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
            if (fieldInfo.matchesStringPattern() || spannerType.equals("JSON")) {
              builder.set(field, nullValue ? null : row.getString(fieldIndex));
            } else if (spannerType.equals("TIMESTAMP")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldIndex).toString());
            } else if (spannerType.equals("DATE")) {
              builder.set(field, nullValue ? null : dateToString(row.getDate(fieldIndex)));
            }
          } else if (dialect == Dialect.POSTGRESQL) {
            if (spannerType.equals("jsonb")) {
              builder.set(field, nullValue ? null : row.getPgJsonb(fieldIndex));
            } else if (fieldInfo.matchesVarcharPattern() || spannerType.equals("text")) {
              builder.set(field, nullValue ? null : row.getString(fieldIndex));
            } else if (spannerType.equals("timestamp with time zone")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldIndex).toString());
            } else if (spannerType.equals("spanner.commit_timestamp")) {
              builder.set(field, nullValue ? null : row.getTimestamp(fieldIndex).toString());
            } else if (spannerType.equals("date")) {
              builder.set(field, nullValue ? null : dateToString(row.getDate(fieldIndex)));
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
                builder.set(field, nullValue ? null : row.getBooleanList(fieldIndex));
                break;
              case LONG:
                if ((dialect == Dialect.GOOGLE_STANDARD_SQL
                        && spannerType.equals("ARRAY<TIMESTAMP>"))
                    || (dialect == Dialect.POSTGRESQL
                        && spannerType.equals("timestamp with time zone[]"))) {
                  List<Long> values =
                      row.getTimestampList(fieldIndex).stream()
                          .map(
                              timestamp ->
                                  timestamp == null
                                      ? null
                                      : (TimeUnit.SECONDS.toMicros(timestamp.getSeconds())
                                          + TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos())))
                          .collect(Collectors.toList());
                  builder.set(field, nullValue ? null : values);
                } else {
                  builder.set(field, nullValue ? null : row.getLongList(fieldIndex));
                }
                break;
              case DOUBLE:
                {
                  builder.set(field, nullValue ? null : row.getDoubleList(fieldIndex));
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
                        row.getStringList(fieldIndex).stream()
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
                        row.getStringList(fieldIndex).stream()
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
                        row.getBytesList(fieldIndex).stream()
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
                    if (fieldInfo.matchesArrayPattern() || spannerType.equals("ARRAY<JSON>")) {
                      builder.set(field, nullValue ? null : row.getStringList(fieldIndex));
                    } else if (spannerType.equals("ARRAY<TIMESTAMP>")) {
                      setTimestampArray(row, builder, field, fieldIndex, nullValue);
                    } else if (spannerType.equals("ARRAY<DATE>")) {
                      setDateArray(row, builder, field, fieldIndex, nullValue);
                    }
                  }
                  if (dialect == Dialect.POSTGRESQL) {
                    if (spannerType.equals("jsonb[]")) {
                      builder.set(field, nullValue ? null : row.getPgJsonbList(fieldIndex));
                    } else if (fieldInfo.matchesVarcharArrayPattern()
                        || spannerType.equals("text[]")) {
                      builder.set(field, nullValue ? null : row.getStringList(fieldIndex));
                    } else if (spannerType.equals("timestamp with time zone[]")) {
                      setTimestampArray(row, builder, field, fieldIndex, nullValue);
                    } else if (spannerType.equals("date[]")) {
                      setDateArray(row, builder, field, fieldIndex, nullValue);
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

  // Package scope to be accessible to tests.
  static String dateToString(Date date) {
    StringBuilder b = new StringBuilder(10);
    int year = date.getYear();
    if (year < 1000) {
      b.append(ZERO_DIGIT);
      if (year < 100) {
        b.append(ZERO_DIGIT);
        if (year < 10) {
          b.append(ZERO_DIGIT);
        }
      }
    }
    b.append(year);

    b.append('-');

    int month = date.getMonth();
    if (month < 10) {
      b.append(ZERO_DIGIT);
    }
    b.append(month);

    b.append('-');

    int dayOfMonth = date.getDayOfMonth();
    if (dayOfMonth < 10) {
      b.append(ZERO_DIGIT);
    }
    return b.append(dayOfMonth).toString();
  }

  private static void setTimestampArray(
      Struct row,
      GenericRecordBuilder builder,
      Schema.Field field,
      int fieldIndex,
      boolean nullValue) {
    if (nullValue) {
      builder.set(field, null);
    } else {
      List<String> values =
          row.getTimestampList(fieldIndex).stream()
              .map(timestamp -> timestamp == null ? null : timestamp.toString())
              .collect(Collectors.toList());
      builder.set(field, values);
    }
  }

  private static void setDateArray(
      Struct row,
      GenericRecordBuilder builder,
      Schema.Field field,
      int fieldIndex,
      boolean nullValue) {
    if (nullValue) {
      builder.set(field, null);
    } else {
      List<String> values =
          row.getDateList(fieldIndex).stream()
              .map(date -> date == null ? null : dateToString(date))
              .collect(Collectors.toList());
      builder.set(field, values);
    }
  }
}
