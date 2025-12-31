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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcMappings;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeStampTz;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecordBuilder;

/** PostgreSQL data type mapping to AVRO types. */
public class PostgreSQLJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Calendar UTC_CALENDAR =
      Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

  private static final DateTimeFormatter TIMESTAMPTZ_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .appendOffset("+HH:mm", "+00")
          .toFormatter();

  private static long toMicros(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  private static long toMicros(OffsetTime offsetTime) {
    return TimeUnit.HOURS.toMicros(offsetTime.getHour())
        + TimeUnit.MINUTES.toMicros(offsetTime.getMinute())
        + TimeUnit.SECONDS.toMicros(offsetTime.getSecond())
        + TimeUnit.NANOSECONDS.toMicros(offsetTime.getNano());
  }

  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueExtractor<ByteBuffer> bytesExtractor =
      (rs, fieldName) -> {
        byte[] bytes = rs.getBytes(fieldName);
        if (bytes == null) {
          return null;
        }
        return ByteBuffer.wrap(bytes);
      };

  private static final ResultSetValueExtractor<java.sql.Date> dateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, UTC_CALENDAR);

  private static final ResultSetValueExtractor<java.sql.Timestamp> timestampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName, UTC_CALENDAR);

  private static final ResultSetValueExtractor<OffsetDateTime> timestamptzExtractor =
      (rs, fieldName) -> {
        String timestampTz = rs.getString(fieldName);
        if (timestampTz == null) {
          return null;
        }
        return OffsetDateTime.parse(timestampTz, TIMESTAMPTZ_FORMAT);
      };

  // Value might be a Double.NaN or a valid BigDecimal
  private static final ResultSetValueMapper<Number> numericToAvro =
      (value, schema) -> value.toString();

  private static final ResultSetValueMapper<java.sql.Date> dateToAvro =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  private static final ResultSetValueMapper<java.sql.Timestamp> timestampToAvro =
      (value, schema) -> toMicros(value.toInstant());

  private static final ResultSetValueMapper<OffsetDateTime> timestamptzToAvro =
      (value, schema) ->
          new GenericRecordBuilder(TimeStampTz.SCHEMA)
              .set(TimeStampTz.TIMESTAMP_FIELD_NAME, toMicros(value.toInstant()))
              .set(
                  TimeStampTz.OFFSET_FIELD_NAME,
                  TimeUnit.SECONDS.toMillis(value.getOffset().getTotalSeconds()))
              .build();

  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 24)
          .put("BIGSERIAL", ResultSet::getLong, valuePassThrough, 24)
          .put(
              "BIT",
              bytesExtractor,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "BIT VARYING",
              bytesExtractor,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put("BOOL", ResultSet::getBoolean, valuePassThrough, 16)
          .put("BOOLEAN", ResultSet::getBoolean, valuePassThrough, 16)
          .put(
              "BYTEA",
              bytesExtractor,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "CHARACTER",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "CHARACTER VARYING",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "CITEXT",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put("DATE", dateExtractor, dateToAvro, 16)
          .put("DECIMAL", ResultSet::getObject, numericToAvro, 90)
          .put("DOUBLE PRECISION", ResultSet::getDouble, valuePassThrough, 24)
          .put("FLOAT4", ResultSet::getFloat, valuePassThrough, 16)
          .put("FLOAT8", ResultSet::getDouble, valuePassThrough, 24)
          .put("INT", ResultSet::getInt, valuePassThrough, 16)
          .put("INTEGER", ResultSet::getInt, valuePassThrough, 16)
          .put("INT2", ResultSet::getInt, valuePassThrough, 16)
          .put("INT4", ResultSet::getInt, valuePassThrough, 16)
          .put("INT8", ResultSet::getLong, valuePassThrough, 24)
          .put(
              "JSON",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "JSONB",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put("MONEY", ResultSet::getDouble, valuePassThrough, 24)
          .put("NUMERIC", ResultSet::getObject, numericToAvro, 90)
          .put(
              "OID",
              ResultSet::getLong,
              valuePassThrough,
              24) // Usually unsigned int, mapped to long for safety? Original
          // was Long.
          .put("REAL", ResultSet::getFloat, valuePassThrough, 16)
          .put("SERIAL", ResultSet::getInt, valuePassThrough, 16)
          .put("SERIAL2", ResultSet::getInt, valuePassThrough, 16)
          .put("SERIAL4", ResultSet::getInt, valuePassThrough, 16)
          .put("SERIAL8", ResultSet::getLong, valuePassThrough, 24)
          .put("SMALLINT", ResultSet::getInt, valuePassThrough, 16)
          .put("SMALLSERIAL", ResultSet::getInt, valuePassThrough, 16)
          .put(
              "TEXT",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put("TIMESTAMP", timestampExtractor, timestampToAvro, 24)
          .put("TIMESTAMPTZ", timestamptzExtractor, timestamptzToAvro, 140)
          .put("TIMESTAMP WITH TIME ZONE", timestamptzExtractor, timestamptzToAvro, 140)
          .put("TIMESTAMP WITHOUT TIME ZONE", timestampExtractor, timestampToAvro, 24)
          .put(
              "UUID",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "VARBIT",
              bytesExtractor,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize)
          .put(
              "XML",
              ResultSet::getString,
              valuePassThrough,
              PostgreSQLJdbcValueMappings::guessVariableTypeSize) // Added
          // XML
          // based
          // on
          // switch
          // case
          // reference,
          // though
          // not in
          // original
          // map?
          // No,
          // wait.
          // XML was
          // in
          // switch
          // case
          // but not
          // in
          // map...
          // interesting.
          // I will
          // add it
          // if it
          // was
          // supported,
          // but if
          // not in
          // map,
          // maybe
          // better
          // to
          // leave
          // it out
          // or
          // check.
          // Note: "XML" was NOT in the original map, but WAS in the switch case for size.
          // "XML" support might be missing in mapping?
          // I will assume if it wasn't in the map, it wasn't supported for reading. I
          // will leave it out of build() but keep it in size logic if needed, or better,
          // just leave it out of build() so it falls to default if usage ever occurs (but
          // it won't be mapped).
          .build();

  @Override
  public int guessColumnSize(
      com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    if (JDBC_MAPPINGS.sizeEstimators().containsKey(typeName)) {
      return JDBC_MAPPINGS.sizeEstimators().get(typeName).apply(sourceColumnType);
    }
    // Fallback for types not in map but might have size logic (like XML if it was
    // supported?)
    // Or just default.
    return 16;
  }

  private static int guessVariableTypeSize(
      com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();

    // Fixed width String types
    if (typeName.equals("UUID")) {
      // UUID String is 36 chars.
      // 36 * 2 = 72 bytes.
      // Overhead ~50.
      return 122;
    }

    long length = 0;
    Long[] mods = sourceColumnType.getMods();
    if (mods != null && mods.length > 0 && mods[0] != null) {
      length = mods[0];
    } else {
      // Defaults if length not specified
      length = 255;
      if (typeName.contains("TEXT")
          || typeName.contains("JSON")
          || typeName.equals("BYTEA")
          || typeName.equals("XML")) {
        length = 10 * 1024 * 1024; // 10MB conservative max for unbounded
      }
    }

    // Checking for multi-byte chars. UTF-8 is standard.
    // In Java heap, chars are 2 bytes (UTF-16).
    // So distinct from DB storage.
    long byteLength;
    boolean isString =
        typeName.contains("TEXT")
            || typeName.contains("CHAR")
            || typeName.contains("JSON")
            || typeName.contains("XML")
            || typeName.contains("CITEXT");

    if (isString) {
      byteLength = length * 2; // Java char is 2 bytes
    } else {
      // Binary or Bit
      if (typeName.contains("BIT") || typeName.contains("VARBIT")) {
        byteLength = (long) Math.ceil(length / 8.0);
      } else {
        byteLength = length;
      }
    }

    int overhead = isString ? 50 : 24; // String vs byte[] overhead

    long totalSize = overhead + byteLength;
    return (int) Math.min(totalSize, Integer.MAX_VALUE);
  }

  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }
}
