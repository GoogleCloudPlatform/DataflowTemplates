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
      /*
      Postgres JDBC uses binary encoding for most types ref:org.postgresql.jdbc.PgConnection.getSupportedBinaryOids()
      */
      JdbcMappings.builder()
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 8) // -
          .put("BIGSERIAL", ResultSet::getLong, valuePassThrough, 8) // -
          .put(
              "BIT",
              bytesExtractor,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) ((n > 0 ? n : 1)); // bit uses text protocol.
              })
          .put(
              "BIT VARYING",
              bytesExtractor,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int)
                    ((n > 0
                        ? n
                        : 10 * 1024
                            * 1024)); // bit varying without a length specification means unlimited
                // length. ref:
                // https://www.postgresql.org/docs/current/datatype-bit.html
              })
          .put("BOOL", ResultSet::getBoolean, valuePassThrough, 1)
          .put("BOOLEAN", ResultSet::getBoolean, valuePassThrough, 1)
          .put(
              "BYTEA",
              bytesExtractor,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min(length, Integer.MAX_VALUE);
              })
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // CHAR(N) -> N * 4 bytes (UTF-8 max) + overhead.
                return (int) Math.min(((n > 0 ? n : 255) * 4), Integer.MAX_VALUE);
              })
          .put(
              "CHARACTER",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) Math.min(((n > 0 ? n : 255) * 4) + 24, Integer.MAX_VALUE);
              })
          .put(
              "CHARACTER VARYING",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) Math.min(((n > 0 ? n : 255) * 4) + 24, Integer.MAX_VALUE);
              })
          .put(
              "CITEXT",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min((length * 4) + 24, Integer.MAX_VALUE);
              })
          .put("DATE", dateExtractor, dateToAvro, 4)
          .put(
              "DECIMAL",
              ResultSet::getObject,
              numericToAvro,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) (n / 2 + 8);
              })
          .put("DOUBLE PRECISION", ResultSet::getDouble, valuePassThrough, 8)
          .put("FLOAT4", ResultSet::getFloat, valuePassThrough, 4)
          .put("FLOAT8", ResultSet::getDouble, valuePassThrough, 8)
          .put("INT", ResultSet::getInt, valuePassThrough, 4)
          .put("INTEGER", ResultSet::getInt, valuePassThrough, 4)
          .put("INT2", ResultSet::getInt, valuePassThrough, 2)
          .put("INT4", ResultSet::getInt, valuePassThrough, 4)
          .put("INT8", ResultSet::getLong, valuePassThrough, 8)
          .put(
              "JSON",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min((length * 4) + 24, Integer.MAX_VALUE);
              })
          .put(
              "JSONB",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min((length * 4) + 24, Integer.MAX_VALUE);
              })
          .put("MONEY", ResultSet::getDouble, valuePassThrough, 8)
          .put(
              "NUMERIC",
              ResultSet::getObject,
              numericToAvro,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) (n / 2 + 8);
              })
          .put(
              "OID",
              ResultSet::getLong,
              valuePassThrough,
              4) // Usually unsigned int, mapped to long for safety
          .put("REAL", ResultSet::getFloat, valuePassThrough, 4)
          .put("SERIAL", ResultSet::getInt, valuePassThrough, 4)
          .put("SERIAL2", ResultSet::getInt, valuePassThrough, 2)
          .put("SERIAL4", ResultSet::getInt, valuePassThrough, 4)
          .put("SERIAL8", ResultSet::getLong, valuePassThrough, 8)
          .put("SMALLINT", ResultSet::getInt, valuePassThrough, 2)
          .put("SMALLSERIAL", ResultSet::getInt, valuePassThrough, 2)
          .put(
              "TEXT",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min((length * 4) + 24, Integer.MAX_VALUE);
              })
          .put("TIMESTAMP", timestampExtractor, timestampToAvro, 8)
          .put("TIMESTAMPTZ", timestamptzExtractor, timestamptzToAvro, 8)
          .put("TIMESTAMP WITH TIME ZONE", timestamptzExtractor, timestamptzToAvro, 8)
          .put("TIMESTAMP WITHOUT TIME ZONE", timestampExtractor, timestampToAvro, 8)
          .put("UUID", ResultSet::getString, valuePassThrough, 16)
          .put(
              "VARBIT",
              bytesExtractor,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) ((n > 0 ? n : 10 * 1024 * 1024) + 24);
              })
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                return (int) Math.min(((n > 0 ? n : 10 * 1024 * 1024) * 4) + 24, Integer.MAX_VALUE);
              })
          .put(
              "XML",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                long length = n > 0 ? n : 10 * 1024 * 1024;
                return (int) Math.min((length * 4) + 24, Integer.MAX_VALUE);
              })
          .build();

  @Override
  public int estimateColumnSize(
      com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    if (JDBC_MAPPINGS.sizeEstimators().containsKey(typeName)) {
      return JDBC_MAPPINGS.sizeEstimators().get(typeName).apply(sourceColumnType);
    }
    throw new IllegalArgumentException("Unknown column type: " + sourceColumnType);
  }

  private static long getLengthOrPrecision(
      com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
    Long[] mods = sourceColumnType.getMods();
    return (mods != null && mods.length > 0 && mods[0] != null) ? mods[0] : 0;
  }

  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }
}
