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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeStampTz;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeTz;
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
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;

/** PostgreSQL data type mapping to AVRO types. */
public class PostgreSQLJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Calendar UTC_CALENDAR =
      Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));

  private static final DateTimeFormatter TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .appendOffsetId()
          .toFormatter();

  private static final DateTimeFormatter TIMETZ_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .appendOffset("+HH:mm", "+00")
          .toFormatter();

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

  // We cannot use `java.sql.Time` here, as the PostgreSQL time might be '24:00:00'. This makes the
  // java.sql.Time jump a day (from the underlying `java.sql.Date`) and microseconds extraction gets
  // a wrong value. We parse the time from a String into an `OffsetTime` instead.
  private static final ResultSetValueExtractor<OffsetTime> timeExtractor =
      (rs, fieldName) -> {
        String time = rs.getString(fieldName);
        if (time == null) {
          return null;
        }
        return OffsetTime.parse(time + ZoneOffset.UTC, TIME_FORMAT);
      };

  private static final ResultSetValueExtractor<OffsetTime> timetzExtractor =
      (rs, fieldName) -> {
        String timeTz = rs.getString(fieldName);
        if (timeTz == null) {
          return null;
        }
        return OffsetTime.parse(timeTz, TIMETZ_FORMAT);
      };

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

  private static final ResultSetValueMapper<OffsetTime> timeToAvro =
      (value, schema) -> toMicros(value);

  private static final ResultSetValueMapper<OffsetTime> timetzToAvro =
      (value, schema) ->
          new GenericRecordBuilder(TimeTz.SCHEMA)
              .set(
                  TimeTz.TIME_FIELD_NAME,
                  TimeUnit.NANOSECONDS.toMicros(value.toLocalTime().toNanoOfDay()))
              .set(
                  TimeTz.OFFSET_FIELD_NAME,
                  TimeUnit.SECONDS.toMillis(value.getOffset().getTotalSeconds()))
              .build();

  private static final ResultSetValueMapper<OffsetDateTime> timestamptzToAvro =
      (value, schema) ->
          new GenericRecordBuilder(TimeStampTz.SCHEMA)
              .set(TimeStampTz.TIMESTAMP_FIELD_NAME, toMicros(value.toInstant()))
              .set(
                  TimeStampTz.OFFSET_FIELD_NAME,
                  TimeUnit.SECONDS.toMillis(value.getOffset().getTotalSeconds()))
              .build();

  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIGSERIAL", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIT", Pair.of(bytesExtractor, valuePassThrough))
          .put("BIT VARYING", Pair.of(bytesExtractor, valuePassThrough))
          .put("BOOL", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("BOOLEAN", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("BOX", Pair.of(ResultSet::getString, valuePassThrough))
          .put("BYTEA", Pair.of(bytesExtractor, valuePassThrough))
          .put("CHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CHARACTER", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CHARACTER VARYING", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CIDR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CIRCLE", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CITEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(dateExtractor, dateToAvro))
          .put("DECIMAL", Pair.of(ResultSet::getObject, numericToAvro))
          .put("DOUBLE PRECISION", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("ENUM", Pair.of(ResultSet::getString, valuePassThrough))
          .put("FLOAT4", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("FLOAT8", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("INET", Pair.of(ResultSet::getString, valuePassThrough))
          .put("INT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INTEGER", Pair.of(ResultSet::getInt, valuePassThrough))
          // TODO(thiagotnunes): INTERVAL
          .put("INT2", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INT4", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INT8", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("JSON", Pair.of(ResultSet::getString, valuePassThrough))
          .put("JSONB", Pair.of(ResultSet::getString, valuePassThrough))
          .put("LINE", Pair.of(ResultSet::getString, valuePassThrough))
          .put("LSEG", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MACADDR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MACADDR8", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MONEY", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("NUMERIC", Pair.of(ResultSet::getObject, numericToAvro))
          .put("OID", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("PATH", Pair.of(ResultSet::getString, valuePassThrough))
          .put("PG_LSN", Pair.of(ResultSet::getString, valuePassThrough))
          .put("PG_SNAPSHOT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("POINT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("POLYGON", Pair.of(ResultSet::getString, valuePassThrough))
          .put("REAL", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("SERIAL", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SERIAL2", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SERIAL4", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SERIAL8", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("SMALLINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SMALLSERIAL", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIME", Pair.of(timeExtractor, timeToAvro))
          .put("TIMETZ", Pair.of(timetzExtractor, timetzToAvro))
          .put("TIMESTAMP", Pair.of(timestampExtractor, timestampToAvro))
          .put("TIMESTAMPTZ", Pair.of(timestamptzExtractor, timestamptzToAvro))
          .put("TIME WITH TIME ZONE", Pair.of(timetzExtractor, timetzToAvro))
          .put("TIME WITHOUT TIME ZONE", Pair.of(timeExtractor, timeToAvro))
          .put("TIMESTAMP WITH TIME ZONE", Pair.of(timestamptzExtractor, timestamptzToAvro))
          .put("TIMESTAMP WITHOUT TIME ZONE", Pair.of(timestampExtractor, timestampToAvro))
          .put("TSQUERY", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TSVECTOR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TXID_SNAPSHOT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("UUID", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARBIT", Pair.of(bytesExtractor, valuePassThrough))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("XML", Pair.of(ResultSet::getString, valuePassThrough))
          .build()
          .entrySet()
          .stream()
          .map(
              entry ->
                  Map.entry(
                      entry.getKey(),
                      new JdbcValueMapper<>(
                          entry.getValue().getLeft(), entry.getValue().getRight())))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return SCHEMA_MAPPINGS;
  }
}
