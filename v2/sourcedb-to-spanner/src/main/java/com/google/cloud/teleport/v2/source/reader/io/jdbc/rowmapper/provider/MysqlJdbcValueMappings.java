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
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.TimeIntervalMicros;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.Interval;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class MysqlJdbcValueMappings implements JdbcValueMappingsProvider {

  /**
   * Pass the value extracted from {@link ResultSet} to {@link GenericRecordBuilder#set(Field,
   * Object)}. Most of the values, like basic types don't need any marshalling and can be directly
   * pass through from jdbc to avro.
   */
  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  /** Map the bytes returned by BIT(N) type to long. MySql Bit(N) has max 64 bits. */
  private static final ResultSetValueMapper<byte[]> bytesToLong =
      (value, schema) -> new BigInteger(value).longValue();

  /** Map BigDecimal type to Byte array. */
  private static final ResultSetValueMapper<BigDecimal> bigDecimalToByteArray =
      (value, schema) ->
          ByteBuffer.wrap(
              value
                  .setScale((int) schema.getObjectProp("scale"), RoundingMode.HALF_DOWN)
                  .unscaledValue()
                  .toByteArray());

  /** Map BigInt Unsigned type to Avro Number. */
  private static final ResultSetValueMapper<BigDecimal> bigDecimalToAvroNumber =
      (value, schema) -> value.toString();

  /* Hex Encoded string for bytes type. */
  private static final ResultSetValueMapper<byte[]> bytesToHexString =
      (value, schema) -> new String(Hex.encodeHex(value));

  /* Hex Encoded string for blob types. */
  private static final ResultSetValueMapper<Blob> blobToHexString =
      (value, schema) -> new String(Hex.encodeHex(value.getBytes(1, (int) value.length())));

  /* Extract UTC Values for date and time related types */
  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  private static final ResultSetValueExtractor<java.sql.Date> utcDateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, utcCalendar);
  private static final ResultSetValueExtractor<java.sql.Timestamp> utcTimeStampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName, utcCalendar);

  /* Map Date and Time related types to Avro */
  /**
   * Map {@link java.sql.Date} to <a href=https://avro.apache.org/docs/1.8.0/spec.html#Date>Date
   * LogicalType</a>.
   */
  private static final ResultSetValueMapper<java.sql.Date> sqlDateToAvroTimestampMicros =
      (value, schema) -> TimeUnit.DAYS.toMicros(value.toLocalDate().toEpochDay());

  /** Map {@link java.sql.Timestamp} to {@link DateTime#SCHEMA} generic record. */
  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroDateTime =
      (value, schema) ->
          new GenericRecordBuilder(DateTime.SCHEMA)
              .set(
                  DateTime.DATE_FIELD_NAME,
                  (int) value.toLocalDateTime().toLocalDate().toEpochDay())
              .set(
                  DateTime.TIME_FIELD_NAME,
                  TimeUnit.NANOSECONDS.toMicros(
                      value.toLocalDateTime().toLocalTime().toNanoOfDay()))
              .build();

  /**
   * Pattern to extract fields form MySQl time output of HH:MM:ss.SSS
   *
   * <p><b>Note:</b>
   *
   * <p>Parsing the output via {@link org.joda.time.Period} causes loss of micro second precision.
   * Java time library does not parse hours to be more than 24.
   */
  static final Pattern TIME_STRING_PATTERN =
      Pattern.compile("^(-)?(\\d+):(\\d+):(\\d+)(\\.(\\d+))?$");

  /**
   * DEPRECATED: Unified type interval is no longer utilized for MySQL. However, this can be reused
   * for Postgres, whenever the support is added. Map Time type to {@link Interval#SCHEMA}. Note
   * Time type records an interval between 2 timestamps, and is independent of timezone.
   */
  private static final ResultSetValueMapper<String> timeStringToAvroInterval =
      (value, schema) -> {
        Matcher matcher = TIME_STRING_PATTERN.matcher(value);
        Preconditions.checkArgument(
            matcher.matches(),
            "The time string " + value + " does not match " + TIME_STRING_PATTERN);

        /* MySQL output is always hours::minutes::seconds.fractionalSeconds */
        boolean isNegative = matcher.group(1) != null;
        int hours = Integer.parseInt(matcher.group(2));
        int minutes = Integer.parseInt(matcher.group(3));
        int seconds = Integer.parseInt(matcher.group(4));
        long nanoSeconds =
            matcher.group(5) == null
                ? 0
                : Long.parseLong(StringUtils.rightPad(matcher.group(6), 9, '0'));
        return new GenericRecordBuilder(Interval.SCHEMA)
            .set(Interval.MONTHS_FIELD_NAME, 0)
            .set(Interval.HOURS_FIELD_NAME, hours * ((isNegative) ? -1 : 1))
            .set(
                Interval.MICROS_FIELD_NAME,
                ((isNegative) ? -1 : 1)
                    * (TimeUnit.MINUTES.toMicros(minutes)
                        + TimeUnit.SECONDS.toMicros(seconds)
                        + TimeUnit.NANOSECONDS.toMicros(nanoSeconds)))
            .build();
      };

  private static long instantToMicro(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  /** Map {@link java.sql.Timestamp} to timestampMicros LogicalType. */
  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> instantToMicro(value.toInstant());

  /**
   * Map Time type to {@link TimeIntervalMicros}. Note Time type records an interval between 2
   * timestamps in microseconds, and is independent of timezone.
   */
  private static final ResultSetValueMapper<String> timeStringToAvroTimeInterval =
      (value, schema) -> {
        /* MySQL output is always hours:minutes:seconds.fractionalSeconds */
        Matcher matcher = TIME_STRING_PATTERN.matcher(value);
        Preconditions.checkArgument(
            matcher.matches(),
            "The time string " + value + " does not match " + TIME_STRING_PATTERN);
        boolean isNegative = matcher.group(1) != null;
        int hours = Integer.parseInt(matcher.group(2));
        int minutes = Integer.parseInt(matcher.group(3));
        int seconds = Integer.parseInt(matcher.group(4));
        long nanoSeconds =
            matcher.group(5) == null
                ? 0
                : Long.parseLong(StringUtils.rightPad(matcher.group(6), 9, '0'));
        Long micros =
            TimeUnit.NANOSECONDS.toMicros(
                TimeUnit.HOURS.toNanos(hours)
                    + TimeUnit.MINUTES.toNanos(minutes)
                    + TimeUnit.SECONDS.toNanos(seconds)
                    + nanoSeconds);
        // Negate if negative.
        return isNegative ? -1 * micros : micros;
      };

  /**
   * Static mapping of SourceColumnType to {@link ResultSetValueExtractor} and {@link
   * ResultSetValueMapper}.
   */
  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIGINT UNSIGNED", Pair.of(ResultSet::getBigDecimal, bigDecimalToAvroNumber))
          .put("BINARY", Pair.of(ResultSet::getBytes, bytesToHexString))
          .put("BIT", Pair.of(ResultSet::getBytes, bytesToLong))
          .put("BLOB", Pair.of(ResultSet::getBlob, blobToHexString))
          .put("BOOL", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("CHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(utcDateExtractor, sqlDateToAvroTimestampMicros))
          .put("DATETIME", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroDateTime))
          .put("DECIMAL", Pair.of(ResultSet::getBigDecimal, bigDecimalToByteArray))
          .put("DOUBLE", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("ENUM", Pair.of(ResultSet::getString, valuePassThrough))
          .put("FLOAT", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("INTEGER", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INTEGER UNSIGNED", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("JSON", Pair.of(ResultSet::getString, valuePassThrough))
          .put("LONGBLOB", Pair.of(ResultSet::getBlob, blobToHexString))
          .put("LONGTEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MEDIUMBLOB", Pair.of(ResultSet::getBlob, blobToHexString))
          .put("MEDIUMINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("MEDIUMTEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("SET", Pair.of(ResultSet::getString, valuePassThrough))
          .put("SMALLINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIME", Pair.of(ResultSet::getString, timeStringToAvroTimeInterval))
          .put("TIMESTAMP", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put("TINYBLOB", Pair.of(ResultSet::getBlob, blobToHexString))
          .put("TINYINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("TINYTEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARBINARY", Pair.of(ResultSet::getBytes, bytesToHexString))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("YEAR", Pair.of(ResultSet::getInt, valuePassThrough))
          .build()
          .entrySet()
          .stream()
          .map(
              entry ->
                  Map.entry(
                      entry.getKey(),
                      new JdbcValueMapper<>(
                          entry.getValue().getLeft(), entry.getValue().getRight())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return SCHEMA_MAPPINGS;
  }
}
