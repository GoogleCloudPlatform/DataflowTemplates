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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.math.BigDecimal;
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
  @SuppressWarnings("null")
  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIGINT UNSIGNED", Pair.of(ResultSet::getBigDecimal, bigDecimalToAvroNumber))
          .put("BINARY", Pair.of(ResultSet::getBytes, bytesToHexString))
          .put("BIT", Pair.of(ResultSet::getLong, valuePassThrough))
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

  /**
   * Guess the column size in bytes for a given column type.
   *
   * <p>
   * Ref: <a href=
   * "https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html">MySQL
   * Storage Requirements</a>
   */
  @Override
  public int guessColumnSize(com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
      String typeName = sourceColumnType.getName().toUpperCase();
      switch (typeName) {
          // numeric types
          // Ref: https://dev.mysql.com/doc/refman/8.4/en/integer-types.html
          case "TINYINT":
              return 1;
          case "SMALLINT":
          case "YEAR":
              return 2;
          case "MEDIUMINT":
              return 3;
          case "INT":
          case "INTEGER":
          case "FLOAT": // Partitioned as float/int
              return 4;
          case "BIGINT":
          case "DOUBLE":
          case "REAL": // MySQL REAL is DOUBLE by default unless REAL_AS_FLOAT sql mode is enabled
              return 8;
          // Date and Time
          // Ref:
          // https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html#data-types-storage-reqs-date-time
          case "DATE":
              return 3;
          case "TIME": // Time is 3 bytes + fractional seconds storage (0-3 bytes)
              return 6; // average/safe upper bound
          case "TIMESTAMP": // 4 bytes + fractional
              return 7;
          case "DATETIME": // 5 bytes + fractional
              return 8;
          // String/Binary types
          // Ref:
          // https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html#data-types-storage-reqs-strings
          case "CHAR":
          case "BINARY":
          case "VARCHAR":
          case "VARBINARY":
          case "TEXT":
          case "BLOB":
          case "TINYTEXT":
          case "TINYBLOB":
          case "MEDIUMTEXT":
          case "MEDIUMBLOB":
          case "LONGTEXT":
          case "LONGBLOB":
          case "ENUM":
          case "SET":
          case "JSON":
              return guessVariableTypeSize(sourceColumnType);
          case "BIT":
              // (M+7)/8 bytes
              Long[] mods = sourceColumnType.getMods();
              long bits = (mods != null && mods.length > 0 && mods[0] != null) ? mods[0] : 1;
              return (int) ((bits + 7) / 8);
          case "DECIMAL":
          case "NUMERIC":
              // Variable, but usually packed. precision/9 * 4 bytes.
              // Let's assume a safe average if not calculable. 16 bytes is decent for common
              // usage.
              return 16;
          default:
              // Default safe fallback
              return 16;
      }
  }

  private int guessVariableTypeSize(
          com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType sourceColumnType) {
      String typeName = sourceColumnType.getName().toUpperCase();
      long length = 0;
      Long[] mods = sourceColumnType.getMods();
      if (mods != null && mods.length > 0 && mods[0] != null) {
          length = mods[0];
      } else {
          // Defaults if length not specified
          switch (typeName) {
              case "TINYTEXT":
              case "TINYBLOB":
                  length = 255;
                  break;
              case "TEXT":
              case "BLOB":
                  length = 65_535;
                  break;
              case "MEDIUMTEXT":
              case "MEDIUMBLOB":
                  length = 16_777_215;
                  break;
              case "LONGTEXT":
              case "LONGBLOB":
              case "JSON":
                  // Cap at a reasonable limit for fetch size calculation to avoid divide by zero
                  // or tiny fetch sizes.
                  // 4GB is too big. Let's assume 20KB average for unchecked huge fields?
                  // Or strictly follow "max row size" logic which would kill fetch size?
                  // The user wanted "max row size", but for unbounded types, using full 4GB is
                  // impractical (fetch size = 0).
                  // 4MB (max allowed packet default often) might be a better "max" proxy?
                  // Let's stick to the previous conservative 1KB or similar, OR use a
                  // configurable max?
                  // FetchSizeCalculator previously used 10MB (10 * 1024 * 1024).
                  length = 10 * 1024 * 1024;
                  break;
              default:
                  length = 255; // Default for VARCHAR/CHAR if unknown
          }
      }

      // Checking for multi-byte chars (Utf8mb4 is max 4 bytes)
      // binary types don't need multiplier.
      if (typeName.contains("TEXT") || typeName.contains("CHAR") || typeName.equals("JSON") || typeName.equals("ENUM")
              || typeName.equals("SET")) {
          return (int) (length * 4);
      } else {
          return (int) length;
      }
  }
}
