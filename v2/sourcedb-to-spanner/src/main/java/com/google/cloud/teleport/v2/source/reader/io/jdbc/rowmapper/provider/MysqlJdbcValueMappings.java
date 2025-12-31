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
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.TimeIntervalMicros;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
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
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

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

  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 24)
          .put("BIGINT UNSIGNED", ResultSet::getBigDecimal, bigDecimalToAvroNumber, 24)
          .put(
              "BINARY",
              ResultSet::getBytes,
              bytesToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "BIT",
              ResultSet::getLong,
              valuePassThrough,
              (sourceColumnType) -> {
                return 24;
              })
          .put(
              "BLOB",
              ResultSet::getBlob,
              blobToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("BOOL", ResultSet::getInt, valuePassThrough, 20)
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("DATE", utcDateExtractor, sqlDateToAvroTimestampMicros, 24)
          .put("DATETIME", utcTimeStampExtractor, sqlTimestampToAvroDateTime, 32)
          .put("DECIMAL", ResultSet::getBigDecimal, bigDecimalToByteArray, 48)
          .put("DOUBLE", ResultSet::getDouble, valuePassThrough, 24)
          .put(
              "ENUM",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("FLOAT", ResultSet::getFloat, valuePassThrough, 20)
          .put("INTEGER", ResultSet::getInt, valuePassThrough, 20)
          .put("INTEGER UNSIGNED", ResultSet::getLong, valuePassThrough, 20)
          .put(
              "JSON",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "LONGBLOB",
              ResultSet::getBlob,
              blobToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "LONGTEXT",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "MEDIUMBLOB",
              ResultSet::getBlob,
              blobToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("MEDIUMINT", ResultSet::getInt, valuePassThrough, 20)
          .put(
              "MEDIUMTEXT",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "SET",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("SMALLINT", ResultSet::getInt, valuePassThrough, 20)
          .put(
              "TEXT",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("TIME", ResultSet::getString, timeStringToAvroTimeInterval, 32)
          .put("TIMESTAMP", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 32)
          .put(
              "TINYBLOB",
              ResultSet::getBlob,
              blobToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("TINYINT", ResultSet::getInt, valuePassThrough, 20)
          .put(
              "TINYTEXT",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "VARBINARY",
              ResultSet::getBytes,
              bytesToHexString,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              MysqlJdbcValueMappings::guessVariableTypeSize)
          .put("YEAR", ResultSet::getInt, valuePassThrough, 20)
          .put("INT", ResultSet::getInt, valuePassThrough, 20) // Alias for INTEGER
          .put("REAL", ResultSet::getDouble, valuePassThrough, 24) // Alias for DOUBLE
          .put("NUMERIC", ResultSet::getBigDecimal, bigDecimalToByteArray, 48) // Alias for DECIMAL
          .build();

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }

  /**
   * Guess the column size in bytes for a given column type.
   *
   * <p>Ref: <a href= "https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html">MySQL
   * Storage Requirements</a>
   */
  @Override
  public int guessColumnSize(SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    if (JDBC_MAPPINGS.sizeEstimators().containsKey(typeName)) {
      return JDBC_MAPPINGS.sizeEstimators().get(typeName).apply(sourceColumnType);
    }
    return 16; // Default fallback
  }

  private static int guessVariableTypeSize(SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    long length = 0;
    Long[] mods = sourceColumnType.getMods();
    if (mods != null && mods.length > 0 && mods[0] != null) {
      length = mods[0];
    } else {
      // Defaults to the max possible size for the type
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
          length = 10 * 1024 * 1024;
          break;
        default:
          length = 255; // Default for VARCHAR/CHAR if unknown
      }
    }

    // Checking for multi-byte chars (Utf8mb4 is max 4 bytes)
    // binary types don't need multiplier.
    // TEXT/BLOB/JSON types are already defined by byte length limits, not character
    // counts,
    // so they do not need the multiplier.
    if (typeName.contains("CHAR") || typeName.equals("ENUM") || typeName.equals("SET")) {
      return (int) (length * 4);
    } else {
      return (int) length;
    }
  }
}
