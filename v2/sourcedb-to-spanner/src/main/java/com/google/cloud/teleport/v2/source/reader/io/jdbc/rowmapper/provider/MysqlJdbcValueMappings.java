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
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.Interval;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MysqlJdbcValueMappings.class);

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

  private static long getLengthOrPrecision(SourceColumnType sourceColumnType) {
    Long[] mods = sourceColumnType.getMods();
    return (mods != null && mods.length > 0 && mods[0] != null) ? mods[0] : 0;
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
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 8)
          .put("BIGINT UNSIGNED", ResultSet::getBigDecimal, bigDecimalToAvroNumber, 8)
          .put(
              "BINARY",
              ResultSet::getBytes,
              bytesToHexString,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // in BINARY length is measured in bytes. ref:
                // https://dev.mysql.com/doc/refman/8.4/en/binary-varbinary.html
                return (int) (n > 0 ? n : 255);
              })
          .put(
              "BIT",
              ResultSet::getLong,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // BIT(N) -> (N+7)/8 since it is stored in bytes
                return (int) ((n > 0 ? n : 1) + 7) / 8;
              })
          .put("BLOB", ResultSet::getBlob, blobToHexString, 65_535) // BLOB -> 65,535 bytes
          .put("BOOL", ResultSet::getInt, valuePassThrough, 1)
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // CHAR -> N * 4 since it takes 4 bytes per char in utf8mb4 format. Max length
                // is 255.
                return (int) ((n > 0 ? n : 255) * 4);
              })
          /*
           * Time related type sizes are inferred from the way the JDBC driver decodes the
           * binary data ref:
           * https://github.com/mysql/mysql-connector-j/blob/release/8.0/src/main/protocol-impl/java/com/mysql/cj/protocol/a/MysqlBinaryValueDecoder.java
           */
          .put("DATE", utcDateExtractor, sqlDateToAvroTimestampMicros, 4)
          .put("DATETIME", utcTimeStampExtractor, sqlTimestampToAvroDateTime, 11)
          .put(
              "DECIMAL",
              ResultSet::getBigDecimal,
              bigDecimalToByteArray,
              sourceColumnType -> {
                long m = getLengthOrPrecision(sourceColumnType);
                // DECIMAL(M,D) -> M + 2 bytes since it is internally stored as a byte encoded
                // string (+2 for sign and decimal point)
                // Max number of digits in decimal is 65. Ref:
                // https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html
                return (int) ((m > 0 ? m : 65) + 2);
              })
          .put("DOUBLE", ResultSet::getDouble, valuePassThrough, 8)
          .put(
              "ENUM",
              ResultSet::getString,
              valuePassThrough,
              1020) // The maximum supported length of an individual ENUM element is M <= 255
          // and (M x w) <= 1020, where M is the element literal length and w is the
          // number of bytes required for the maximum-length character in the character
          // set. https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html
          .put("FLOAT", ResultSet::getFloat, valuePassThrough, 4)
          .put("INTEGER", ResultSet::getInt, valuePassThrough, 4)
          .put("INTEGER UNSIGNED", ResultSet::getLong, valuePassThrough, 4)
          .put(
              "JSON", ResultSet::getString, valuePassThrough, Integer.MAX_VALUE) // JSON -> Long Max
          .put(
              "LONGBLOB",
              ResultSet::getBlob,
              blobToHexString,
              Integer.MAX_VALUE) // LONGBLOB -> Long Max
          .put(
              "LONGTEXT",
              ResultSet::getString,
              valuePassThrough,
              Integer.MAX_VALUE) // LONGTEXT -> Long Max
          .put("MEDIUMBLOB", ResultSet::getBlob, blobToHexString, 16_777_215) // MEDIUMBLOB -> 16MB
          .put("MEDIUMINT", ResultSet::getInt, valuePassThrough, 4)
          .put(
              "MEDIUMTEXT",
              ResultSet::getString,
              valuePassThrough,
              16_777_215) // MEDIUMTEXT -> 16MB
          .put(
              "SET",
              ResultSet::getString,
              valuePassThrough,
              1020 * 64) // Number of elements in a SET can be up to 64. The maximum supported
          // length of an individual SET element is M <= 255 and (M x w) <= 1020, where M
          // is the element literal length and w is the number of bytes required for the
          // maximum-length character in the character set.
          // https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html
          .put("SMALLINT", ResultSet::getInt, valuePassThrough, 2)
          .put("TEXT", ResultSet::getString, valuePassThrough, 65_535)
          .put("TIME", ResultSet::getString, timeStringToAvroTimeInterval, 12)
          .put("TIMESTAMP", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 11)
          .put("TINYBLOB", ResultSet::getBlob, blobToHexString, 255)
          .put("TINYINT", ResultSet::getInt, valuePassThrough, 1)
          .put("TINYTEXT", ResultSet::getString, valuePassThrough, 255)
          .put(
              "VARBINARY",
              ResultSet::getBytes,
              bytesToHexString,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // in VARBINARY length is measured in bytes. ref:
                // https://dev.mysql.com/doc/refman/8.4/en/binary-varbinary.html
                return (int) (n > 0 ? n : 65535);
              })
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                long n = getLengthOrPrecision(sourceColumnType);
                // VARCHAR -> N * 4 since it takes 4 bytes per char in utf8mb4 format. Max bytes
                // allowed is 65535. ref: https://dev.mysql.com/doc/refman/8.4/en/char.html
                return (int) (n > 0 ? n * 4 : 65535);
              })
          .put("YEAR", ResultSet::getInt, valuePassThrough, 2)
          .put("INT", ResultSet::getInt, valuePassThrough, 4)
          .put("REAL", ResultSet::getDouble, valuePassThrough, 8)
          .put(
              "NUMERIC",
              ResultSet::getBigDecimal,
              bigDecimalToByteArray,
              sourceColumnType -> {
                long m = getLengthOrPrecision(sourceColumnType);
                // NUMERIC(M,D) -> M + 2 bytes since it is internally stored as a byte encoded
                // string (+2 for sign and decimal point)
                // Max number of digits in Numeric is 65. Ref:
                // https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html
                return (int) ((m > 0 ? m : 65) + 2);
              })
          .build();

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }

  /**
   * estimate the column size in bytes for a given column type.
   *
   * <p>Ref: <a href= "https://dev.mysql.com/doc/refman/8.4/en/storage-requirements.html">MySQL
   * Storage Requirements</a>
   */
  @Override
  public int estimateColumnSize(SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    if (JDBC_MAPPINGS.sizeEstimators().containsKey(typeName)) {
      return JDBC_MAPPINGS.sizeEstimators().get(typeName).apply(sourceColumnType);
    }
    LOG.warn(
        "Unknown column type: {}. Defaulting to size: 65,535.",
        sourceColumnType);
    return 65_535;
  }
}
