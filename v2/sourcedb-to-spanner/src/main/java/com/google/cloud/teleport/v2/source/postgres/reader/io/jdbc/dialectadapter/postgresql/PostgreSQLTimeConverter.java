/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.dialectadapter.postgresql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.regex.Pattern;

/**
 * Utility class to convert PostgreSQL TIME and TIMETZ values (in bytes) to {@link LocalTime} and
 * {@link OffsetTime}. Handles both Text Protocol (String bytes) and Binary Protocol (Packed bytes).
 */
public class PostgreSQLTimeConverter implements Serializable {

  private static final long MAX_POSTGRES_TIME_MICROS = 86400000000L;

  // Matches PostgreSQL TIME text formats (e.g., "15:45:30", "15:45:30.123456")
  private static final Pattern TIME_PATTERN = Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?$");

  // Matches PostgreSQL TIMETZ text formats (e.g., "15:45:30+05", "15:45:30.123456-07:30")
  private static final Pattern TIMETZ_PATTERN =
      Pattern.compile("^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}(:\\d{2}(:\\d{2})?)?)$");

  // Formatter for parsing the standard components of a TIMETZ string once validated.
  private static final DateTimeFormatter TIMETZ_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
          .optionalEnd()
          .appendOffset("+HH:mm:ss", "+00")
          .toFormatter();

  /**
   * Converts raw byte[] from a PostgreSQL TIME column to a {@link LocalTime}.
   *
   * @param bytes The byte array from ResultSet.getBytes().
   * @return The corresponding LocalTime, or null if value is null.
   */
  public static LocalTime toLocalTime(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    // Binary format
    // A PostgreSQL binary time payload represents microseconds since midnight.
    // Max value is 86,400,000,000, which takes at most 5 bytes.
    // Thus, the first byte of a valid 8-byte binary time payload will always be 0x00.
    // Text payloads (like "08:00:00") will start with an ASCII digit byte ('0'-'9'), never 0x00.
    // This allows us to safely distinguish binary format without string allocation.
    if (bytes.length == 8 && bytes[0] == 0) {
      long microseconds = ByteBuffer.wrap(bytes).getLong();
      if (microseconds == MAX_POSTGRES_TIME_MICROS) {
        return LocalTime.MAX;
      }
      return LocalTime.ofNanoOfDay(microseconds * 1000L);
    }

    // Text format
    String textFormat = new String(bytes, StandardCharsets.UTF_8);
    if (TIME_PATTERN.matcher(textFormat).matches()) {
      if (textFormat.startsWith("24:00:00")) {
        return LocalTime.MAX;
      }
      return LocalTime.parse(textFormat);
    }

    throw new IllegalArgumentException("Unknown time format received from PostgreSQL");
  }

  /**
   * Converts raw byte[] from a PostgreSQL TIMETZ column to a {@link OffsetTime}.
   *
   * @param bytes The byte array from ResultSet.getBytes().
   * @return The corresponding OffsetTime, or null if value is null.
   */
  public static OffsetTime toOffsetTime(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    // Binary format
    // The first 8 bytes of a 12-byte PostgreSQL binary timetz payload represent
    // microseconds since midnight (max 86,400,000,000), meaning the first byte is always 0x00.
    // A text payload (like "08:00:00+00") starts with an ASCII digit ('0'-'9').
    // This allows us to safely distinguish binary format without string allocation.
    if (bytes.length == 12 && bytes[0] == 0) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      long microseconds = buffer.getLong();
      int offsetSeconds = buffer.getInt();

      // PostgreSQL stores timezone offset inverted (West of UTC is positive).
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(-offsetSeconds);

      if (microseconds == MAX_POSTGRES_TIME_MICROS) {
        return OffsetTime.of(LocalTime.MAX, offset);
      }
      return OffsetTime.of(LocalTime.ofNanoOfDay(microseconds * 1000L), offset);
    }

    // Text format
    String textFormat = new String(bytes, StandardCharsets.UTF_8);
    if (TIMETZ_PATTERN.matcher(textFormat).matches()) {
      if (textFormat.startsWith("24:00:00")) {
        String replacedStr = "00" + textFormat.substring(2);
        OffsetTime parsed = OffsetTime.parse(replacedStr, TIMETZ_FORMAT);
        return OffsetTime.of(LocalTime.MAX, parsed.getOffset());
      }
      return OffsetTime.parse(textFormat, TIMETZ_FORMAT);
    }

    throw new IllegalArgumentException("Unknown TIMETZ format received from PostgreSQL");
  }
}
