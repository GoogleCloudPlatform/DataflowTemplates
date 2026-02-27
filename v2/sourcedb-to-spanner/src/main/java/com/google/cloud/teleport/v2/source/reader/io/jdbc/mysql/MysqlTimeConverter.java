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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.mysql;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class to convert MySQL TIME values (in bytes) to {@link java.time.Duration}. Handles both
 * Text Protocol (String bytes) and Binary Protocol (Packed bytes).
 */
public class MysqlTimeConverter implements Serializable {

  // Pattern to extract fields form MySQL time output of HH:MM:ss.SSS
  private static final Pattern TIME_STRING_PATTERN =
      Pattern.compile("^(-)?(\\d+):(\\d+):(\\d+)(\\.(\\d+))?$");

  /**
   * Converts raw byte[] from a MySQL TIME column to a {@link Duration}.
   *
   * @param value The byte array from ResultSet.getBytes().
   * @return The corresponding Duration, or null if value is null/empty.
   */
  public static Duration toDuration(byte[] value) {
    if (value == null || value.length == 0) {
      return null;
    }

    // Binary Protocol decoding (uses 0x00 or 0x01 as the first byte for sign flag)
    // In Text Protocol, first byte is '-' (0x2D) or an ASCII digit (>= 0x30).
    if (value[0] == 0 || value[0] == 1) {
      boolean isNegative = (value[0] == 1);
      int days = 0;
      int hours = 0;
      int minutes = 0;
      int seconds = 0;
      long micros = 0;

      if (value.length >= 8) {
        days =
            (value[1] & 0xFF)
                | ((value[2] & 0xFF) << 8)
                | ((value[3] & 0xFF) << 16)
                | ((value[4] & 0xFF) << 24);
        hours = value[5] & 0xFF;
        minutes = value[6] & 0xFF;
        seconds = value[7] & 0xFF;
      }
      if (value.length >= 12) {
        micros =
            ((value[8] & 0xFF)
                | ((value[9] & 0xFF) << 8)
                | ((value[10] & 0xFF) << 16)
                | ((value[11] & 0xFF) << 24));
      }

      long totalMicros =
          TimeUnit.HOURS.toMicros((days * 24L) + hours)
              + TimeUnit.MINUTES.toMicros(minutes)
              + TimeUnit.SECONDS.toMicros(seconds)
              + micros;

      return Duration.ofNanos((isNegative ? -1 : 1) * TimeUnit.MICROSECONDS.toNanos(totalMicros));

    } else {
      // Text Protocol handling
      String timeStr = new String(value, StandardCharsets.UTF_8);
      /* MySQL output is always hours:minutes:seconds.fractionalSeconds */
      Matcher matcher = TIME_STRING_PATTERN.matcher(timeStr);
      Preconditions.checkArgument(
          matcher.matches(),
          "The time string " + timeStr + " does not match " + TIME_STRING_PATTERN);
      boolean isNegative = matcher.group(1) != null;
      int hours = Integer.parseInt(matcher.group(2));
      int minutes = Integer.parseInt(matcher.group(3));
      int seconds = Integer.parseInt(matcher.group(4));
      long nanoSeconds =
          matcher.group(5) == null
              ? 0
              : Long.parseLong(StringUtils.rightPad(matcher.group(6), 9, '0'));

      // Calculate total nanos
      long totalNanos =
          TimeUnit.HOURS.toNanos(hours)
              + TimeUnit.MINUTES.toNanos(minutes)
              + TimeUnit.SECONDS.toNanos(seconds)
              + nanoSeconds;

      return Duration.ofNanos(isNegative ? -totalNanos : totalNanos);
    }
  }
}
