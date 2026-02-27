/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link MysqlTimeConverter}. */
@RunWith(JUnit4.class)
public class MysqlTimeConverterTest {

  @Test
  public void testNullOrEmpty() {
    assertNull(MysqlTimeConverter.toDuration(null));
    assertNull(MysqlTimeConverter.toDuration(new byte[0]));
  }

  @Test
  public void testTextProtocol() {
    // Standard positive case
    assertEquals(
        Duration.ofHours(10).plusMinutes(11).plusSeconds(12).plusNanos(123456000),
        MysqlTimeConverter.toDuration("10:11:12.123456".getBytes(StandardCharsets.UTF_8)));

    // Standard negative case
    assertEquals(
        Duration.ofHours(-838).minusMinutes(59).minusSeconds(59),
        MysqlTimeConverter.toDuration("-838:59:59.000000".getBytes(StandardCharsets.UTF_8)));

    // Zero time
    assertEquals(
        Duration.ZERO,
        MysqlTimeConverter.toDuration("00:00:00.000000".getBytes(StandardCharsets.UTF_8)));

    // Missing fraction
    assertEquals(
        Duration.ofHours(838).plusMinutes(59).plusSeconds(59),
        MysqlTimeConverter.toDuration("838:59:59".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testTextProtocolInvalidFormat() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MysqlTimeConverter.toDuration("invalid_time_string".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testBinaryProtocol() {
    // 0x00 (positive)
    // 0x01, 0x00, 0x00, 0x00 (1 day)
    // 0x02 (2 hours)
    // 0x03 (3 mins)
    // 0x04 (4 seconds)
    // 0x05, 0x00, 0x00, 0x00 (5 micros) -> length 12
    byte[] binaryPositive = new byte[] {0, 1, 0, 0, 0, 2, 3, 4, 5, 0, 0, 0};
    Duration expectedPositive =
        Duration.ofDays(1)
            .plusHours(2)
            .plusMinutes(3)
            .plusSeconds(4)
            .plusNanos(5000); // 5 micros = 5000 nanos
    assertEquals(expectedPositive, MysqlTimeConverter.toDuration(binaryPositive));

    // Negative case
    byte[] binaryNegative = new byte[] {1, 1, 0, 0, 0, 2, 3, 4, 5, 0, 0, 0};
    assertEquals(expectedPositive.negated(), MysqlTimeConverter.toDuration(binaryNegative));

    // Zero time
    byte[] binaryZero = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    assertEquals(Duration.ZERO, MysqlTimeConverter.toDuration(binaryZero));

    // Short binary (only up to seconds)
    byte[] binaryShort = new byte[] {0, 1, 0, 0, 0, 2, 3, 4};
    Duration expectedShort = Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4);
    assertEquals(expectedShort, MysqlTimeConverter.toDuration(binaryShort));
  }
}
