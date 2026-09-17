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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PostgreSQLTimeConverterTest {

  @Test
  public void testToLocalTime_Null() {
    assertNull(PostgreSQLTimeConverter.toLocalTime(null));
  }

  @Test
  public void testToLocalTime_TextProtocol_Normal() {
    byte[] textBytes = "15:45:30.123456".getBytes(StandardCharsets.UTF_8);
    LocalTime time = PostgreSQLTimeConverter.toLocalTime(textBytes);
    assertEquals(LocalTime.of(15, 45, 30, 123456000), time);
  }

  @Test
  public void testToLocalTime_TextProtocol_24Hours() {
    byte[] textBytes = "24:00:00".getBytes(StandardCharsets.UTF_8);
    LocalTime time = PostgreSQLTimeConverter.toLocalTime(textBytes);
    assertEquals(LocalTime.MAX, time);
  }

  @Test
  public void testToLocalTime_BinaryProtocol_Normal() {
    long microseconds = 15L * 3600_000_000L + 45L * 60_000_000L + 30L * 1_000_000L + 123456L;
    byte[] binaryBytes = ByteBuffer.allocate(8).putLong(microseconds).array();

    LocalTime time = PostgreSQLTimeConverter.toLocalTime(binaryBytes);
    assertEquals(LocalTime.of(15, 45, 30, 123456000), time);
  }

  @Test
  public void testToLocalTime_BinaryProtocol_24Hours() {
    long microseconds = 86400000000L;
    byte[] binaryBytes = ByteBuffer.allocate(8).putLong(microseconds).array();

    LocalTime time = PostgreSQLTimeConverter.toLocalTime(binaryBytes);
    assertEquals(LocalTime.MAX, time);
  }

  @Test
  public void testToOffsetTime_Null() {
    assertNull(PostgreSQLTimeConverter.toOffsetTime(null));
  }

  @Test
  public void testToOffsetTime_TextProtocol_Normal() {
    byte[] textBytes = "15:45:30.123456+02:00".getBytes(StandardCharsets.UTF_8);
    OffsetTime time = PostgreSQLTimeConverter.toOffsetTime(textBytes);
    assertEquals(OffsetTime.of(15, 45, 30, 123456000, ZoneOffset.ofHours(2)), time);
  }

  @Test
  public void testToOffsetTime_TextProtocol_24Hours() {
    byte[] textBytes = "24:00:00-05:00".getBytes(StandardCharsets.UTF_8);
    OffsetTime time = PostgreSQLTimeConverter.toOffsetTime(textBytes);
    assertEquals(OffsetTime.of(LocalTime.MAX, ZoneOffset.ofHours(-5)), time);
  }

  @Test
  public void testToOffsetTime_BinaryProtocol_Normal() {
    long microseconds = 15L * 3600_000_000L + 45L * 60_000_000L + 30L * 1_000_000L + 123456L;
    // Postgres stores timezone offset inverted (West of UTC is positive).
    // So +02:00 is -7200 seconds.
    int offsetSeconds = -7200;

    byte[] binaryBytes =
        ByteBuffer.allocate(12).putLong(microseconds).putInt(offsetSeconds).array();

    OffsetTime time = PostgreSQLTimeConverter.toOffsetTime(binaryBytes);
    assertEquals(OffsetTime.of(15, 45, 30, 123456000, ZoneOffset.ofHours(2)), time);
  }

  @Test
  public void testToOffsetTime_BinaryProtocol_24Hours() {
    long microseconds = 86400000000L;
    // -05:00 is 18000 seconds in Postgres format.
    int offsetSeconds = 18000;

    byte[] binaryBytes =
        ByteBuffer.allocate(12).putLong(microseconds).putInt(offsetSeconds).array();

    OffsetTime time = PostgreSQLTimeConverter.toOffsetTime(binaryBytes);
    assertEquals(OffsetTime.of(LocalTime.MAX, ZoneOffset.ofHours(-5)), time);
  }
}
