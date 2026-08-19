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
package com.google.cloud.teleport.v2.source.mysql.reader.io.jdbc.rowmapper.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MysqlJdbcValueMappingsTest {

  @Test
  public void testAllMappedTypesHaveRowSizeEstimate() {
    MysqlJdbcValueMappings mappings = new MysqlJdbcValueMappings();
    for (String typeName : mappings.getMappings().keySet()) {
      SourceColumnType sourceColumnType = new SourceColumnType(typeName, new Long[] {10L}, null);
      int size = mappings.estimateColumnSize(sourceColumnType);
      assertTrue("Row size estimate for type " + typeName + " should be > 0", size > 0);
    }
  }

  @Test
  public void testAllMappedTypesHaveRowSizeEstimateWithoutMods() {
    MysqlJdbcValueMappings mappings = new MysqlJdbcValueMappings();
    for (String typeName : mappings.getMappings().keySet()) {
      SourceColumnType sourceColumnType = new SourceColumnType(typeName, null, null);
      int size = mappings.estimateColumnSize(sourceColumnType);
      assertTrue(
          "Row size estimate for type " + typeName + " without mods should be > 0", size > 0);
    }
  }

  @Test
  public void testUnknownTypeReturnsDefaultSize() {
    MysqlJdbcValueMappings mappings = new MysqlJdbcValueMappings();
    SourceColumnType sourceColumnType =
        new SourceColumnType("UNKNOWN_TYPE", new Long[] {10L}, null);
    int size = mappings.estimateColumnSize(sourceColumnType);
    assertEquals(65_535, size);
  }

  @Test
  public void testBytesToAvroTimeInterval_TextProtocol() throws Exception {
    Field field = MysqlJdbcValueMappings.class.getDeclaredField("bytesToAvroTimeInterval");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    ResultSetValueMapper<byte[]> mapper = (ResultSetValueMapper<byte[]>) field.get(null);

    // Test standard cases
    assertEquals(
        TimeUnit.HOURS.toMicros(10)
            + TimeUnit.MINUTES.toMicros(11)
            + TimeUnit.SECONDS.toMicros(12)
            + 123456L,
        mapper.map("10:11:12.123456".getBytes(StandardCharsets.UTF_8), null));

    assertEquals(
        -1
            * (TimeUnit.HOURS.toMicros(838)
                + TimeUnit.MINUTES.toMicros(59)
                + TimeUnit.SECONDS.toMicros(59)),
        mapper.map("-838:59:59.000000".getBytes(StandardCharsets.UTF_8), null));

    assertEquals(0L, mapper.map("00:00:00.000000".getBytes(StandardCharsets.UTF_8), null));

    assertEquals(
        TimeUnit.HOURS.toMicros(838)
            + TimeUnit.MINUTES.toMicros(59)
            + TimeUnit.SECONDS.toMicros(59),
        mapper.map("838:59:59".getBytes(StandardCharsets.UTF_8), null));
  }

  @Test
  public void testBytesToAvroTimeInterval_BinaryProtocol() throws Exception {
    Field field = MysqlJdbcValueMappings.class.getDeclaredField("bytesToAvroTimeInterval");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    ResultSetValueMapper<byte[]> mapper = (ResultSetValueMapper<byte[]>) field.get(null);

    // Test standard positive case
    // 0x00 (positive)
    // 0x01, 0x00, 0x00, 0x00 (1 day)
    // 0x02 (2 hours)
    // 0x03 (3 mins)
    // 0x04 (4 seconds)
    // 0x05, 0x00, 0x00, 0x00 (5 micros) -> length 12
    byte[] binaryPositive = new byte[] {0, 1, 0, 0, 0, 2, 3, 4, 5, 0, 0, 0};
    long expectedPositive =
        TimeUnit.DAYS.toMicros(1)
            + TimeUnit.HOURS.toMicros(2)
            + TimeUnit.MINUTES.toMicros(3)
            + TimeUnit.SECONDS.toMicros(4)
            + 5;
    assertEquals(expectedPositive, mapper.map(binaryPositive, null));

    // Test standard negative case
    byte[] binaryNegative = new byte[] {1, 1, 0, 0, 0, 2, 3, 4, 5, 0, 0, 0};
    assertEquals(-expectedPositive, mapper.map(binaryNegative, null));

    // Test zero time
    byte[] binaryZero = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    assertEquals(0L, mapper.map(binaryZero, null));

    // Test null / empty
    assertEquals(0L, mapper.map(new byte[0], null));
    assertEquals(0L, mapper.map(null, null));

    // Test short binary (only up to seconds)
    byte[] binaryShort = new byte[] {0, 1, 0, 0, 0, 2, 3, 4};
    long expectedShort =
        TimeUnit.DAYS.toMicros(1)
            + TimeUnit.HOURS.toMicros(2)
            + TimeUnit.MINUTES.toMicros(3)
            + TimeUnit.SECONDS.toMicros(4);
    assertEquals(expectedShort, mapper.map(binaryShort, null));
  }

  @Test
  public void testSqlDateToAvroDate() throws Exception {
    Field field = MysqlJdbcValueMappings.class.getDeclaredField("sqlDateToAvroDate");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    ResultSetValueMapper<Date> mapper = (ResultSetValueMapper<Date>) field.get(null);

    // Standard date
    int epochDay1 = (int) LocalDate.parse("2012-09-17").toEpochDay();
    Date d1 = Date.valueOf("2012-09-17");
    assertEquals(epochDay1, (int) mapper.map(d1, null));

    // Min date (1000-01-01)
    int epochDay2 = (int) LocalDate.parse("1000-01-01").toEpochDay();
    Date d2 = Date.valueOf("1000-01-01");
    assertEquals(epochDay2, (int) mapper.map(d2, null));

    // Max date (9999-12-31)
    int epochDay3 = (int) LocalDate.parse("9999-12-31").toEpochDay();
    Date d3 = Date.valueOf("9999-12-31");
    assertEquals(epochDay3, (int) mapper.map(d3, null));

    // Leap year (2000-02-29)
    int epochDay4 = (int) LocalDate.parse("2000-02-29").toEpochDay();
    Date d4 = Date.valueOf("2000-02-29");
    assertEquals(epochDay4, (int) mapper.map(d4, null));

    // Leap year (2024-02-29)
    int epochDay5 = (int) LocalDate.parse("2024-02-29").toEpochDay();
    Date d5 = Date.valueOf("2024-02-29");
    assertEquals(epochDay5, (int) mapper.map(d5, null));

    // Epoch day zero (1970-01-01)
    Date d6 = Date.valueOf("1970-01-01");
    assertEquals(0, (int) mapper.map(d6, null));
  }
}
