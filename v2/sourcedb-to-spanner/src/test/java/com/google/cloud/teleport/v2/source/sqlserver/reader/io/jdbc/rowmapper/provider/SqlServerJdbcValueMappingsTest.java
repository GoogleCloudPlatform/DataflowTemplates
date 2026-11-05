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
package com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.rowmapper.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import org.junit.Test;

public class SqlServerJdbcValueMappingsTest {

  @Test
  public void testJsonJdbcMappings() throws Exception {
    SqlServerJdbcValueMappings provider = new SqlServerJdbcValueMappings();
    assertTrue(provider.getMappings().containsKey("JSON"));

    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("json_col")).thenReturn("{\"key\": \"value\"}");

    JdbcValueMapper<?> jsonMapper = provider.getMappings().get("JSON");
    assertEquals("{\"key\": \"value\"}", jsonMapper.mapValue(rs, "json_col", null));
  }

  @Test
  public void testAllTypeMappingsAndEstimateColumnSize() throws Exception {
    SqlServerJdbcValueMappings provider = new SqlServerJdbcValueMappings();
    ResultSet rs = mock(ResultSet.class);

    byte[] sampleBytes = new byte[] {1, 2, 3, 4};
    when(rs.getBytes("bin_col")).thenReturn(sampleBytes);
    assertEquals(
        ByteBuffer.wrap(sampleBytes),
        provider.getMappings().get("BINARY").mapValue(rs, "bin_col", null));

    Timestamp ts = new Timestamp(1000L);
    ts.setNanos(500000);
    when(rs.getTimestamp(eq("dt_col"), any())).thenReturn(ts);
    assertEquals(
        Long.valueOf(1000L * 1000 + 500),
        provider.getMappings().get("DATETIME2").mapValue(rs, "dt_col", null));

    Date sqlDate = Date.valueOf("2023-01-02");
    when(rs.getDate(eq("date_col"), any())).thenReturn(sqlDate);
    assertEquals(
        Integer.valueOf((int) sqlDate.toLocalDate().toEpochDay()),
        provider.getMappings().get("DATE").mapValue(rs, "date_col", null));

    BigDecimal bd = new BigDecimal("123.45");
    when(rs.getBigDecimal("dec_col")).thenReturn(bd);
    assertEquals(
        ByteBuffer.wrap(bd.unscaledValue().toByteArray()),
        provider.getMappings().get("DECIMAL").mapValue(rs, "dec_col", null));

    // estimateColumnSize for known and unknown types
    assertEquals(4, provider.estimateColumnSize(new SourceColumnType("INT", new Long[] {}, null)));
    assertEquals(
        5, provider.estimateColumnSize(new SourceColumnType("DECIMAL", new Long[] {5L, 2L}, null)));
    assertEquals(
        9,
        provider.estimateColumnSize(new SourceColumnType("DECIMAL", new Long[] {15L, 2L}, null)));
    assertEquals(
        13,
        provider.estimateColumnSize(new SourceColumnType("DECIMAL", new Long[] {25L, 2L}, null)));
    assertEquals(
        17,
        provider.estimateColumnSize(new SourceColumnType("NUMERIC", new Long[] {38L, 9L}, null)));
    assertEquals(
        10, provider.estimateColumnSize(new SourceColumnType("CHAR", new Long[] {10L}, null)));
    assertEquals(
        50, provider.estimateColumnSize(new SourceColumnType("VARCHAR", new Long[] {50L}, null)));
    assertEquals(
        8000, provider.estimateColumnSize(new SourceColumnType("VARCHAR", new Long[] {}, null)));
    assertEquals(
        20, provider.estimateColumnSize(new SourceColumnType("NCHAR", new Long[] {10L}, null)));
    assertEquals(
        100, provider.estimateColumnSize(new SourceColumnType("NVARCHAR", new Long[] {50L}, null)));
    assertEquals(
        16, provider.estimateColumnSize(new SourceColumnType("BINARY", new Long[] {16L}, null)));
    assertEquals(
        100,
        provider.estimateColumnSize(new SourceColumnType("VARBINARY", new Long[] {100L}, null)));
    assertEquals(
        65535,
        provider.estimateColumnSize(new SourceColumnType("UNKNOWN_TYPE", new Long[] {}, null)));
  }
}
