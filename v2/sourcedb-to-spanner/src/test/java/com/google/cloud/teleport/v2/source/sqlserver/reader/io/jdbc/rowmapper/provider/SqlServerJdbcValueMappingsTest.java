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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.spanner.migrations.avro.AvroToValueMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

public class SqlServerJdbcValueMappingsTest {

  @Test
  public void testJsonAndVectorJdbcMappings() throws Exception {
    SqlServerJdbcValueMappings provider = new SqlServerJdbcValueMappings();
    assertTrue(provider.getMappings().containsKey("JSON"));
    assertTrue(provider.getMappings().containsKey("VECTOR"));

    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("json_col")).thenReturn("{\"key\": \"value\"}");
    when(rs.getObject("vec_col")).thenReturn("[1.5, -2.5, 3.0]");
    when(rs.getObject("empty_vec_col")).thenReturn("[]");
    when(rs.getObject("null_vec_col")).thenReturn(null);

    ByteBuffer binaryVecBuf = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
    binaryVecBuf.put((byte) 1); // version
    binaryVecBuf.put((byte) 0); // float32
    binaryVecBuf.putShort((short) 0); // reserved
    binaryVecBuf.putInt(3); // 3 dimensions
    binaryVecBuf.putFloat(1.5f);
    binaryVecBuf.putFloat(2.5f);
    binaryVecBuf.putFloat(3.5f);
    when(rs.getObject("binary_vec_col")).thenReturn(binaryVecBuf.array());

    JdbcValueMapper<?> jsonMapper = provider.getMappings().get("JSON");
    assertEquals("{\"key\": \"value\"}", jsonMapper.mapValue(rs, "json_col", null));

    JdbcValueMapper<?> vectorMapper = provider.getMappings().get("VECTOR");
    assertEquals(Arrays.asList(1.5, -2.5, 3.0), vectorMapper.mapValue(rs, "vec_col", null));
    assertEquals(Arrays.asList(1.5, 2.5, 3.5), vectorMapper.mapValue(rs, "binary_vec_col", null));
    assertEquals(Collections.emptyList(), vectorMapper.mapValue(rs, "empty_vec_col", null));
    assertNull(vectorMapper.mapValue(rs, "null_vec_col", null));
  }

  @Test
  public void testAvroToSpannerConversionsForJsonAndVector() {
    Schema jsonSchema = SchemaBuilder.builder().stringType();
    String jsonStr = "{\"a\": 1}";

    // GSQL JSON and STRING
    assertEquals(
        Value.json(jsonStr),
        AvroToValueMapper.convertorMap()
            .get(Dialect.GOOGLE_STANDARD_SQL)
            .get(Type.json())
            .apply(jsonStr, jsonSchema));
    assertEquals(
        Value.string(jsonStr),
        AvroToValueMapper.convertorMap()
            .get(Dialect.GOOGLE_STANDARD_SQL)
            .get(Type.string())
            .apply(jsonStr, jsonSchema));

    // PG Dialect jsonb and varchar
    assertEquals(
        Value.pgJsonb(jsonStr),
        AvroToValueMapper.convertorMap()
            .get(Dialect.POSTGRESQL)
            .get(Type.pgJsonb())
            .apply(jsonStr, jsonSchema));
    assertEquals(
        Value.string(jsonStr),
        AvroToValueMapper.convertorMap()
            .get(Dialect.POSTGRESQL)
            .get(Type.pgVarchar())
            .apply(jsonStr, jsonSchema));

    // Vector -> ARRAY / array
    Schema vectorSchema = SchemaBuilder.builder().array().items().doubleType();
    List<Double> vectorVal = Arrays.asList(1.0, 2.5, -3.5);

    // GSQL ARRAY<FLOAT64> and ARRAY<FLOAT32>
    assertEquals(
        Value.float64Array(vectorVal),
        AvroToValueMapper.convertorMap()
            .get(Dialect.GOOGLE_STANDARD_SQL)
            .get(Type.array(Type.float64()))
            .apply(vectorVal, vectorSchema));
    assertEquals(
        Value.float32Array(Arrays.asList(1.0f, 2.5f, -3.5f)),
        AvroToValueMapper.convertorMap()
            .get(Dialect.GOOGLE_STANDARD_SQL)
            .get(Type.array(Type.float32()))
            .apply(vectorVal, vectorSchema));

    // PG Dialect float8[] and float4[]
    assertEquals(
        Value.float64Array(vectorVal),
        AvroToValueMapper.convertorMap()
            .get(Dialect.POSTGRESQL)
            .get(Type.pgArray(Type.pgFloat8()))
            .apply(vectorVal, vectorSchema));
    assertEquals(
        Value.float32Array(Arrays.asList(1.0f, 2.5f, -3.5f)),
        AvroToValueMapper.convertorMap()
            .get(Dialect.POSTGRESQL)
            .get(Type.pgArray(Type.pgFloat4()))
            .apply(vectorVal, vectorSchema));
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

    // Vector edge cases: byte[] < 8 bytes and string without brackets
    when(rs.getObject("short_bytes_vec")).thenReturn(new byte[] {1, 2, 3});
    assertEquals(
        Collections.emptyList(),
        provider.getMappings().get("VECTOR").mapValue(rs, "short_bytes_vec", null));

    when(rs.getObject("unbracketed_vec")).thenReturn("1.5, 2.5");
    assertEquals(
        Arrays.asList(1.5, 2.5),
        provider.getMappings().get("VECTOR").mapValue(rs, "unbracketed_vec", null));

    when(rs.getObject("partial_bracket_vec")).thenReturn("[1.5, 2.5");
    assertThrows(
        NumberFormatException.class,
        () -> provider.getMappings().get("VECTOR").mapValue(rs, "partial_bracket_vec", null));

    when(rs.getObject("null_val_col")).thenReturn(null);
    when(rs.wasNull()).thenReturn(false);
    assertNull(provider.getMappings().get("VECTOR").mapValue(rs, "null_val_col", null));

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
        65535, provider.estimateColumnSize(new SourceColumnType("VARCHAR", new Long[] {}, null)));
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
        20, provider.estimateColumnSize(new SourceColumnType("VECTOR", new Long[] {3L}, null)));
    assertEquals(
        65535, provider.estimateColumnSize(new SourceColumnType("VECTOR", new Long[] {}, null)));
    assertEquals(
        65535,
        provider.estimateColumnSize(new SourceColumnType("UNKNOWN_TYPE", new Long[] {}, null)));
  }
}
