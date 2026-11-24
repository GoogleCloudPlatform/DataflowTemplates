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
package com.google.cloud.teleport.v2.source.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SqlServerTypeMapper}; one entry per matrix type (or its JDBC spelling). */
@RunWith(JUnit4.class)
public class SqlServerTypeMapperTest {

  private final SqlServerTypeMapper mapper = new SqlServerTypeMapper();

  private static final Map<String, LogicalType> EXPECTED =
      ImmutableMap.<String, LogicalType>builder()
          // Integer
          .put("tinyint", LogicalType.INT64)
          .put("smallint", LogicalType.INT64)
          .put("int", LogicalType.INT64)
          .put("INTEGER", LogicalType.INT64)
          .put("bigint", LogicalType.INT64)
          .put("int identity", LogicalType.INT64)
          .put("bigint identity", LogicalType.INT64)
          .put("tinyint identity", LogicalType.INT64)
          // Exact numeric
          .put("decimal", LogicalType.NUMERIC)
          .put("DEC", LogicalType.NUMERIC)
          .put("numeric", LogicalType.NUMERIC)
          .put("decimal() identity", LogicalType.NUMERIC)
          .put("numeric(18,4)", LogicalType.NUMERIC)
          .put("money", LogicalType.NUMERIC)
          .put("smallmoney", LogicalType.NUMERIC)
          // Approximate numeric
          .put("float", LogicalType.FLOAT64)
          .put("FLOAT(53)", LogicalType.FLOAT64)
          .put("DOUBLE PRECISION", LogicalType.FLOAT64)
          .put("real", LogicalType.FLOAT64)
          .put("FLOAT(24)", LogicalType.FLOAT64)
          // Character
          .put("char", LogicalType.STRING)
          .put("CHARACTER", LogicalType.STRING)
          .put("varchar", LogicalType.STRING)
          .put("CHAR VARYING", LogicalType.STRING)
          .put("CHARACTER VARYING", LogicalType.STRING)
          .put("VARCHAR(MAX)", LogicalType.STRING)
          .put("text", LogicalType.STRING)
          .put("nchar", LogicalType.STRING)
          .put("NATIONAL CHARACTER", LogicalType.STRING)
          .put("NATIONAL CHAR", LogicalType.STRING)
          .put("nvarchar", LogicalType.STRING)
          .put("NATIONAL CHARACTER VARYING", LogicalType.STRING)
          .put("NATIONAL CHAR VARYING", LogicalType.STRING)
          .put("NVARCHAR(MAX)", LogicalType.STRING)
          .put("ntext", LogicalType.STRING)
          .put("NATIONAL TEXT", LogicalType.STRING)
          .put("sysname", LogicalType.STRING)
          // Binary
          .put("binary", LogicalType.BYTES)
          .put("varbinary", LogicalType.BYTES)
          .put("BINARY VARYING", LogicalType.BYTES)
          .put("VARBINARY(MAX)", LogicalType.BYTES)
          .put("image", LogicalType.BYTES)
          .put("rowversion", LogicalType.BYTES)
          .put("timestamp", LogicalType.BYTES)
          // Date / time
          .put("date", LogicalType.DATE)
          .put("time", LogicalType.TIMESTAMP)
          .put("datetime2", LogicalType.TIMESTAMP)
          .put("datetimeoffset", LogicalType.TIMESTAMP)
          .put("datetime", LogicalType.TIMESTAMP)
          .put("smalldatetime", LogicalType.TIMESTAMP)
          // Boolean, JSON, UUID
          .put("bit", LogicalType.BOOLEAN)
          .put("json", LogicalType.JSON)
          .put("uniqueidentifier", LogicalType.UUID)
          // String with override in the fetcher
          .put("xml", LogicalType.STRING)
          .put("geometry", LogicalType.STRING)
          .put("geography", LogicalType.STRING)
          .put("hierarchyid", LogicalType.STRING)
          .put("sql_variant", LogicalType.STRING)
          // Unknown (skipped by the fetcher) default to STRING
          .put("vector", LogicalType.STRING)
          .put("my_alias_type", LogicalType.STRING)
          .build();

  @Test
  public void testGetLogicalType_allMatrixTypes() {
    for (Map.Entry<String, LogicalType> entry : EXPECTED.entrySet()) {
      assertWithMessage("type %s", entry.getKey())
          .that(mapper.getLogicalType(entry.getKey(), null, null))
          .isEqualTo(entry.getValue());
    }
  }

  @Test
  public void testGetLogicalType_nullThrows() {
    assertThrows(IllegalArgumentException.class, () -> mapper.getLogicalType(null, null, null));
  }

  @Test
  public void testNormalizeTypeName() {
    assertThat(SqlServerTypeMapper.normalizeTypeName("int identity")).isEqualTo("INT");
    assertThat(SqlServerTypeMapper.normalizeTypeName("decimal() identity")).isEqualTo("DECIMAL");
    assertThat(SqlServerTypeMapper.normalizeTypeName(" varchar(max) ")).isEqualTo("VARCHAR");
    assertThat(SqlServerTypeMapper.normalizeTypeName("national  char   varying(10)"))
        .isEqualTo("NATIONAL CHAR VARYING");
    assertThat(SqlServerTypeMapper.normalizeTypeName("vector(3, float16)")).isEqualTo("VECTOR");
  }

  @Test
  public void testIsIdentityTypeName() {
    assertThat(SqlServerTypeMapper.isIdentityTypeName("int identity")).isTrue();
    assertThat(SqlServerTypeMapper.isIdentityTypeName("BIGINT  IDENTITY")).isTrue();
    assertThat(SqlServerTypeMapper.isIdentityTypeName("int")).isFalse();
    assertThat(SqlServerTypeMapper.isIdentityTypeName(null)).isFalse();
  }
}
