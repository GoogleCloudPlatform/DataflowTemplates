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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SqlServerTypeMapper}. */
@RunWith(JUnit4.class)
public class SqlServerTypeMapperTest {

  private final SqlServerTypeMapper mapper = new SqlServerTypeMapper();

  private LogicalType map(String type) {
    return mapper.getLogicalType(type, null, null);
  }

  @Test
  public void testBooleanTypes() {
    assertThat(map("bit")).isEqualTo(LogicalType.BOOLEAN);
  }

  @Test
  public void testIntegerTypes() {
    assertThat(map("tinyint")).isEqualTo(LogicalType.INT64);
    assertThat(map("smallint")).isEqualTo(LogicalType.INT64);
    assertThat(map("int")).isEqualTo(LogicalType.INT64);
    assertThat(map("bigint")).isEqualTo(LogicalType.INT64);
    // Identity suffix as reported by the SQL Server JDBC driver.
    assertThat(map("int identity")).isEqualTo(LogicalType.INT64);
    assertThat(map("bigint identity")).isEqualTo(LogicalType.INT64);
  }

  @Test
  public void testFloatTypes() {
    assertThat(map("float")).isEqualTo(LogicalType.FLOAT64);
    assertThat(map("real")).isEqualTo(LogicalType.FLOAT64);
  }

  @Test
  public void testNumericTypes() {
    assertThat(map("decimal")).isEqualTo(LogicalType.NUMERIC);
    assertThat(map("DECIMAL(10,2)")).isEqualTo(LogicalType.NUMERIC);
    assertThat(map("numeric")).isEqualTo(LogicalType.NUMERIC);
    assertThat(map("money")).isEqualTo(LogicalType.NUMERIC);
    assertThat(map("smallmoney")).isEqualTo(LogicalType.NUMERIC);
  }

  @Test
  public void testStringTypes() {
    assertThat(map("char")).isEqualTo(LogicalType.STRING);
    assertThat(map("nchar")).isEqualTo(LogicalType.STRING);
    assertThat(map("varchar")).isEqualTo(LogicalType.STRING);
    assertThat(map("nvarchar")).isEqualTo(LogicalType.STRING);
    assertThat(map("text")).isEqualTo(LogicalType.STRING);
    assertThat(map("ntext")).isEqualTo(LogicalType.STRING);
    assertThat(map("xml")).isEqualTo(LogicalType.STRING);
    assertThat(map("sysname")).isEqualTo(LogicalType.STRING);
  }

  @Test
  public void testBinaryTypes() {
    assertThat(map("binary")).isEqualTo(LogicalType.BYTES);
    assertThat(map("varbinary")).isEqualTo(LogicalType.BYTES);
    assertThat(map("image")).isEqualTo(LogicalType.BYTES);
    assertThat(map("timestamp")).isEqualTo(LogicalType.BYTES);
    assertThat(map("rowversion")).isEqualTo(LogicalType.BYTES);
  }

  @Test
  public void testDateTimeTypes() {
    assertThat(map("date")).isEqualTo(LogicalType.DATE);
    assertThat(map("time")).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(map("datetime")).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(map("datetime2")).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(map("smalldatetime")).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(map("datetimeoffset")).isEqualTo(LogicalType.TIMESTAMP);
  }

  @Test
  public void testUuid() {
    assertThat(map("uniqueidentifier")).isEqualTo(LogicalType.UUID);
  }

  @Test
  public void testUnknownDefaultsToString() {
    assertThat(map("geography")).isEqualTo(LogicalType.STRING);
    assertThat(map("hierarchyid")).isEqualTo(LogicalType.STRING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTypeThrows() {
    map(null);
  }
}
