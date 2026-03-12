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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTypeMapperTest {

  private final SpannerTypeMapper mapper = new SpannerTypeMapper();

  @Test
  public void testGetLogicalType_googleSql() {
    Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;
    assertThat(mapper.getLogicalType("BOOL", dialect, null)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("INT64", dialect, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("FLOAT64", dialect, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("STRING(MAX)", dialect, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("BYTES(1024)", dialect, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("DATE", dialect, null)).isEqualTo(LogicalType.DATE);
    assertThat(mapper.getLogicalType("TIMESTAMP", dialect, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("NUMERIC", dialect, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("JSON", dialect, null)).isEqualTo(LogicalType.JSON);
    assertThat(mapper.getLogicalType("ARRAY<INT64>", dialect, null))
        .isEqualTo(LogicalType.STRING); // Default
    assertThat(mapper.getLogicalType(null, dialect, null)).isEqualTo(LogicalType.STRING);
  }

  @Test
  public void testGetLogicalType_postgreSql() {
    Dialect dialect = Dialect.POSTGRESQL;
    assertThat(mapper.getLogicalType("boolean", dialect, null)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("bigint", dialect, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("double precision", dialect, null))
        .isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("varchar(255)", dialect, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("text", dialect, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("bytea", dialect, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("date", dialect, null)).isEqualTo(LogicalType.DATE);
    assertThat(mapper.getLogicalType("timestamp with time zone", dialect, null))
        .isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("numeric(10,2)", dialect, null))
        .isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("jsonb", dialect, null)).isEqualTo(LogicalType.JSON);
    assertThat(mapper.getLogicalType(null, dialect, null)).isEqualTo(LogicalType.STRING);
  }

  @Test
  public void testGetLogicalType_invalidDialect() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> mapper.getLogicalType("INT64", "not a dialect", null));
    assertThat(exception).hasMessageThat().contains("Expected com.google.cloud.spanner.Dialect");

    exception =
        assertThrows(
            IllegalArgumentException.class, () -> mapper.getLogicalType("INT64", null, null));
    assertThat(exception).hasMessageThat().contains("Expected com.google.cloud.spanner.Dialect");
  }
}
