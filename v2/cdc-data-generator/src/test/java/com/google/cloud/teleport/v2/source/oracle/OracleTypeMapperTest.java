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
package com.google.cloud.teleport.v2.source.oracle;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleTypeMapperTest {

  private final OracleTypeMapper mapper = new OracleTypeMapper();

  @Test
  public void testGetLogicalType_allTypes() {
    // Character / String types
    assertThat(mapper.getLogicalType("VARCHAR2", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("VARCHAR2(2000)", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("VARCHAR", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("CHAR", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("CHAR(255)", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("CHARACTER", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NVARCHAR2(2000)", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NCHAR(255)", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NCHAR VARYING", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NATIONAL CHARACTER", null, null))
        .isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NATIONAL CHAR", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NATIONAL CHARACTER VARYING", null, null))
        .isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NATIONAL CHAR VARYING", null, null))
        .isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("CLOB", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("NCLOB", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("LONG", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("ROWID", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("UROWID(4000)", null, null)).isEqualTo(LogicalType.STRING);

    // Numeric types
    assertThat(mapper.getLogicalType("NUMBER", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("NUMBER(38, 0)", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("NUMERIC(10, 2)", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("DECIMAL(10, 2)", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("DEC", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("INTEGER", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("INT", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("SMALLINT", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("FLOAT", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("FLOAT(126)", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("DOUBLE PRECISION", null, null))
        .isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("REAL", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("BINARY_FLOAT", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("BINARY_DOUBLE", null, null)).isEqualTo(LogicalType.FLOAT64);

    // Temporal types
    assertThat(mapper.getLogicalType("DATE", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("TIMESTAMP", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("TIMESTAMP(6)", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("TIMESTAMP(6) WITH TIME ZONE", null, null))
        .isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("TIMESTAMP(6) WITH LOCAL TIME ZONE", null, null))
        .isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("INTERVAL YEAR(2) TO MONTH", null, null))
        .isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("INTERVAL DAY(2) TO SECOND(6)", null, null))
        .isEqualTo(LogicalType.STRING);

    // Binary / LOB types
    assertThat(mapper.getLogicalType("RAW(2000)", null, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("LONG RAW", null, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("BLOB", null, null)).isEqualTo(LogicalType.BYTES);

    // Boolean & JSON
    assertThat(mapper.getLogicalType("BOOLEAN", null, null)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("JSON", null, null)).isEqualTo(LogicalType.JSON);
    assertThat(mapper.getLogicalType("XMLTYPE", null, null)).isEqualTo(LogicalType.STRING);

    assertThat(mapper.getLogicalType("UNKNOWN_TYPE", null, null)).isEqualTo(LogicalType.STRING);
    assertThrows(IllegalArgumentException.class, () -> mapper.getLogicalType(null, null, null));
  }
}
