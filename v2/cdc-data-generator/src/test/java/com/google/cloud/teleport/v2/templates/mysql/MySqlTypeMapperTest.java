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
package com.google.cloud.teleport.v2.templates.mysql;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MySqlTypeMapperTest {

  private final MySqlTypeMapper mapper = new MySqlTypeMapper();

  @Test
  public void testGetLogicalType_allTypes() {
    assertThat(mapper.getLogicalType("BIT", null, null)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("BOOLEAN", null, null)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("TINYINT", null, 1L)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("TINYINT(1)", null, 1L)).isEqualTo(LogicalType.BOOLEAN);
    assertThat(mapper.getLogicalType("TINYINT", null, 2L)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("TINYINT(2)", null, 2L)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("smallint", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("MEDIUMINT", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("INT", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("BIGINT", null, null)).isEqualTo(LogicalType.INT64);
    assertThat(mapper.getLogicalType("FLOAT", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("DOUBLE", null, null)).isEqualTo(LogicalType.FLOAT64);
    assertThat(mapper.getLogicalType("DECIMAL", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("DECIMAL(10,2)", null, null)).isEqualTo(LogicalType.NUMERIC);
    assertThat(mapper.getLogicalType("CHAR", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("VARCHAR(255)", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("TEXT", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("ENUM('a','b')", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType("BINARY", null, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("BLOB", null, null)).isEqualTo(LogicalType.BYTES);
    assertThat(mapper.getLogicalType("DATE", null, null)).isEqualTo(LogicalType.DATE);
    assertThat(mapper.getLogicalType("TIME", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("TIMESTAMP", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("DATETIME", null, null)).isEqualTo(LogicalType.TIMESTAMP);
    assertThat(mapper.getLogicalType("JSON", null, null)).isEqualTo(LogicalType.JSON);
    assertThat(mapper.getLogicalType("UNKNOWN_TYPE", null, null)).isEqualTo(LogicalType.STRING);
    assertThat(mapper.getLogicalType(null, null, null)).isEqualTo(LogicalType.STRING);
  }
}
