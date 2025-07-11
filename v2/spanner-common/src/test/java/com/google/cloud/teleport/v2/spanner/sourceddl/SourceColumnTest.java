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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SourceColumnTest {

  @Test
  public void testBuilderAndProperties() {
    SourceColumn column =
        SourceColumn.builder(SourceDatabaseType.MYSQL)
            .name("id")
            .type("INT")
            .isNullable(false)
            .isPrimaryKey(true)
            .size(10L)
            .precision(5)
            .scale(0)
            .columnOptions(ImmutableList.of("AUTO_INCREMENT"))
            .build();
    assertEquals("id", column.name());
    assertEquals("INT", column.type());
    assertEquals(false, column.isNullable());
    assertEquals(true, column.isPrimaryKey());
    assertEquals(Long.valueOf(10L), column.size());
    assertEquals(Integer.valueOf(5), column.precision());
    assertEquals(Integer.valueOf(0), column.scale());
    assertEquals(ImmutableList.of("AUTO_INCREMENT"), column.columnOptions());
    assertEquals(SourceDatabaseType.MYSQL, column.sourceType());
  }

  @Test
  public void testToBuilder() {
    SourceColumn column =
        SourceColumn.builder(SourceDatabaseType.POSTGRESQL).name("name").type("VARCHAR").build();
    SourceColumn copy = column.toBuilder().isNullable(false).build();
    assertEquals(column.name(), copy.name());
    assertEquals(column.type(), copy.type());
    assertEquals(false, copy.isNullable());
  }

  @Test
  public void testNullables() {
    SourceColumn column =
        SourceColumn.builder(SourceDatabaseType.CASSANDRA).name("col").type("TEXT").build();
    assertNull(column.size());
    assertNull(column.precision());
    assertNull(column.scale());
  }
}
