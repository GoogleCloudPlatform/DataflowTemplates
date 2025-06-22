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

public class SourceTableTest {

  @Test
  public void testBuilderAndProperties() {
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("users")
            .schema("public")
            .columns(
                ImmutableList.of(
                    SourceColumn.builder(SourceDatabaseType.MYSQL)
                        .name("id")
                        .type("INT")
                        .isNullable(false)
                        .isPrimaryKey(true)
                        .columnOptions(ImmutableList.of())
                        .build()))
            .primaryKeyColumns(ImmutableList.of("id"))
            .comment("User table")
            .build();

    assertEquals("users", table.name());
    assertEquals("public", table.schema());
    assertEquals(1, table.columns().size());
    assertEquals("id", table.columns().get(0).name());
    assertEquals(ImmutableList.of("id"), table.primaryKeyColumns());
    assertEquals(SourceDatabaseType.MYSQL, table.sourceType());
    assertEquals("User table", table.comment());
  }

  @Test
  public void testToBuilder() {
    SourceTable table = SourceTable.builder(SourceDatabaseType.POSTGRESQL).name("accounts").build();
    SourceTable copy = table.toBuilder().comment("Accounts table").build();
    assertEquals(table.name(), copy.name());
    assertEquals(table.sourceType(), copy.sourceType());
    assertEquals("Accounts table", copy.comment());
  }

  @Test
  public void testNullables() {
    SourceTable table = SourceTable.builder(SourceDatabaseType.CASSANDRA).name("test").build();
    assertNull(table.schema());
    assertNull(table.comment());
  }
}
