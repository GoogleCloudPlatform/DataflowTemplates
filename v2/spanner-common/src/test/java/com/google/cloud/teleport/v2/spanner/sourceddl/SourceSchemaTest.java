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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class SourceSchemaTest {

  @Test
  public void testBuilderAndProperties() {
    SourceTable table = SourceTable.builder(SourceDatabaseType.MYSQL).name("users").build();
    SourceSchema schema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("users", table))
            .version("1.0")
            .build();
    assertEquals("testdb", schema.databaseName());
    assertEquals(SourceDatabaseType.MYSQL, schema.sourceType());
    assertEquals(1, schema.tables().size());
    assertEquals(table, schema.tables().get("users"));
    assertEquals("1.0", schema.version());
  }

  @Test
  public void testToBuilder() {
    SourceSchema schema =
        SourceSchema.builder(SourceDatabaseType.POSTGRESQL).databaseName("db").build();
    SourceSchema copy = schema.toBuilder().version("2.0").build();
    assertEquals(schema.databaseName(), copy.databaseName());
    assertEquals(schema.sourceType(), copy.sourceType());
    assertEquals("2.0", copy.version());
  }

  @Test
  public void testNullables() {
    SourceSchema schema =
        SourceSchema.builder(SourceDatabaseType.CASSANDRA).databaseName("ks").build();
    assertNull(schema.version());
  }
}
