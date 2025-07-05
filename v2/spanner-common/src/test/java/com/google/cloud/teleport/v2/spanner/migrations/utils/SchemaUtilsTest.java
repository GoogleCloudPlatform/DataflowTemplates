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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.io.Resources;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class SchemaUtilsTest {

  @Test
  public void testBuildDdlFromSessionFile_valid() throws Exception {
    Path sessionFile = Paths.get(Resources.getResource("session-file.json").toURI());
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(sessionFile.toString());
    // Basic assertions: table names and column names
    assertTrue(ddl.table("new_cart") != null);
    assertTrue(ddl.table("new_people") != null);
    assertEquals("new_cart", ddl.table("new_cart").name());
    assertEquals("new_people", ddl.table("new_people").name());
    assertTrue(
        ddl.table("new_cart").columns().stream().anyMatch(c -> c.name().equals("new_product_id")));
    assertTrue(
        ddl.table("new_cart").columns().stream().anyMatch(c -> c.name().equals("new_quantity")));
    assertTrue(
        ddl.table("new_cart").columns().stream().anyMatch(c -> c.name().equals("new_user_id")));

    // Assert types
    assertEquals(
        Type.string(),
        ddl.table("new_cart").columns().stream()
            .filter(c -> c.name().equals("new_product_id"))
            .findFirst()
            .get()
            .type());
    assertEquals(
        Type.int64(),
        ddl.table("new_cart").columns().stream()
            .filter(c -> c.name().equals("new_quantity"))
            .findFirst()
            .get()
            .type());
    assertEquals(
        Type.string(),
        ddl.table("new_cart").columns().stream()
            .filter(c -> c.name().equals("new_user_id"))
            .findFirst()
            .get()
            .type());
  }

  @Test
  public void testBuildDdlFromSessionFile_empty() throws Exception {
    Path sessionFile = Paths.get(Resources.getResource("session-file-empty.json").toURI());
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(sessionFile.toString());
    // Should have no tables
    assertTrue(ddl.allTables().isEmpty());
  }

  @Test(expected = RuntimeException.class)
  public void testBuildDdlFromSessionFile_nonexistent() {
    SchemaUtils.buildDdlFromSessionFile("/nonexistent/path/to/session-file.json");
  }

  @Test
  public void testBuildSourceSchemaFromSessionFile_valid() throws Exception {
    Path sessionFile = Paths.get(Resources.getResource("session-file.json").toURI());
    SourceSchema schema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile.toString());
    assertEquals("cart", schema.tables().get("cart").name());
    assertEquals("people", schema.tables().get("people").name());
    assertEquals("my_schema", schema.tables().get("cart").schema());
    assertTrue(
        schema.tables().get("cart").columns().stream()
            .anyMatch(c -> c.name().equals("product_id")));
    assertTrue(
        schema.tables().get("cart").columns().stream().anyMatch(c -> c.name().equals("quantity")));
    assertTrue(
        schema.tables().get("cart").columns().stream().anyMatch(c -> c.name().equals("user_id")));

    // Assert types
    assertEquals(
        "varchar",
        schema.tables().get("cart").columns().stream()
            .filter(c -> c.name().equals("product_id"))
            .findFirst()
            .get()
            .type());
    assertEquals(
        "bigint",
        schema.tables().get("cart").columns().stream()
            .filter(c -> c.name().equals("quantity"))
            .findFirst()
            .get()
            .type());
    assertEquals(
        "varchar",
        schema.tables().get("cart").columns().stream()
            .filter(c -> c.name().equals("user_id"))
            .findFirst()
            .get()
            .type());
  }

  @Test
  public void testBuildSourceSchemaFromSessionFile_empty() throws Exception {
    Path sessionFile = Paths.get(Resources.getResource("session-file-empty.json").toURI());
    SourceSchema schema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile.toString());
    // Should have no tables
    assertTrue(schema.tables().isEmpty());
  }

  @Test(expected = RuntimeException.class)
  public void testBuildSourceSchemaFromSessionFile_nonexistent() {
    SchemaUtils.buildSourceSchemaFromSessionFile("/nonexistent/path/to/session-file.json");
  }
}
