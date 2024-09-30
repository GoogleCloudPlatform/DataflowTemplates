/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static junit.framework.TestCase.assertEquals;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class SchemaFileOverridesParserTest {

  SchemaFileOverridesParser schemaFileOverridesParser;

  @Test
  public void testGetTableOverride() {
    Path schemaOverridesFile =
        Paths.get(Resources.getResource("schema-overrides-tables.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    assertEquals(3, schemaFileOverridesParser.schemaFileOverride.getRenamedTables().size());
    assertEquals("Vocalists", schemaFileOverridesParser.getTableOverride("Singers"));
    assertEquals("Records", schemaFileOverridesParser.getTableOverride("Albums"));
    assertEquals("World", schemaFileOverridesParser.getTableOverride("Hello"));
  }

  @Test
  public void testGetColumnOverride() {
    Path schemaOverridesFile =
        Paths.get(Resources.getResource("schema-overrides-columns.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    Pair<String, String> result1 =
        schemaFileOverridesParser.getColumnOverride("Singers", "SingerName");
    Pair<String, String> result2 =
        schemaFileOverridesParser.getColumnOverride("Albums", "AlbumName");
    assertEquals(2, schemaFileOverridesParser.schemaFileOverride.getRenamedColumnTupleMap().size());
    assertEquals("TalentName", result1.getRight());
    assertEquals("RecordName", result2.getRight());
  }

  @Test
  public void testGetTableAndColumnOverride() {
    Path schemaOverridesFile =
        Paths.get(Resources.getResource("schema-overrides-cols-tables.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    Pair<String, String> result1 =
        schemaFileOverridesParser.getColumnOverride("Singers", "SingerName");
    Pair<String, String> result2 =
        schemaFileOverridesParser.getColumnOverride("Albums", "AlbumName");
    assertEquals(3, schemaFileOverridesParser.schemaFileOverride.getRenamedTables().size());
    assertEquals("Vocalists", schemaFileOverridesParser.getTableOverride("Singers"));
    assertEquals("Records", schemaFileOverridesParser.getTableOverride("Albums"));
    assertEquals("World", schemaFileOverridesParser.getTableOverride("Hello"));
    assertEquals(2, schemaFileOverridesParser.schemaFileOverride.getRenamedColumnTupleMap().size());
    assertEquals("TalentName", result1.getRight());
    assertEquals("RecordName", result2.getRight());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedSchemaOverridesFile() {
    Path schemaOverridesFile =
        Paths.get(Resources.getResource("schema-overrides-malformed.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
  }
}
