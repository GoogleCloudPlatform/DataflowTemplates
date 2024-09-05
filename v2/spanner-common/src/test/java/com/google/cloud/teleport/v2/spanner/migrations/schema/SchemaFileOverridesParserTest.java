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
    Path schemaOverridesFile = Paths.get(Resources.getResource("schema-overrides-tables.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    assertEquals(3, schemaFileOverridesParser.schemaFileOverride.getRenamedTables().size());
    assertEquals("Vocalists", schemaFileOverridesParser.getTableOverrideOrDefault("Singers"));
    assertEquals("Records", schemaFileOverridesParser.getTableOverrideOrDefault("Albums"));
    assertEquals("World", schemaFileOverridesParser.getTableOverrideOrDefault("Hello"));
  }

  @Test
  public void testGetColumnOverride() {
    Path schemaOverridesFile = Paths.get(Resources.getResource("schema-overrides-columns.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    Pair<String, String> result1 =  schemaFileOverridesParser.getColumnOverrideOrDefault("Singers", "SingerName");
    Pair<String, String> result2 =  schemaFileOverridesParser.getColumnOverrideOrDefault("Albums", "AlbumName");
    assertEquals(2, schemaFileOverridesParser.schemaFileOverride.getRenamedColumns().size());
    assertEquals("TalentName", result1.getRight());
    assertEquals("RecordName", result2.getRight());
  }

  @Test
  public void testGetTableAndColumnOverride() {
    Path schemaOverridesFile = Paths.get(Resources.getResource("schema-overrides-cols-tables.json").getPath());
    schemaFileOverridesParser = new SchemaFileOverridesParser(schemaOverridesFile.toString());
    Pair<String, String> result1 =  schemaFileOverridesParser.getColumnOverrideOrDefault("Singers", "SingerName");
    Pair<String, String> result2 =  schemaFileOverridesParser.getColumnOverrideOrDefault("Albums", "AlbumName");
    assertEquals(3, schemaFileOverridesParser.schemaFileOverride.getRenamedTables().size());
    assertEquals("Vocalists", schemaFileOverridesParser.getTableOverrideOrDefault("Singers"));
    assertEquals("Records", schemaFileOverridesParser.getTableOverrideOrDefault("Albums"));
    assertEquals("World", schemaFileOverridesParser.getTableOverrideOrDefault("Hello"));
    assertEquals(2, schemaFileOverridesParser.schemaFileOverride.getRenamedColumns().size());
    assertEquals("TalentName", result1.getRight());
    assertEquals("RecordName", result2.getRight());
  }
}