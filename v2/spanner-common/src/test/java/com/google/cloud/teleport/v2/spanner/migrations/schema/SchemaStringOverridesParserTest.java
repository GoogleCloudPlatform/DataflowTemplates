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

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class SchemaStringOverridesParserTest {

  SchemaStringOverridesParser schemaStringOverridesParser;

  @Test
  public void testGetTableOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put(
        "tableOverrides", "[{Singers, Vocalists}, {Albums, Records},{Hello, World}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
    assertEquals(3, schemaStringOverridesParser.tableNameOverrides.keySet().size());
    assertEquals("Vocalists", schemaStringOverridesParser.getTableOverride("Singers"));
    assertEquals("Records", schemaStringOverridesParser.getTableOverride("Albums"));
    assertEquals("World", schemaStringOverridesParser.getTableOverride("Hello"));
  }

  @Test
  public void testGetColumnOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put(
        "columnOverrides",
        "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
    Pair<String, String> result1 =
        schemaStringOverridesParser.getColumnOverride("Singers", "SingerName");
    Pair<String, String> result2 =
        schemaStringOverridesParser.getColumnOverride("Albums", "AlbumName");
    assertEquals(2, schemaStringOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("TalentName", result1.getRight());
    assertEquals("RecordName", result2.getRight());
  }

  @Test
  public void testGetTableAndColumnOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put(
        "tableOverrides", "[{Singers, Vocalists}, {Albums, Records},{Hello, World}]");
    userOptionsOverrides.put(
        "columnOverrides",
        "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
    String sourceTableName = "Singers";
    String sourceColumnName = "SingerName";
    String tableResult = schemaStringOverridesParser.getTableOverride(sourceTableName);
    Pair<String, String> columnResult =
        schemaStringOverridesParser.getColumnOverride(sourceTableName, sourceColumnName);
    assertEquals(3, schemaStringOverridesParser.tableNameOverrides.keySet().size());
    assertEquals("Vocalists", tableResult);
    assertEquals(2, schemaStringOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("TalentName", columnResult.getRight());
  }

  @Test
  public void testGetDefaultTableOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers, Vocalists}, {Albums, Records}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
    String sourceTableName = "Labels";
    String result = schemaStringOverridesParser.getTableOverride(sourceTableName);
    assertEquals(sourceTableName, result);
  }

  @Test
  public void testGetDefaultColumnOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put(
        "columnOverrides",
        "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
    String sourceTableName = "Labels";
    String sourceColumnName = "Owners";
    Pair<String, String> result =
        schemaStringOverridesParser.getColumnOverride(sourceTableName, sourceColumnName);
    assertEquals(2, schemaStringOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("Labels", result.getLeft());
    assertEquals("Owners", result.getRight());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedGetTableOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers}}, {Albums, Records}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedGetColumnOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put(
        "columnOverrides", "[{Singers, Vocalists}, {Albums.AlbumName, Records.RecordName}]");
    schemaStringOverridesParser = new SchemaStringOverridesParser(userOptionsOverrides);
  }
}
