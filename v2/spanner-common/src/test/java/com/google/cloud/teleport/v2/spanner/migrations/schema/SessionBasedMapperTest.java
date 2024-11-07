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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.Before;
import org.junit.Test;

public class SessionBasedMapperTest {

  SessionBasedMapper mapper;

  SessionBasedMapper shardedMapper;

  Ddl ddl;

  Ddl shardedDdl;

  @Before
  public void setup() throws IOException {
    ddl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();

    shardedDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_product_id")
            .string()
            .size(20)
            .endColumn()
            .column("new_user_id")
            .string()
            .size(20)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_product_id")
            .end()
            .endTable()
            .createTable("new_people")
            .column("migration_shard_id")
            .string()
            .size(20)
            .endColumn()
            .column("new_name")
            .string()
            .size(20)
            .endColumn()
            .primaryKey()
            .asc("migration_shard_id")
            .asc("new_name")
            .end()
            .endTable()
            .build();

    String sessionFilePath =
        Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
            .toString();
    this.mapper = new SessionBasedMapper(sessionFilePath, ddl);
    String shardedSessionFilePath =
        Paths.get(Resources.getResource("session-file-sharded.json").getPath()).toString();
    this.shardedMapper = new SessionBasedMapper(shardedSessionFilePath, shardedDdl);
  }

  @Test
  public void testGetSourceTableName() {
    String srcTableName = "new_cart";
    String result = mapper.getSourceTableName("", srcTableName);
    String expectedTableName = "cart";
    assertEquals(expectedTableName, result);

    srcTableName = "new_people";
    result = mapper.getSourceTableName("", srcTableName);
    expectedTableName = "people";
    assertEquals(expectedTableName, result);

    assertThrows(NoSuchElementException.class, () -> mapper.getSourceTableName("", "xyz"));
  }

  @Test
  public void testGetSpannerTableName() {
    String srcTableName = "cart";
    String result = mapper.getSpannerTableName("", srcTableName);
    String expectedTableName = "new_cart";
    assertEquals(expectedTableName, result);

    srcTableName = "people";
    result = mapper.getSpannerTableName("", srcTableName);
    expectedTableName = "new_people";
    assertEquals(expectedTableName, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerTableNameMissingTable() {
    String srcTable = "wrongTableName";
    mapper.getSpannerTableName("", srcTable);
  }

  @Test
  public void testGetSpannerColumnName() {
    String srcTable = "cart";
    String srcColumn = "user_id";
    String result = mapper.getSpannerColumnName("", srcTable, srcColumn);
    String expectedColumn = "new_user_id";
    assertEquals(expectedColumn, result);

    srcTable = "cart";
    srcColumn = "quantity";
    result = mapper.getSpannerColumnName("", srcTable, srcColumn);
    expectedColumn = "new_quantity";
    assertEquals(expectedColumn, result);

    srcTable = "people";
    srcColumn = "name";
    result = mapper.getSpannerColumnName("", srcTable, srcColumn);
    expectedColumn = "new_name";
    assertEquals(expectedColumn, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnNameMissingTable() {
    String srcTable = "wrongTableName";
    String srcColumn = "user_id";
    mapper.getSpannerColumnName("", srcTable, srcColumn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnNameMissingColumn() {
    String srcTable = "cart";
    String srcColumn = "wrongColumn";
    mapper.getSpannerColumnName("", srcTable, srcColumn);
  }

  @Test
  public void testGetSourceColumnName() {
    String spannerTable = "new_cart";
    String spannerColumn = "new_quantity";
    String result = mapper.getSourceColumnName("", spannerTable, spannerColumn);
    String srcColumn = "quantity";
    assertEquals(srcColumn, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSourceColumnNameMissingTable() {
    String spannerTable = "wrongTableName";
    String spannerColumn = "new_quantity";
    mapper.getSourceColumnName("", spannerTable, spannerColumn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSourceColumnNameMissingColumn() {
    String spannerTable = "new_cart";
    String spannerColumn = "wrongColumn";
    mapper.getSourceColumnName("", spannerTable, spannerColumn);
  }

  @Test
  public void testGetSpannerColumnType() {
    String spannerTable = "new_cart";
    String spannerColumn;

    spannerColumn = "new_quantity";
    Type expectedType = Type.int64();
    Type result = mapper.getSpannerColumnType("", spannerTable, spannerColumn);
    assertEquals(expectedType, result);

    spannerColumn = "new_user_id";
    expectedType = Type.string();
    result = mapper.getSpannerColumnType("", spannerTable, spannerColumn);
    assertEquals(expectedType, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnTypeMissingTable() {
    String spannerTable = "wrongTableName";
    String spannerColumn = "new_quantity";
    mapper.getSpannerColumnType("", spannerTable, spannerColumn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnTypeMissingColumn() {
    String spannerTable = "new_cart";
    String spannerColumn = "wrongColumn";
    mapper.getSpannerColumnType("", spannerTable, spannerColumn);
  }

  @Test
  public void testGetSpannerColumns() {
    String spannerTable = "new_cart";
    List<String> expectedColumns = Arrays.asList("new_quantity", "new_user_id");
    List<String> result = mapper.getSpannerColumns("", spannerTable);
    Collections.sort(result);
    Collections.sort(expectedColumns);
    assertEquals(expectedColumns, result);
  }

  @Test
  public void testGetShardIdColumnName() {
    String shardIdCol = shardedMapper.getShardIdColumnName("", "new_people");
    assertEquals("migration_shard_id", shardIdCol);

    assertThrows(
        NoSuchElementException.class,
        () -> shardedMapper.getShardIdColumnName("", "nonexistent_table"));

    String shardedSessionFilePath =
        Paths.get(Resources.getResource("session-file-sharded.json").getPath()).toString();
    Schema erronousSchema = SessionFileReader.read(shardedSessionFilePath);
    Map<String, NameAndCols> spanToId = erronousSchema.getSpannerToID();
    spanToId.put("nonexistent_table", new NameAndCols(null, null));
    erronousSchema.setSpannerToID(spanToId);
    ISchemaMapper erronousMapper = new SessionBasedMapper(erronousSchema, shardedDdl);
    assertThrows(
        NullPointerException.class,
        () -> erronousMapper.getShardIdColumnName("", "nonexistent_table"));
  }

  @Test
  public void testNonShardedGetShardIdColumnName() {
    String cartShardId = mapper.getShardIdColumnName("", "new_cart");
    assertNull(cartShardId);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnsMissingTable() {
    String spannerTable = "WrongTableName";
    List<String> result = mapper.getSpannerColumns("", spannerTable);
  }

  @Test
  public void testValidateSchemaAndDdl() {
    Schema schema =
        SessionFileReader.read(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString());
    SessionBasedMapper.validateSchemaAndDdl(schema, ddl);
  }

  @Test(expected = InputMismatchException.class)
  public void testValidateSchemaAndDdlMismatchTable() {
    Schema schema =
        SessionFileReader.read(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString());
    schema.setToSource(new HashMap<>());
    SessionBasedMapper.validateSchemaAndDdl(schema, ddl);
  }

  @Test(expected = InputMismatchException.class)
  public void testValidateSchemaAndDdlMismatchColumn() {
    Schema schema =
        SessionFileReader.read(
            Paths.get(Resources.getResource("session-file.json").getPath()).toString());
    SessionBasedMapper.validateSchemaAndDdl(schema, ddl);
  }

  @Test
  public void testSourceTablesToMigrate() {
    List<String> sourceTablesToMigrate = mapper.getSourceTablesToMigrate("");
    assertTrue(sourceTablesToMigrate.contains("cart"));
    assertTrue(sourceTablesToMigrate.contains("people"));
    assertEquals(2, sourceTablesToMigrate.size());
  }

  @Test
  public void testIgnoreStrictCheck() {
    // Expected to fail as spanner tables mentioned in session file do not exist
    SessionBasedMapper emptymapper =
        new SessionBasedMapper(
            Paths.get(Resources.getResource("session-file-empty.json").getPath()).toString(),
            ddl,
            false);
    List<String> sourceTablesToMigrate = emptymapper.getSourceTablesToMigrate("");
    assertTrue(sourceTablesToMigrate.isEmpty());
  }

  @Test(expected = InputMismatchException.class)
  public void testSourceTablesToMigrateEmpty() {
    // Expected to fail as spanner tables mentioned in session file do not exist
    SessionBasedMapper emptymapper =
        new SessionBasedMapper(
            Paths.get(Resources.getResource("session-file-empty.json").getPath()).toString(),
            ddl,
            true);
    List<String> sourceTablesToMigrate = emptymapper.getSourceTablesToMigrate("");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSourceTablesToMigrateNamespace() {
    mapper.getSourceTablesToMigrate("test");
  }

  @Test
  public void testGetSyntheticPrimaryKeyColName() {
    // Table with synthetic PK
    String result = mapper.getSyntheticPrimaryKeyColName("", "new_people");
    assertEquals("synth_id", result);

    // Table without synthetic PK
    assertNull(mapper.getSyntheticPrimaryKeyColName("", "new_cart"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSyntheticPrimaryKeyColNameMissingTable() {
    mapper.getSyntheticPrimaryKeyColName("", "nonexistent_table");
  }

  @Test
  public void testColExistsAtSource() {
    assertTrue(mapper.colExistsAtSource("", "new_cart", "new_quantity"));
    assertFalse(mapper.colExistsAtSource("", "new_cart", "abc"));
  }
}
