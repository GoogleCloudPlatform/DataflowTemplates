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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class SourceTableTest {

  @Test
  public void testConstructorAndGetters() {
    String name = "test_table";
    String schema = "test_schema";
    String[] colIds = new String[] {"c1", "c2"};
    Map<String, SourceColumnDefinition> colDefs = new HashMap<>();
    colDefs.put("c1", new SourceColumnDefinition("col1", null));
    colDefs.put("c2", new SourceColumnDefinition("col2", null));
    ColumnPK[] primaryKeys = new ColumnPK[] {new ColumnPK("c1", 1)};

    SourceTable table = new SourceTable(name, schema, colIds, colDefs, primaryKeys);

    assertEquals(name, table.getName());
    assertEquals(schema, table.getSchema());
    assertEquals(colIds, table.getColIds());
    assertEquals(colDefs, table.getColDefs());
    assertEquals(primaryKeys, table.getPrimaryKeys());
  }

  @Test
  public void testConstructor_NullArrays() {
    SourceTable table = new SourceTable("name", "schema", null, null, null);
    assertNotNull(table.getColIds());
    assertEquals(0, table.getColIds().length);
    assertNotNull(table.getColDefs());
    assertTrue(table.getColDefs().isEmpty());
    assertNull(table.getPrimaryKeys());
  }

  @Test
  public void testGetPrimaryKeySet() {
    String[] colIds = new String[] {"c1", "c2"};
    Map<String, SourceColumnDefinition> colDefs = new HashMap<>();
    colDefs.put("c1", new SourceColumnDefinition("col1", null));
    colDefs.put("c2", new SourceColumnDefinition("col2", null));
    ColumnPK[] primaryKeys = new ColumnPK[] {new ColumnPK("c1", 1)};

    SourceTable table = new SourceTable("name", "schema", colIds, colDefs, primaryKeys);
    Set<String> pkSet = table.getPrimaryKeySet();

    assertEquals(1, pkSet.size());
    assertTrue(pkSet.contains("col1"));
  }

  @Test
  public void testGetPrimaryKeySet_NullPrimaryKeys() {
    SourceTable table = new SourceTable("name", "schema", null, null, null);
    Set<String> pkSet = table.getPrimaryKeySet();
    assertTrue(pkSet.isEmpty());
  }

  @Test
  public void testToString() {
    String[] colIds = new String[] {"c1"};
    Map<String, SourceColumnDefinition> colDefs = new HashMap<>();
    colDefs.put("c1", new SourceColumnDefinition("col1", null));
    ColumnPK[] primaryKeys = new ColumnPK[] {new ColumnPK("c1", 1)};

    SourceTable table = new SourceTable("name", "schema", colIds, colDefs, primaryKeys);
    String str = table.toString();

    assertTrue(str.contains("name"));
    assertTrue(str.contains("schema"));
    assertTrue(str.contains("c1"));
  }

  @Test
  public void testEqualsAndHashCode() {
    String[] colIds = new String[] {"c1"};
    Map<String, SourceColumnDefinition> colDefs = new HashMap<>();
    colDefs.put("c1", new SourceColumnDefinition("col1", null));
    ColumnPK[] primaryKeys = new ColumnPK[] {new ColumnPK("c1", 1)};

    SourceTable table1 = new SourceTable("name", "schema", colIds, colDefs, primaryKeys);
    SourceTable table2 = new SourceTable("name", "schema", colIds, colDefs, primaryKeys);
    SourceTable table3 = new SourceTable("different", "schema", colIds, colDefs, primaryKeys);

    assertTrue(table1.equals(table1));
    assertTrue(table1.equals(table2));
    assertFalse(table1.equals(table3));
    assertFalse(table1.equals(null));
    assertFalse(table1.equals("string"));

    assertEquals(table1.hashCode(), table2.hashCode());
    assertNotEquals(table1.hashCode(), table3.hashCode());
  }
}
