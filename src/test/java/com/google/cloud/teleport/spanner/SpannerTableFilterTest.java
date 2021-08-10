/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.SpannerTableFilter.getFilteredTables;
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A set of unit tests testing the functionality of the SpannerHelperFilterClass and it's
 * getFilteredTables() method.
 */
@RunWith(JUnit4.class)
public final class SpannerTableFilterTest {
  private final String allTypesTable = "AllTYPES";
  private final String ordersTable = "Orders";
  private final String usersTable = "Users";
  private final String childTable = "Child";
  private final String refOneTable = "Ref1";
  private final String refTwoTable = "Ref2";
  private final String tableA = "table_a";
  private final String tableB = "table_b";
  private final String tableC = "table_c";

  /* Basic test that provides no table names to getFilteredTables() */
  @Test
  public void emptyTableSelection_selectsAllDbTables() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("Orders")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();

    List<String> filteredTables =
        getFilteredTables(ddl, Collections.emptyList()).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(allTypesTable, ordersTable, usersTable);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void individualTableSelection_selectsOnlyChosenTable() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(usersTable)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(usersTable);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void basicInterleavedTableFilterSelection_selectsChosenAndParentTables() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("AllTYPES")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(allTypesTable)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(allTypesTable, usersTable);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void filterWithAllAncestorsSelection_selectsChosenTableWithAllParents() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_c")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("table_b")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("table_c")
            .onDeleteCascade()
            .endTable()
            .createTable("table_a")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("table_b")
            .onDeleteCascade()
            .endTable()
            .build();

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(tableA)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(tableA, tableB, tableC);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void filterWithPartialAncestorsSelection_selectsChosenAndParentTable() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_c")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("table_b")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("table_c")
            .onDeleteCascade()
            .endTable()
            .createTable("table_a")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("id")
            .end()
            .interleaveInParent("table_b")
            .onDeleteCascade()
            .endTable()
            .build();

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(tableB)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(tableB, tableC);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void basicForeignKeyTableFilterSelection_selectsChosenAndReferencedTable()
      throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Ref1")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("Child")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `Child` would have a foreign key constraint
    // referencing `Ref1`)
    ddl.addNewReferencedTable("Child", "Ref1");

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(childTable)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(childTable, refOneTable);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void multipleForeignKeyTableFilterSelection_selectsChosenAndAllReferencedTables()
      throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Ref1")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("Ref2")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("Child")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `Child` would have two foreign key constraints
    // referencing `Ref1` and `Ref2` )
    ddl.addNewReferencedTable("Child", "Ref1");
    ddl.addNewReferencedTable("Child", "Ref2");

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(childTable)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(childTable, refOneTable, refTwoTable);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void circularForeignKeyTableFilterSelection_selectsBothTables() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `table_a` would have a foreign key constraint
    // referencing `table_b` and vice versa )
    ddl.addNewReferencedTable("table_a", "table_b");
    ddl.addNewReferencedTable("table_b", "table_a");

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(tableB)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(tableA, tableB);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }

  @Test
  public void foreignKeyAndParentTableFilterSelection_selectsAllNecessaryTables() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("table_a")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .end()
            .endTable()
            .createTable("table_b")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .endTable()
            .createTable("table_c")
            .column("id1")
            .int64()
            .endColumn()
            .column("id2")
            .int64()
            .endColumn()
            .column("id3")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id1")
            .asc("id2")
            .asc("id3")
            .end()
            .interleaveInParent("table_b")
            .endTable()
            .build();

    // Add to referencedTable field (i.e. `table_c` would have a foreign key constraint
    // referencing `table_a` )
    ddl.addNewReferencedTable("table_c", "table_a");

    List<String> filteredTables =
        getFilteredTables(ddl, ImmutableList.of(tableC)).stream()
            .map(t -> t.name())
            .collect(Collectors.toList());
    List<String> expectedFilteredTables = ImmutableList.of(tableA, tableB, tableC);

    Collections.sort(filteredTables);

    assertEquals(expectedFilteredTables, filteredTables);
  }
}
