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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Before;
import org.junit.Test;

public class IdentityMapperTest {

  IdentityMapper mapper;

  @Before
  public void setup() {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.string())
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("Account")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("balanceId")
            .int64()
            .notNull()
            .endColumn()
            .column("balance")
            .float64()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();
    this.mapper = new IdentityMapper(ddl);
  }

  @Test
  public void testGetSpannerTableName() {
    String srcTableName = "Users";
    String result = mapper.getSpannerTableName("", srcTableName);
    assertEquals(srcTableName, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerTableNameMissingTable() {
    String srcTableName = "abc";
    mapper.getSpannerTableName("", srcTableName);
  }

  @Test
  public void testGetSpannerColumnName() {
    String srcTable = "Users";
    String srcColumn = "id";
    String result = mapper.getSpannerColumnName("", srcTable, srcColumn);
    assertEquals(srcColumn, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnNameMissingTable() {
    String srcTable = "MySourceTable";
    String srcColumn = "id";
    mapper.getSpannerColumnName("", srcTable, srcColumn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnNameMissingColumn() {
    String srcTable = "Users";
    String srcColumn = "MySourceColumn";
    mapper.getSpannerColumnName("", srcTable, srcColumn);
  }

  @Test
  public void testGetSourceColumnName() {
    String spannerTable = "MySpannerTable";
    String spannerColumn = "MySpannerColumn";
    String result = mapper.getSourceColumnName("", spannerTable, spannerColumn);
    assertEquals(spannerColumn, result);
  }

  @Test
  public void testGetSpannerColumnType() {
    String spannerTable = "Users";
    String spannerColumn = "id";
    Type expectedType = Type.int64();
    Type result = mapper.getSpannerColumnType("", spannerTable, spannerColumn);
    assertEquals(expectedType, result);

    spannerColumn = "first_name";
    expectedType = Type.string();
    result = mapper.getSpannerColumnType("", spannerTable, spannerColumn);
    assertEquals(expectedType, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnTypeMissingTable() {
    String spannerTable = "wrongTableName";
    String spannerColumn = "id";
    mapper.getSpannerColumnType("", spannerTable, spannerColumn);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnTypeMissingColumn() {
    String spannerTable = "Users";
    String spannerColumn = "wrongColumn";
    mapper.getSpannerColumnType("", spannerTable, spannerColumn);
  }

  @Test
  public void testGetSpannerColumns() {
    String spannerTable = "Users";
    List<String> expectedColumns = Arrays.asList("id", "first_name", "last_name");
    List<String> result = mapper.getSpannerColumns("", spannerTable);
    assertEquals(expectedColumns, result);
  }

  @Test(expected = NoSuchElementException.class)
  public void testGetSpannerColumnsMissingTable() {
    String spannerTable = "wrongTableName";
    mapper.getSpannerColumns("", spannerTable);
  }
}
