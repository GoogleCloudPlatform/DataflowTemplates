/*
 * Copyright (C) 2022 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import org.junit.Test;

/** Tests for BuildReadFromTableOperations class. */
public class BuildReadFromTableOperationsTest {

  @Test
  public void testColumnExpressionNumeric() {
    BuildReadFromTableOperations buildReadFromTableOperations =
        new BuildReadFromTableOperations(null);
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("table")
            .column("colName")
            .numeric()
            .endColumn()
            .column("colName1")
            .type(Type.array(Type.numeric()))
            .endColumn()
            .endTable()
            .build();
    assertEquals(
        "t.`colName`",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName")));
    assertEquals(
        "CASE WHEN t.`colName1` IS NULL THEN NULL ELSE IFNULL((SELECT ARRAY_AGG(CASE WHEN num IS NULL THEN NULL ELSE "
            + "CAST(num AS STRING) END) FROM UNNEST(t.`colName1`) AS num), []) END AS colName1",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName1")));
  }

  @Test
  public void testColumnExpressionJson() {
    BuildReadFromTableOperations buildReadFromTableOperations =
        new BuildReadFromTableOperations(null);
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("table")
            .column("colName")
            .json()
            .endColumn()
            .column("colName1")
            .type(Type.array(Type.json()))
            .endColumn()
            .endTable()
            .build();
    assertEquals(
        "CASE WHEN t.`colName` IS NULL THEN NULL ELSE TO_JSON_STRING(t.`colName`) END AS colName",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName")));
    assertEquals(
        "CASE WHEN t.`colName1` IS NULL THEN NULL ELSE IFNULL((SELECT ARRAY_AGG(CASE WHEN element IS NULL THEN NULL "
            + "ELSE TO_JSON_STRING(element) END) FROM UNNEST(t.`colName1`) AS element), []) END AS colName1",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName1")));
  }

  @Test
  public void testColumnExpressionInt() {
    BuildReadFromTableOperations buildReadFromTableOperations =
        new BuildReadFromTableOperations(null);
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("table")
            .column("colName")
            .int64()
            .endColumn()
            .endTable()
            .build();
    assertEquals(
        "t.`colName`",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName")));
  }

  @Test
  public void testColumnExpressionPgInt() {
    BuildReadFromTableOperations buildReadFromTableOperations =
        new BuildReadFromTableOperations(null);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("table")
            .column("colName")
            .int64()
            .endColumn()
            .endTable()
            .build();
    assertEquals(
        "t.\"colName\"",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName")));
  }

  @Test
  public void testColumnExpressionPgJsonb() {
    BuildReadFromTableOperations buildReadFromTableOperations =
        new BuildReadFromTableOperations(null);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("table")
            .column("colName")
            .pgJsonb()
            .endColumn()
            .endTable()
            .build();
    assertEquals(
        "t.\"colName\"",
        buildReadFromTableOperations.createColumnExpression(ddl.table("table").column("colName")));
  }
}
