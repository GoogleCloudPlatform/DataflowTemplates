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
package com.google.cloud.teleport.v2.templates.spanner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

/** Unit tests ShadowTableCreator class. */
public class ShadowTableCreatorTest {

  @Test
  public void canConstructShadowTableForOracleWithGsqlDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithGSqlDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("oracle", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(
            testDdl, "Users_interleaved", Dialect.GOOGLE_STANDARD_SQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has oracle sequence information column in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("scn");
    assertThat(columns, is(expectedColumns));
  }

  @Test
  public void canConstructShadowTableForOracleWithPostgresDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithPostgresDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("oracle", "shadow_", Dialect.POSTGRESQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(testDdl, "Users_interleaved", Dialect.POSTGRESQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has oracle sequence information column in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("scn");
    assertThat(columns, is(expectedColumns));
    List<String> columnTypes =
        shadowTable.columns().stream().map(c -> c.type().toString()).collect(Collectors.toList());
    List<String> expectedColumnTypes = new ArrayList<>();
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgBool().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgFloat8().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgBytea().toString());
    expectedColumnTypes.add(Type.pgTimestamptz().toString());
    expectedColumnTypes.add(Type.pgDate().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    assertThat(columnTypes, is(expectedColumnTypes));
  }

  @Test
  public void canConstructShadowTableForMySqlWithGsqlDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithGSqlDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("mysql", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(
            testDdl, "Users_interleaved", Dialect.GOOGLE_STANDARD_SQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has mysql sequence information in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("log_file");
    expectedColumns.add("log_position");
    assertThat(columns, is(expectedColumns));
  }

  @Test
  public void canConstructShadowTableForMySqlWithPostgresDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithPostgresDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("mysql", "shadow_", Dialect.POSTGRESQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(testDdl, "Users_interleaved", Dialect.POSTGRESQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has mysql sequence information in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("log_file");
    expectedColumns.add("log_position");
    assertThat(columns, is(expectedColumns));
    List<String> columnTypes =
        shadowTable.columns().stream().map(c -> c.type().toString()).collect(Collectors.toList());
    List<String> expectedColumnTypes = new ArrayList<>();
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgBool().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgFloat8().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgBytea().toString());
    expectedColumnTypes.add(Type.pgTimestamptz().toString());
    expectedColumnTypes.add(Type.pgDate().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    assertThat(columnTypes, is(expectedColumnTypes));
  }

  @Test
  public void canConstructShadowTableForPostgresWithGsqlDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithGSqlDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("postgresql", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(
            testDdl, "Users_interleaved", Dialect.GOOGLE_STANDARD_SQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has postgresql sequence information in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("lsn");
    assertThat(columns, is(expectedColumns));
  }

  @Test
  public void canConstructShadowTableForPostgresWithPostgresDialect() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdlWithPostgresDialect();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("postgresql", "shadow_", Dialect.POSTGRESQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(testDdl, "Users_interleaved", Dialect.POSTGRESQL);

    /* Verify
     * (1) name of shadow table
     * (2) primary keys columns are same as data tables
     * (3) Has postgresql sequence information in addition to primary keys columns
     */
    assertEquals(shadowTable.name(), "shadow_Users_interleaved");
    assertThat(shadowTable.primaryKeys(), is(testDdl.table("Users_interleaved").primaryKeys()));
    Set<String> columns =
        shadowTable.columns().stream().map(c -> c.name()).collect(Collectors.toSet());
    Set<String> expectedColumns =
        testDdl.table("Users_interleaved").primaryKeys().stream()
            .map(c -> c.name())
            .collect(Collectors.toSet());
    expectedColumns.add("timestamp");
    expectedColumns.add("lsn");
    assertThat(columns, is(expectedColumns));
    List<String> columnTypes =
        shadowTable.columns().stream().map(c -> c.type().toString()).collect(Collectors.toList());
    List<String> expectedColumnTypes = new ArrayList<>();
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgBool().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgFloat8().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    expectedColumnTypes.add(Type.pgBytea().toString());
    expectedColumnTypes.add(Type.pgTimestamptz().toString());
    expectedColumnTypes.add(Type.pgDate().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgInt8().toString());
    expectedColumnTypes.add(Type.pgVarchar().toString());
    assertThat(columnTypes, is(expectedColumnTypes));
  }

  @Test
  public void canHandlePkColumnNameCollision() {
    Ddl ddl =
        Ddl.builder()
            .createTable("MyTable")
            .column("timestamp")
            .timestamp()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("timestamp")
            .end()
            .endTable()
            .build();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("mysql", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(ddl, "MyTable", Dialect.GOOGLE_STANDARD_SQL);

    assertEquals(shadowTable.name(), "shadow_MyTable");
    // The original PK column should exist.
    assertEquals(shadowTable.column("timestamp").type().getCode(), Type.Code.TIMESTAMP);
    // The new shadow column should have been renamed.
    assertEquals(shadowTable.column("shadow_timestamp").type().getCode(), Type.Code.INT64);
  }

  @Test
  public void canHandleMultiplePkColumnNameCollisions() {
    Ddl ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("MyTable")
            .column("log_file")
            .string()
            .max()
            .endColumn()
            .column("shadow_log_file")
            .int64()
            .max()
            .endColumn()
            .primaryKey()
            .asc("log_file")
            .asc("shadow_log_file")
            .end()
            .endTable()
            .build();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("mysql", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(ddl, "MyTable", Dialect.GOOGLE_STANDARD_SQL);

    assertEquals(shadowTable.name(), "shadow_MyTable");
    // The original PK columns should exist.
    assertEquals(shadowTable.column("log_file").type().getCode(), Type.Code.STRING);
    assertEquals(shadowTable.column("shadow_log_file").type().getCode(), Type.Code.INT64);
    // The new shadow column should have been renamed twice.
    assertEquals(shadowTable.column("shadow_shadow_log_file").type().getCode(), Type.Code.STRING);
  }

  @Test
  public void testGetSafeShadowColumnName() {
    // Base case: no collision
    assertEquals(
        "log_file",
        ShadowTableCreator.getSafeShadowColumnName("log_file", Set.of("column1", "column2")));

    // Single collision: should add one prefix
    assertEquals(
        "shadow_log_file",
        ShadowTableCreator.getSafeShadowColumnName("log_file", Set.of("column1", "log_file")));

    // Multiple collisions: should add two prefixes
    assertEquals(
        "shadow_shadow_log_file",
        ShadowTableCreator.getSafeShadowColumnName(
            "log_file", Set.of("log_file", "shadow_log_file")));

    // Case-insensitivity test
    assertEquals(
        "shadow_log_file",
        ShadowTableCreator.getSafeShadowColumnName("log_file", Set.of("column1", "LOG_FILE")));
  }

  @Test
  public void canConstructShadowTableWithGeneratedPrimaryKey() {
    Ddl ddl =
        Ddl.builder()
            .createTable("MyTable")
            .column("id")
            .int64()
            .isGenerated(true)
            .generationExpression("id_gen")
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator("mysql", "shadow_", Dialect.GOOGLE_STANDARD_SQL);
    Table shadowTable =
        shadowTableCreator.constructShadowTable(ddl, "MyTable", Dialect.GOOGLE_STANDARD_SQL);

    assertEquals(shadowTable.name(), "shadow_MyTable");
    // The shadow table column should be present but NOT generated
    Column idColumn = shadowTable.column("id");
    assertThat(idColumn.isGenerated(), is(false));
    assertThat(idColumn.generationExpression(), is(""));
  }
}
