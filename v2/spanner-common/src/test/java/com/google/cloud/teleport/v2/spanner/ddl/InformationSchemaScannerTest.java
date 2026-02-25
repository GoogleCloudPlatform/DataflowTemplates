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
package com.google.cloud.teleport.v2.spanner.ddl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.junit.Test;

public class InformationSchemaScannerTest {

  void mockGSQLColumnOptions(ReadContext context) {
    Statement listColumnOptions =
        Statement.of(
            "SELECT t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_catalog = '' AND t.table_schema = ''"
                + " ORDER BY t.table_name, t.column_name");
    ResultSet listColumnOptionsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listColumnOptions)).thenReturn(listColumnOptionsResultSet);
    when(listColumnOptionsResultSet.next()).thenReturn(true, false);
    when(listColumnOptionsResultSet.getString(0)).thenReturn("singer");
    when(listColumnOptionsResultSet.getString(1)).thenReturn("singerName");
    when(listColumnOptionsResultSet.getString(2)).thenReturn("option1");
    when(listColumnOptionsResultSet.getString(3)).thenReturn("STRING");
    when(listColumnOptionsResultSet.getString(4)).thenReturn("SomeName");
  }

  void mockGSQLIndex(ReadContext context) {
    Statement listIndexes =
        Statement.of(
            "SELECT t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered"
                + " FROM information_schema.indexes AS t"
                + " WHERE t.table_catalog = '' AND t.table_schema = '' AND"
                + " t.index_type='INDEX' AND t.spanner_is_managed = FALSE"
                + " ORDER BY t.table_name, t.index_name");
    ResultSet listIndexessResultSet = mock(ResultSet.class);
    when(context.executeQuery(listIndexes)).thenReturn(listIndexessResultSet);
    when(listIndexessResultSet.next()).thenReturn(true, true, true, false);
    when(listIndexessResultSet.getString(0)).thenReturn("singer", "singer", "album");
    when(listIndexessResultSet.getString(1)).thenReturn("index1", "PRIMARY_KEY", "PRIMARY_KEY");
    when(listIndexessResultSet.isNull(2)).thenReturn(true);
    when(listIndexessResultSet.getBoolean(3)).thenReturn(false);
    when(listIndexessResultSet.getBoolean(4)).thenReturn(false);
  }

  void mockGSQLIndexColumns(ReadContext context) {
    Statement listIndexColumns =
        Statement.of(
            "SELECT t.table_name, t.column_name, t.column_ordering, t.index_name "
                + "FROM information_schema.index_columns AS t "
                + "WHERE t.table_catalog = '' AND t.table_schema = '' "
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position");
    ResultSet listIndexColumnsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listIndexColumns)).thenReturn(listIndexColumnsResultSet);
    when(listIndexColumnsResultSet.next()).thenReturn(true, true, true, false);
    when(listIndexColumnsResultSet.getString(0)).thenReturn("singer", "singer", "album");
    when(listIndexColumnsResultSet.getString(1)).thenReturn("singerName", "singerId", "albumId");
    when(listIndexColumnsResultSet.isNull(2)).thenReturn(true, false, false);
    when(listIndexColumnsResultSet.getString(2)).thenReturn("ASC", "DESC");
    when(listIndexColumnsResultSet.getString(3)).thenReturn("index1", "PRIMARY_KEY", "PRIMARY_KEY");
  }

  void mockGSQLForeignKey(ReadContext context) {
    Statement listForeignKeys =
        Statement.of(
            "SELECT rc.constraint_name,"
                + " kcu1.table_name,"
                + " kcu1.column_name,"
                + " kcu2.table_name,"
                + " kcu2.column_name"
                + " FROM information_schema.referential_constraints as rc"
                + " INNER JOIN information_schema.key_column_usage as kcu1"
                + " ON kcu1.constraint_catalog = rc.constraint_catalog"
                + " AND kcu1.constraint_schema = rc.constraint_schema"
                + " AND kcu1.constraint_name = rc.constraint_name"
                + " INNER JOIN information_schema.key_column_usage as kcu2"
                + " ON kcu2.constraint_catalog = rc.unique_constraint_catalog"
                + " AND kcu2.constraint_schema = rc.unique_constraint_schema"
                + " AND kcu2.constraint_name = rc.unique_constraint_name"
                + " AND kcu2.ordinal_position = kcu1.position_in_unique_constraint"
                + " WHERE rc.constraint_catalog = ''"
                + " AND rc.constraint_schema = ''"
                + " AND kcu1.constraint_catalog = ''"
                + " AND kcu1.constraint_schema = ''"
                + " AND kcu2.constraint_catalog = ''"
                + " AND kcu2.constraint_schema = ''"
                + " ORDER BY rc.constraint_name, kcu1.ordinal_position;");
    ResultSet listForeignKeysResultSet = mock(ResultSet.class);
    when(context.executeQuery(listForeignKeys)).thenReturn(listForeignKeysResultSet);
    when(listForeignKeysResultSet.next()).thenReturn(true, false);
    when(listForeignKeysResultSet.getString(0)).thenReturn("fk1");
    when(listForeignKeysResultSet.getString(1)).thenReturn("album");
    when(listForeignKeysResultSet.getString(2)).thenReturn("singerId");
    when(listForeignKeysResultSet.getString(3)).thenReturn("singer");
    when(listForeignKeysResultSet.getString(4)).thenReturn("singerId");
  }

  void mockGSQLCheckConstraint(ReadContext context) {
    Statement listCheckConstraints =
        Statement.of(
            "SELECT ctu.TABLE_NAME,"
                + " cc.CONSTRAINT_NAME,"
                + " cc.CHECK_CLAUSE"
                + " FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE as ctu"
                + " INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS as cc"
                + " ON ctu.constraint_catalog = cc.constraint_catalog"
                + " AND ctu.constraint_schema = cc.constraint_schema"
                + " AND ctu.CONSTRAINT_NAME = cc.CONSTRAINT_NAME"
                + " WHERE NOT STARTS_WITH(cc.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_')"
                + " AND ctu.table_catalog = ''"
                + " AND ctu.table_schema = ''"
                + " AND ctu.constraint_catalog = ''"
                + " AND ctu.constraint_schema = ''"
                + " AND cc.SPANNER_STATE = 'COMMITTED';");
    ResultSet listCheckConstraintsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listCheckConstraints)).thenReturn(listCheckConstraintsResultSet);
    when(listCheckConstraintsResultSet.next()).thenReturn(true, false);
    when(listCheckConstraintsResultSet.getString(0)).thenReturn("album");
    when(listCheckConstraintsResultSet.getString(1)).thenReturn("check1");
    when(listCheckConstraintsResultSet.getString(2)).thenReturn("albumName!=NULL");
  }

  void mockGSQLListTables(ReadContext context) {
    Statement listTables =
        Statement.of(
            "SELECT t.table_name, t.parent_table_name, t.on_delete_action, t.interleave_type"
                + " FROM information_schema.tables AS t"
                + " WHERE t.table_catalog = '' AND t.table_schema = ''"
                + " AND t.table_type='BASE TABLE'");
    ResultSet listTablesResultSet = mock(ResultSet.class);
    when(context.executeQuery(listTables)).thenReturn(listTablesResultSet);
    when(listTablesResultSet.next()).thenReturn(true, true, false);
    when(listTablesResultSet.getString(0)).thenReturn("singer", "album");
    when(listTablesResultSet.getString(1)).thenReturn(null, "singer");
    when(listTablesResultSet.getString(2)).thenReturn(null, "CASCADE");
    when(listTablesResultSet.getString(3)).thenReturn(null, "IN PARENT");
  }

  void mockGSQLListColumns(ReadContext context) {
    Statement listColumns =
        Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored"
                + " FROM information_schema.columns as c"
                + " WHERE c.table_catalog = '' AND c.table_schema = '' "
                + " AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position");
    ResultSet listColumnsResultSet = mock(ResultSet.class);

    when(context.executeQuery(listColumns)).thenReturn(listColumnsResultSet);
    when(listColumnsResultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
    when(listColumnsResultSet.getString(0))
        .thenReturn("singer", "singer", "album", "album", "album", "album", "album");
    when(listColumnsResultSet.getString(1))
        .thenReturn(
            "singerId", "singerName", "singerId", "albumId", "albumName", "rating", "ratings");
    when(listColumnsResultSet.getString(3))
        .thenReturn(
            "INT64", "STRING(50)", "INT64", "INT64", "STRING(50)", "FLOAT32", "ARRAY<FLOAT32>");
    when(listColumnsResultSet.getString(4)).thenReturn("NO");
    when(listColumnsResultSet.getString(5)).thenReturn("NO");
    when(listColumnsResultSet.isNull(6)).thenReturn(true);
    when(listColumnsResultSet.isNull(7)).thenReturn(true);
  }

  void mockPgSQLColumnOptions(ReadContext context) {
    Statement listColumnOptions =
        Statement.of(
            "SELECT t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " ORDER BY t.table_name, t.column_name");
    ResultSet listColumnOptionsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listColumnOptions)).thenReturn(listColumnOptionsResultSet);
    when(listColumnOptionsResultSet.next()).thenReturn(true, false);
    when(listColumnOptionsResultSet.getString(0)).thenReturn("singer");
    when(listColumnOptionsResultSet.getString(1)).thenReturn("singerName");
    when(listColumnOptionsResultSet.getString(2)).thenReturn("option1");
    when(listColumnOptionsResultSet.getString(3)).thenReturn("character varying");
    when(listColumnOptionsResultSet.getString(4)).thenReturn("SomeName");
  }

  void mockPgSQLIndex(ReadContext context) {
    Statement listIndexes =
        Statement.of(
            "SELECT t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered, t.filter FROM information_schema.indexes AS t "
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.index_type='INDEX' AND t.spanner_is_managed = 'NO' "
                + " ORDER BY t.table_name, t.index_name");
    ResultSet listIndexessResultSet = mock(ResultSet.class);
    when(context.executeQuery(listIndexes)).thenReturn(listIndexessResultSet);
    when(listIndexessResultSet.next()).thenReturn(true, false);
    when(listIndexessResultSet.getString(0)).thenReturn("singer");
    when(listIndexessResultSet.getString(1)).thenReturn("index1");
    when(listIndexessResultSet.isNull(2)).thenReturn(true);
    when(listIndexessResultSet.getString(3)).thenReturn("YES");
    when(listIndexessResultSet.getString(4)).thenReturn("YES");
    when(listIndexessResultSet.isNull(5)).thenReturn(true);
  }

  void mockPgSQLIndexColumns(ReadContext context) {
    Statement listIndexColumns =
        Statement.of(
            "SELECT t.table_name, t.column_name, t.column_ordering, t.index_name "
                + "FROM information_schema.index_columns AS t "
                + "WHERE t.table_schema NOT IN "
                + "('information_schema', 'spanner_sys', 'pg_catalog') "
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position");
    ResultSet listIndexColumnsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listIndexColumns)).thenReturn(listIndexColumnsResultSet);
    when(listIndexColumnsResultSet.next()).thenReturn(true, false);
    when(listIndexColumnsResultSet.getString(0)).thenReturn("singer");
    when(listIndexColumnsResultSet.getString(1)).thenReturn("singerName");
    when(listIndexColumnsResultSet.isNull(2)).thenReturn(true);
    when(listIndexColumnsResultSet.getString(3)).thenReturn("index1");
  }

  void mockPgSQLForeignKey(ReadContext context) {
    Statement listForeignKeys =
        Statement.of(
            "SELECT rc.constraint_name,"
                + " kcu1.table_name,"
                + " kcu1.column_name,"
                + " kcu2.table_name,"
                + " kcu2.column_name"
                + " FROM information_schema.referential_constraints as rc"
                + " INNER JOIN information_schema.key_column_usage as kcu1"
                + " ON kcu1.constraint_catalog = rc.constraint_catalog"
                + " AND kcu1.constraint_schema = rc.constraint_schema"
                + " AND kcu1.constraint_name = rc.constraint_name"
                + " INNER JOIN information_schema.key_column_usage as kcu2"
                + " ON kcu2.constraint_catalog = rc.unique_constraint_catalog"
                + " AND kcu2.constraint_schema = rc.unique_constraint_schema"
                + " AND kcu2.constraint_name = rc.unique_constraint_name"
                + " AND kcu2.ordinal_position = kcu1.position_in_unique_constraint"
                + " WHERE rc.constraint_catalog = kcu1.constraint_catalog"
                + " AND rc.constraint_catalog = kcu2.constraint_catalog"
                + " AND rc.constraint_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND rc.constraint_schema = kcu1.constraint_schema"
                + " AND rc.constraint_schema = kcu2.constraint_schema"
                + " ORDER BY rc.constraint_name, kcu1.ordinal_position;");
    ResultSet listForeignKeysResultSet = mock(ResultSet.class);
    when(context.executeQuery(listForeignKeys)).thenReturn(listForeignKeysResultSet);
    when(listForeignKeysResultSet.next()).thenReturn(true, false);
    when(listForeignKeysResultSet.getString(0)).thenReturn("fk1");
    when(listForeignKeysResultSet.getString(1)).thenReturn("album");
    when(listForeignKeysResultSet.getString(2)).thenReturn("singerId");
    when(listForeignKeysResultSet.getString(3)).thenReturn("singer");
    when(listForeignKeysResultSet.getString(4)).thenReturn("singerId");
  }

  void mockPgSQLCheckConstraint(ReadContext context) {
    Statement listCheckConstraints =
        Statement.of(
            "SELECT ctu.TABLE_NAME,"
                + " cc.CONSTRAINT_NAME,"
                + " cc.CHECK_CLAUSE"
                + " FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as ctu"
                + " INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS as cc"
                + " ON ctu.constraint_catalog = cc.constraint_catalog"
                + " AND ctu.constraint_schema = cc.constraint_schema"
                + " AND ctu.CONSTRAINT_NAME = cc.CONSTRAINT_NAME"
                + " WHERE NOT STARTS_WITH(cc.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_')"
                + " AND ctu.table_catalog = ctu.constraint_catalog"
                + " AND ctu.table_schema NOT IN"
                + "('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND ctu.table_schema = ctu.constraint_schema"
                + " AND cc.SPANNER_STATE = 'COMMITTED';");
    ResultSet listCheckConstraintsResultSet = mock(ResultSet.class);
    when(context.executeQuery(listCheckConstraints)).thenReturn(listCheckConstraintsResultSet);
    when(listCheckConstraintsResultSet.next()).thenReturn(true, false);
    when(listCheckConstraintsResultSet.getString(0)).thenReturn("album");
    when(listCheckConstraintsResultSet.getString(1)).thenReturn("check1");
    when(listCheckConstraintsResultSet.getString(2)).thenReturn("albumName!=NULL");
  }

  void mockPgSQLListTables(ReadContext context) {
    Statement listTables =
        Statement.of(
            "SELECT t.table_name, t.parent_table_name, t.on_delete_action, t.interleave_type FROM"
                + " information_schema.tables AS t"
                + " WHERE t.table_schema NOT IN "
                + "('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.table_type='BASE TABLE'");
    ResultSet listTablesResultSet = mock(ResultSet.class);
    when(context.executeQuery(listTables)).thenReturn(listTablesResultSet);
    when(listTablesResultSet.next()).thenReturn(true, true, false);
    when(listTablesResultSet.getString(0)).thenReturn("singer", "album");
    when(listTablesResultSet.getString(1)).thenReturn(null, "singer");
    when(listTablesResultSet.getString(2)).thenReturn(null, "CASCADE");
    when(listTablesResultSet.getString(3)).thenReturn(null, "IN PARENT");
  }

  void mockPgSQLListColumns(ReadContext context) {
    Statement listColumns =
        Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored"
                + " FROM information_schema.columns as c"
                + " WHERE c.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog') "
                + " AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position");
    ResultSet listColumnsResultSet = mock(ResultSet.class);

    when(context.executeQuery(listColumns)).thenReturn(listColumnsResultSet);
    when(listColumnsResultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
    when(listColumnsResultSet.getString(0))
        .thenReturn("singer", "singer", "album", "album", "album", "album", "album");
    when(listColumnsResultSet.getString(1))
        .thenReturn(
            "singerId", "singerName", "singerId", "albumId", "albumName", "rating", "ratings");
    when(listColumnsResultSet.getString(3))
        .thenReturn(
            "bigint",
            "character varying(50)",
            "bigint",
            "bigint",
            "character varying(50)",
            "real",
            "real[]");
    when(listColumnsResultSet.getString(4)).thenReturn("NO");
    when(listColumnsResultSet.getString(5)).thenReturn("NO");
    when(listColumnsResultSet.isNull(6)).thenReturn(true);
    when(listColumnsResultSet.isNull(7)).thenReturn(true);
  }

  @Test
  public void testScanGSQLDdl() {
    ReadContext context = mock(ReadContext.class);

    mockGSQLListTables(context);
    mockGSQLListColumns(context);
    mockGSQLColumnOptions(context);
    mockGSQLIndex(context);
    mockGSQLIndexColumns(context);
    mockGSQLForeignKey(context);
    mockGSQLCheckConstraint(context);
    InformationSchemaScanner informationSchemaScanner =
        new InformationSchemaScanner(context, Dialect.GOOGLE_STANDARD_SQL);
    Ddl ddl = informationSchemaScanner.scan();
    String expectedDdl =
        "CREATE TABLE `singer` (\n"
            + "\t`singerId`                              INT64 NOT NULL,\n"
            + "\t`singerName`                            STRING(50) NOT NULL OPTIONS (option1=\"SomeName\"),\n"
            + ") PRIMARY KEY (`singerId` ASC)\n"
            + "CREATE INDEX `PRIMARY_KEY` ON `singer`()\n"
            + "CREATE INDEX `index1` ON `singer`() STORING (`singerName`)\n"
            + "\n"
            + "CREATE TABLE `album` (\n"
            + "\t`singerId`                              INT64 NOT NULL,\n"
            + "\t`albumId`                               INT64 NOT NULL,\n"
            + "\t`albumName`                             STRING(50) NOT NULL,\n"
            + "\t`rating`                                FLOAT32 NOT NULL,\n"
            + "\t`ratings`                               ARRAY<FLOAT32> NOT NULL,\n"
            + "\tCONSTRAINT `check1` CHECK (albumName!=NULL),\n"
            + ") PRIMARY KEY (`albumId` DESC),\n"
            + "INTERLEAVE IN PARENT `singer` ON DELETE CASCADE\n"
            + "CREATE INDEX `PRIMARY_KEY` ON `album`()\n"
            + "ALTER TABLE `album` ADD CONSTRAINT `fk1` FOREIGN KEY (`singerId`) REFERENCES `singer` (`singerId`)";
    assertEquals(expectedDdl, ddl.prettyPrint());
  }

  @Test
  public void testScanPgSQLDdl() {
    ReadContext context = mock(ReadContext.class);

    mockPgSQLListTables(context);
    mockPgSQLListColumns(context);
    mockPgSQLColumnOptions(context);
    mockPgSQLIndex(context);
    mockPgSQLIndexColumns(context);
    mockPgSQLForeignKey(context);
    mockPgSQLCheckConstraint(context);
    InformationSchemaScanner informationSchemaScanner =
        new InformationSchemaScanner(context, Dialect.POSTGRESQL);
    Ddl ddl = informationSchemaScanner.scan();
    String expectedDdl =
        "CREATE TABLE \"singer\" (\n"
            + "\t\"singerId\"                              bigint NOT NULL,\n"
            + "\t\"singerName\"                            character varying(50) NOT NULL OPTIONS (option1='SomeName'),\n"
            + "\tPRIMARY KEY ()\n"
            + ")\n"
            + "CREATE UNIQUE INDEX \"index1\" ON \"singer\"() INCLUDE (\"singerName\")\n"
            + "\n"
            + "CREATE TABLE \"album\" (\n"
            + "\t\"singerId\"                              bigint NOT NULL,\n"
            + "\t\"albumId\"                               bigint NOT NULL,\n"
            + "\t\"albumName\"                             character varying(50) NOT NULL,\n"
            + "\t\"rating\"                                real NOT NULL,\n"
            + "\t\"ratings\"                               real[] NOT NULL,\n"
            + "\tCONSTRAINT \"check1\" CHECK (albumName!=NULL),\n"
            + "\tPRIMARY KEY ()\n"
            + ") \n"
            + "INTERLEAVE IN PARENT \"singer\" ON DELETE CASCADE\n"
            + "\n"
            + "ALTER TABLE \"album\" ADD CONSTRAINT \"fk1\" FOREIGN KEY (\"singerId\") REFERENCES \"singer\" (\"singerId\")";
    assertEquals(expectedDdl, ddl.prettyPrint());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithInvalidDialect() {
    ReadContext context = mock(ReadContext.class);
    InformationSchemaScanner informationSchemaScanner =
        new InformationSchemaScanner(context, Dialect.fromName("xyz"));
    Ddl ddl = informationSchemaScanner.scan();
  }
}
