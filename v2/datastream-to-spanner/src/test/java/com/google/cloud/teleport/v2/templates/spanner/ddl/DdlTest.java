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
package com.google.cloud.teleport.v2.templates.spanner.ddl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.spanner.common.Type;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test coverage for {@link Ddl}. */
public class DdlTest {

  @Test
  public void testEmptyDbGSQL() {
    Ddl empty = Ddl.builder().build();
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
    assertNotNull(empty.hashCode());
  }

  @Test
  public void testEmptyDbPG() {
    Ddl empty = Ddl.builder(Dialect.POSTGRESQL).build();
    assertEquals(empty.dialect(), Dialect.POSTGRESQL);
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
    assertFalse(empty.equals(null));
    assertFalse(empty.equals(Boolean.TRUE));
    assertNotNull(empty.hashCode());
  }

  @Test
  public void testDdlGSQL() {
    Ddl.Builder builder = Ddl.builder();
    builder
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
        .column("full_name")
        .type(Type.string())
        .max()
        .generatedAs("CONCAT(first_name, ' ', last_name)")
        .stored()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .indexes(ImmutableList.of("CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"))
        .foreignKeys(
            ImmutableList.of(
                "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                    + " REFERENCES `AllowedNames` (`first_name`)"))
        .checkConstraints(ImmutableList.of("CONSTRAINT `ck` CHECK (`first_name` != `last_name`)"))
        .endTable();
    Ddl ddl = builder.build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            " CREATE TABLE `Users` ("
                + " `id` INT64 NOT NULL,"
                + " `first_name` STRING(10),"
                + " `last_name` STRING(MAX),"
                + " `full_name` STRING(MAX) AS (CONCAT(first_name, ' ', last_name)) STORED,"
                + " CONSTRAINT `ck` CHECK (`first_name` != `last_name`),"
                + " ) PRIMARY KEY (`id` ASC)"
                + " CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                + " REFERENCES `AllowedNames` (`first_name`)"));
    List<String> statements = ddl.statements();
    assertEquals(3, statements.size());
    assertThat(
        statements.get(0),
        equalToCompressingWhiteSpace(
            " CREATE TABLE `Users` ("
                + " `id` INT64 NOT NULL,"
                + " `first_name` STRING(10),"
                + " `last_name` STRING(MAX),"
                + " `full_name` STRING(MAX) AS (CONCAT(first_name, ' ', last_name)) STORED,"
                + " CONSTRAINT `ck` CHECK (`first_name` != `last_name`),"
                + " ) PRIMARY KEY (`id` ASC)"));
    assertThat(
        statements.get(1),
        equalToCompressingWhiteSpace(" CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"));
    assertThat(
        statements.get(2),
        equalToCompressingWhiteSpace(
            "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`) REFERENCES"
                + " `AllowedNames` (`first_name`)"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void testDdlPG() {
    Ddl.Builder builder = Ddl.builder(Dialect.POSTGRESQL);
    builder
        .createTable("Users")
        .column("id")
        .pgInt8()
        .notNull()
        .endColumn()
        .column("first_name")
        .pgVarchar()
        .size(10)
        .endColumn()
        .column("last_name")
        .type(Type.pgVarchar())
        .max()
        .endColumn()
        .column("full_name")
        .type(Type.pgVarchar())
        .max()
        .generatedAs("CONCAT(first_name, ' ', last_name)")
        .stored()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .indexes(
            ImmutableList.of("CREATE INDEX \"UsersByFirstName\" ON \"Users\" (\"first_name\")"))
        .foreignKeys(
            ImmutableList.of(
                "ALTER TABLE \"Users\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"first_name\")"
                    + " REFERENCES \"AllowedNames\" (\"first_name\")"))
        .checkConstraints(
            ImmutableList.of("CONSTRAINT \"ck\" CHECK (\"first_name\" != \"last_name\")"))
        .endTable();
    Ddl ddl = builder.build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            " CREATE TABLE \"Users\" ("
                + " \"id\" bigint NOT NULL,"
                + " \"first_name\" character varying(10),"
                + " \"last_name\" character varying,"
                + " \"full_name\" character varying GENERATED ALWAYS AS"
                + " (CONCAT(first_name, ' ', last_name)) STORED,"
                + " CONSTRAINT \"ck\" CHECK (\"first_name\" != \"last_name\"),"
                + " PRIMARY KEY (\"id\")"
                + " ) "
                + " CREATE INDEX \"UsersByFirstName\" ON \"Users\" (\"first_name\")"
                + " ALTER TABLE \"Users\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"first_name\")"
                + " REFERENCES \"AllowedNames\" (\"first_name\")"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void testInterleavingGSQL() {
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
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " ) PRIMARY KEY (`id` ASC)"
                + " CREATE TABLE `Account` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `balanceId`                             INT64 NOT NULL,"
                + " `balance`                               FLOAT64 NOT NULL,"
                + " ) PRIMARY KEY (`id` ASC), "
                + " INTERLEAVE IN PARENT `Users` ON DELETE CASCADE"));
    Collection<Table> rootTables = ddl.rootTables();
    assertEquals(1, rootTables.size());
    assertEquals("Users", rootTables.iterator().next().name());
    HashMultimap<Integer, String> perLevelView = ddl.perLevelView();
    assertEquals(2, perLevelView.size());
    assertTrue(perLevelView.containsKey(0));
    assertEquals("users", perLevelView.get(0).iterator().next());
    assertTrue(perLevelView.containsKey(1));
    assertEquals("account", perLevelView.get(1).iterator().next());
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void testInterleavingPG() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.pgVarchar())
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("Account")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("balanceId")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("balance")
            .pgFloat8()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE \"Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " PRIMARY KEY (\"id\")"
                + " ) "
                + " CREATE TABLE \"Account\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"balanceId\"                             bigint NOT NULL,"
                + " \"balance\"                               double precision NOT NULL,"
                + " PRIMARY KEY (\"id\")"
                + " ) "
                + " INTERLEAVE IN PARENT \"Users\" ON DELETE CASCADE"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void testIndexGSQL() {
    Index.Builder builder =
        Index.builder().name("user_index").table("User").unique().nullFiltered(true);
    builder
        .columns()
        .create()
        .name("first_name")
        .asc()
        .endIndexColumn()
        .create()
        .name("last_name")
        .desc()
        .endIndexColumn()
        .create()
        .name("full_name")
        .storing()
        .endIndexColumn()
        .end();
    Index index = builder.build();
    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE UNIQUE NULL_FILTERED INDEX `user_index` ON `User`(`first_name` ASC,"
                + " `last_name` DESC) STORING (`full_name`)"));
    assertTrue(index.equals(index));
    assertFalse(index.equals(Boolean.TRUE));
    builder = index.autoToBuilder();
    builder
        .columns()
        .create()
        .name("first_name")
        .asc()
        .endIndexColumn()
        .create()
        .name("last_name")
        .desc()
        .endIndexColumn()
        .create()
        .name("full_name")
        .storing()
        .endIndexColumn()
        .end();
    Index index1 = builder.build();
    assertTrue(index.equals(index1));
    assertNotNull(index.hashCode());
  }

  @Test
  public void testIndexPG() {
    Index.Builder builder =
        Index.builder(Dialect.POSTGRESQL)
            .name("user_index")
            .table("User")
            .unique()
            .filter("\"first_name\" IS NOT NULL AND \"last_name\" IS NOT NULL");
    builder
        .columns()
        .create()
        .name("first_name")
        .asc()
        .endIndexColumn()
        .create()
        .name("last_name")
        .desc()
        .endIndexColumn()
        .create()
        .name("full_name")
        .storing()
        .endIndexColumn()
        .end();
    Index index = builder.build();
    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE UNIQUE INDEX \"user_index\" ON \"User\"(\"first_name\" ASC,"
                + " \"last_name\" DESC) INCLUDE (\"full_name\") WHERE \"first_name\" IS"
                + " NOT NULL AND \"last_name\" IS NOT NULL"));
    assertTrue(index.equals(index));
    assertFalse(index.equals(Boolean.TRUE));
    builder = index.autoToBuilder();
    builder
        .columns()
        .create()
        .name("first_name")
        .asc()
        .endIndexColumn()
        .create()
        .name("last_name")
        .desc()
        .endIndexColumn()
        .create()
        .name("full_name")
        .storing()
        .endIndexColumn()
        .end();
    Index index1 = builder.build();
    assertTrue(index.equals(index1));
    assertNotNull(index.hashCode());
  }

  @Test
  public void testCheckConstraintGSQL() {
    CheckConstraint checkConstraint =
        CheckConstraint.builder()
            .name("name_check")
            .expression("`first_name` != `last_name`")
            .build();
    assertThat(
        checkConstraint.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CONSTRAINT `name_check` CHECK (`first_name` != `last_name`)"));
    assertNotNull(checkConstraint.hashCode());
  }

  @Test
  public void testCheckConstraintPG() {
    CheckConstraint checkConstraint =
        CheckConstraint.builder(Dialect.POSTGRESQL)
            .name("name_check")
            .expression("\"first_name\" != \"last_name\"")
            .build();
    assertThat(
        checkConstraint.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CONSTRAINT \"name_check\" CHECK (\"first_name\" != \"last_name\")"));
    assertNotNull(checkConstraint.hashCode());
  }

  @Test
  public void testForeignKeyGSQL() {
    ForeignKey.Builder builder =
        ForeignKey.builder().name("account_to_user").table("Account").referencedTable("User");
    builder.columnsBuilder().add("account_id", "owner_name");
    builder.referencedColumnsBuilder().add("user_id", "full_name");
    ForeignKey foreignKey = builder.build();
    assertThat(
        foreignKey.prettyPrint(),
        equalToCompressingWhiteSpace(
            "ALTER TABLE `Account` ADD CONSTRAINT `account_to_user` FOREIGN KEY"
                + " (`account_id`, `owner_name`) REFERENCES `User` (`user_id`,"
                + " `full_name`)"));
    assertNotNull(foreignKey.hashCode());
  }

  @Test
  public void testForeignKeyPG() {
    ForeignKey.Builder builder =
        ForeignKey.builder(Dialect.POSTGRESQL)
            .name("account_to_user")
            .table("Account")
            .referencedTable("User");
    builder.columnsBuilder().add("account_id", "owner_name");
    builder.referencedColumnsBuilder().add("user_id", "full_name");
    ForeignKey foreignKey = builder.build();
    assertThat(
        foreignKey.prettyPrint(),
        equalToCompressingWhiteSpace(
            "ALTER TABLE \"Account\" ADD CONSTRAINT \"account_to_user\" FOREIGN KEY"
                + " (\"account_id\", \"owner_name\") REFERENCES \"User\" (\"user_id\","
                + " \"full_name\")"));
    assertNotNull(foreignKey.hashCode());
  }

  @Test
  public void testDdlEquals() {
    Ddl ddl1 = Ddl.builder(Dialect.GOOGLE_STANDARD_SQL).build();
    Ddl ddl2 = Ddl.builder(Dialect.POSTGRESQL).build();
    assertFalse(ddl1.equals(ddl2));
    Ddl.Builder ddl1Builder =
        Ddl.builder().createTable("Users").column("id").int64().endColumn().endTable();
    ddl1Builder.createTable("Users");
    ddl1 = ddl1Builder.build();
    assertFalse(ddl1.equals(ddl2));
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testIndexColumnBuilder() {
    IndexColumn.IndexColumnsBuilder indexColumnsBuilder =
        new IndexColumn.IndexColumnsBuilder(null, null);
    thrown.expect(IllegalArgumentException.class);
    indexColumnsBuilder.name("name");
    indexColumnsBuilder.asc();
    indexColumnsBuilder.desc();
    indexColumnsBuilder.storing();

    IndexColumn.Builder indexColumnBuilder = new AutoValue_IndexColumn.Builder();
    thrown.expect(NullPointerException.class);
    indexColumnBuilder.name(null);
    indexColumnBuilder.order(null);
    indexColumnBuilder.dialect(null);
    thrown.expect(IllegalStateException.class);

    indexColumnBuilder.autoBuild();
    IndexColumn indexColumn =
        indexColumnBuilder
            .name("col1")
            .order(IndexColumn.Order.ASC)
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .autoBuild();
    assertTrue(indexColumn.equals(indexColumn));
    assertFalse(indexColumn.equals(Boolean.TRUE));
    IndexColumn indexColumn1 = IndexColumn.create("col1", IndexColumn.Order.ASC);
    assertTrue(indexColumn.equals(indexColumn1));
  }

  @Test
  public void testColumnBuilder() {
    Column.Builder columnBuilder = Column.builder();
    thrown.expect(NullPointerException.class);
    columnBuilder.name(null);
    columnBuilder.type(null);
    columnBuilder.columnOptions(null);
    columnBuilder.dialect(null);
    thrown.expect(IllegalStateException.class);
    columnBuilder.autoBuild();
    Column column = Column.builder().name("colName").type(Type.string()).autoBuild();
    assertTrue(column.equals(column));
    assertFalse(column.equals(Boolean.TRUE));
    Column column1 = Column.builder().name("colname").type(Type.bool()).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.bool()).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).size(20).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).notNull().autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).isGenerated(true).autoBuild();
    assertFalse(column.equals(column1));
    column1 =
        Column.builder()
            .name("colName")
            .type(Type.string())
            .generationExpression("1+2")
            .autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).isStored(true).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).autoBuild();
    assertTrue(column.equals(column1));
  }

  @Test
  public void testTableBuilder() {
    Table.Builder tableBuilder = Table.builder();
    thrown.expect(NullPointerException.class);
    tableBuilder.primaryKeys(null);
    tableBuilder.columns(null);
    tableBuilder.indexes(null);
    tableBuilder.foreignKeys(null);
    tableBuilder.checkConstraints(null);
    tableBuilder.dialect(null);
    thrown.expect(IllegalStateException.class);
    tableBuilder.autoBuild();
    tableBuilder.columns();
    Ddl.Builder ddlBuilder = Ddl.builder();
    ddlBuilder
        .createTable("Users")
        .column("id")
        .int64()
        .notNull()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .endTable();
    Table table = ddlBuilder.build().table("Users");
    assertTrue(table.equals(table));
    assertFalse(table.equals(Boolean.TRUE));
    ddlBuilder = Ddl.builder();
    ddlBuilder
        .createTable("Users")
        .column("id")
        .int64()
        .notNull()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .endTable();
    Table table1 = ddlBuilder.build().table("Users");
    assertTrue(table.equals(table1));
    tableBuilder.columns(ImmutableList.of());
    assertEquals(0, tableBuilder.columns().size());
  }

  @Test
  public void testForeignKeyBuilder() {
    ForeignKey.Builder foreignKeyBuilder = ForeignKey.builder();
    thrown.expect(NullPointerException.class);
    foreignKeyBuilder.name(null);
    foreignKeyBuilder.table(null);
    foreignKeyBuilder.referencedTable(null);
    foreignKeyBuilder.dialect(null);
    thrown.expect(IllegalStateException.class);
    foreignKeyBuilder.build();
    ForeignKey foreignKey =
        foreignKeyBuilder.name("fk").table("table1").referencedTable("table2").build();
    assertTrue(foreignKey.equals(foreignKey));
    assertFalse(foreignKey.equals(Boolean.TRUE));
    ForeignKey foreignKey1 =
        ForeignKey.builder().name("fk").table("table1").referencedTable("table2").build();
    assertTrue(foreignKey.equals(foreignKey1));
  }

  @Test
  public void testCheckConstraintBuilder() {
    CheckConstraint.Builder checkConstraintBuilder = CheckConstraint.builder();
    thrown.expect(NullPointerException.class);
    checkConstraintBuilder.name(null);
    checkConstraintBuilder.expression(null);
    checkConstraintBuilder.dialect(null);
    thrown.expect(IllegalStateException.class);
    checkConstraintBuilder.build();
    CheckConstraint checkConstraint = checkConstraintBuilder.name("ck").expression("1<2").build();
    assertTrue(checkConstraint.equals(checkConstraint));
    assertFalse(checkConstraint.equals(Boolean.TRUE));
    CheckConstraint checkConstraint1 = checkConstraintBuilder.name("ck").expression("1<2").build();
    assertTrue(checkConstraint.equals(checkConstraint1));
  }
}
