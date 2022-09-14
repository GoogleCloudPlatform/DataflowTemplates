/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.IndexColumn.IndexColumnsBuilder;
import com.google.cloud.teleport.spanner.ddl.IndexColumn.Order;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;

/** Test coverage for {@link Ddl}. */
public class DdlTest {

  @Test
  public void emptyDb() {
    Ddl empty = Ddl.builder().build();
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
    assertTrue(empty.equals(empty));
    assertNotNull(empty.hashCode());
  }

  @Test
  public void pgEmptyDb() {
    Ddl empty = Ddl.builder(Dialect.POSTGRESQL).build();
    assertEquals(empty.dialect(), Dialect.POSTGRESQL);
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
    assertFalse(empty.equals(null));
    assertFalse(empty.equals(Boolean.TRUE));
    assertNotNull(empty.hashCode());
  }

  @Test
  public void simple() {
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
    Export export =
        Export.newBuilder()
            .addDatabaseOptions(
                Export.DatabaseOption.newBuilder()
                    .setOptionName("version_retention_period")
                    .setOptionValue("4d")
                    .build())
            .build();
    builder.mergeDatabaseOptions(export.getDatabaseOptionsList());
    Ddl ddl = builder.build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "ALTER DATABASE `%db_name%` SET OPTIONS ( version_retention_period = 4d )"
                + " CREATE TABLE `Users` ("
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
    assertEquals(4, statements.size());
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
    assertThat(
        statements.get(3),
        equalToCompressingWhiteSpace(
            "ALTER DATABASE `%db_name%` SET OPTIONS ( version_retention_period = 4d )"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void pgSimple() {
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
        .defaultExpression("John")
        .endColumn()
        .column("last_name")
        .type(Type.pgVarchar())
        .max()
        .defaultExpression("Lennon")
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
    Export export =
        Export.newBuilder()
            .addDatabaseOptions(
                Export.DatabaseOption.newBuilder()
                    .setOptionName("version_retention_period")
                    .setOptionValue("4d")
                    .setOptionType("STRING")
                    .build())
            .build();
    builder.mergeDatabaseOptions(export.getDatabaseOptionsList());
    Ddl ddl = builder.build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "ALTER DATABASE \"%db_name%\" SET spanner.version_retention_period = '4d'"
                + " CREATE TABLE \"Users\" ("
                + " \"id\" bigint NOT NULL,"
                + " \"first_name\" character varying(10) DEFAULT John,"
                + " \"last_name\" character varying DEFAULT Lennon,"
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
  public void interleaves() {
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
  public void pgInterleaves() {
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
  public void testDatabaseOptions() {
    Ddl.Builder builder = Ddl.builder();
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("version_retention_period")
            .setOptionValue("4d")
            .build());
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("optimizer_version")
            .setOptionValue("2")
            .build());
    builder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = builder.build();
    List<String> optionStatements = ddl.setOptionsStatements("database_id");
    assertThat(optionStatements.size(), is(1));
    assertThat(
        optionStatements.get(0),
        is("ALTER DATABASE `database_id` SET OPTIONS ( version_retention_period = 4d )"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void pgTestDatabaseOptions() {
    Ddl.Builder builder = Ddl.builder(Dialect.POSTGRESQL);
    List<Export.DatabaseOption> dbOptionList = new ArrayList<>();
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("version_retention_period")
            .setOptionValue("4d")
            .setOptionValue("4d")
            .setOptionType("STRING")
            .build());
    dbOptionList.add(
        Export.DatabaseOption.newBuilder()
            .setOptionName("optimizer_version")
            .setOptionValue("2")
            .build());
    builder.mergeDatabaseOptions(dbOptionList);
    Ddl ddl = builder.build();
    List<String> optionStatements = ddl.setOptionsStatements("database_id");
    assertThat(optionStatements.size(), is(1));
    assertThat(
        optionStatements.get(0),
        is("ALTER DATABASE \"database_id\" SET spanner.version_retention_period = '4d'"));
    assertNotNull(ddl.hashCode());
  }

  @Test
  public void pgTestIndex() {
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
  public void pgTestIndexNullsOrder() {
    Index.Builder builder =
        Index.builder(Dialect.POSTGRESQL).name("user_index").table("User").unique();
    builder
        .columns()
        .create()
        .name("first_name")
        .asc()
        .nullsFirst()
        .endIndexColumn()
        .create()
        .name("last_name")
        .desc()
        .nullsLast()
        .endIndexColumn()
        .create()
        .name("first_nick_name")
        .asc()
        .nullsLast()
        .endIndexColumn()
        .create()
        .name("last_nick_name")
        .desc()
        .nullsFirst()
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
            "CREATE UNIQUE INDEX \"user_index\" ON \"User\"(\"first_name\" ASC NULLS FIRST,"
                + " \"last_name\" DESC NULLS LAST, \"first_nick_name\" ASC NULLS LAST,"
                + " \"last_nick_name\" DESC NULLS FIRST) INCLUDE (\"full_name\")"));
    assertNotNull(index.hashCode());
  }

  @Test
  public void pgTestCheckConstraint() {
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
  public void pgTestForeignKey() {
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
  public void testView() {
    View view = View.builder().name("user_view").query("SELECT * FROM `User`").build();
    assertThat(
        view.prettyPrint(),
        equalToCompressingWhiteSpace("CREATE VIEW `user_view` AS SELECT * FROM `User`"));
  }

  @Test
  public void pgTestView() {
    View view =
        View.builder(Dialect.POSTGRESQL)
            .name("user_view")
            .query("SELECT * FROM \"User\"")
            .security(View.SqlSecurity.INVOKER)
            .build();
    assertThat(
        view.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE VIEW \"user_view\" SQL SECURITY INVOKER AS SELECT * FROM \"User\""));
    Ddl.Builder ddlBuilder = Ddl.builder();
    ddlBuilder.addView(view);
    Ddl ddl = ddlBuilder.build();
    assertEquals(view, ddl.view("user_view"));
    List<String> statements = ddl.statements();
    assertEquals(1, statements.size());
    assertEquals(
        "CREATE VIEW \"user_view\" SQL SECURITY INVOKER AS SELECT * FROM \"User\"",
        statements.get(0));
    assertNotNull(view.hashCode());
  }

  @Test
  public void changeStreams() {
    Ddl ddl =
        Ddl.builder()
            .createChangeStream("ChangeStreamAll")
            .forClause("FOR ALL")
            .options(
                ImmutableList.of(
                    "retention_period=\"7d\"", "value_capture_type=\"OLD_AND_NEW_VALUES\""))
            .endChangeStream()
            .createChangeStream("ChangeStreamEmpty")
            .endChangeStream()
            .createChangeStream("ChangeStreamTableColumns")
            .forClause("FOR `T1`, `T2`(`c1`, `c2`), `T3`()")
            .endChangeStream()
            .build();
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE CHANGE STREAM `ChangeStreamAll`"
                + " FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")"
                + " CREATE CHANGE STREAM `ChangeStreamEmpty`"
                + " CREATE CHANGE STREAM `ChangeStreamTableColumns`"
                + " FOR `T1`, `T2`(`c1`, `c2`), `T3`()"));

    List<String> statements = ddl.statements();
    assertEquals(3, statements.size());
    assertThat(
        statements.get(0),
        equalToCompressingWhiteSpace(
            "CREATE CHANGE STREAM `ChangeStreamAll`"
                + " FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")"));
    assertThat(
        statements.get(1),
        equalToCompressingWhiteSpace(" CREATE CHANGE STREAM `ChangeStreamEmpty`"));
    assertThat(
        statements.get(2),
        equalToCompressingWhiteSpace(
            " CREATE CHANGE STREAM `ChangeStreamTableColumns`"
                + " FOR `T1`, `T2`(`c1`, `c2`), `T3`()"));
    assertNotNull(ddl.hashCode());
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

  @Test
  public void testIndexColumnBuilder() {
    IndexColumnsBuilder indexColumnsBuilder = new IndexColumnsBuilder(null, null);
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.name("name"));
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.asc());
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.desc());
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.storing());
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.nullsFirst());
    assertThrows(IllegalArgumentException.class, () -> indexColumnsBuilder.nullsLast());

    IndexColumn.Builder indexColumnBuilder = new AutoValue_IndexColumn.Builder();
    assertThrows(NullPointerException.class, () -> indexColumnBuilder.name(null));
    assertThrows(NullPointerException.class, () -> indexColumnBuilder.order(null));
    assertThrows(NullPointerException.class, () -> indexColumnBuilder.dialect(null));
    assertThrows(IllegalStateException.class, () -> indexColumnBuilder.autoBuild());
    IndexColumn indexColumn =
        indexColumnBuilder
            .name("col1")
            .order(Order.ASC)
            .dialect(Dialect.GOOGLE_STANDARD_SQL)
            .autoBuild();
    assertTrue(indexColumn.equals(indexColumn));
    assertFalse(indexColumn.equals(Boolean.TRUE));
    IndexColumn indexColumn1 = IndexColumn.create("col1", Order.ASC);
    assertTrue(indexColumn.equals(indexColumn1));
  }

  @Test
  public void testColumnBuilder() {
    Column.Builder columnBuilder = Column.builder();
    assertThrows(NullPointerException.class, () -> columnBuilder.name(null));
    assertThrows(NullPointerException.class, () -> columnBuilder.type(null));
    assertThrows(NullPointerException.class, () -> columnBuilder.columnOptions(null));
    assertThrows(NullPointerException.class, () -> columnBuilder.dialect(null));
    assertThrows(NullPointerException.class, () -> columnBuilder.name(null));
    assertThrows(NullPointerException.class, () -> columnBuilder.name(null));
    assertThrows(IllegalStateException.class, () -> columnBuilder.autoBuild());
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
    column1 =
        Column.builder().name("colName").type(Type.string()).defaultExpression("1+2").autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).isStored(true).autoBuild();
    assertFalse(column.equals(column1));
    column1 = Column.builder().name("colName").type(Type.string()).autoBuild();
    assertTrue(column.equals(column1));
  }

  @Test
  public void testTableBuilder() {
    Table.Builder tableBuilder = Table.builder();
    assertThrows(NullPointerException.class, () -> tableBuilder.primaryKeys(null));
    assertThrows(NullPointerException.class, () -> tableBuilder.columns(null));
    assertThrows(NullPointerException.class, () -> tableBuilder.indexes(null));
    assertThrows(NullPointerException.class, () -> tableBuilder.foreignKeys(null));
    assertThrows(NullPointerException.class, () -> tableBuilder.checkConstraints(null));
    assertThrows(NullPointerException.class, () -> tableBuilder.dialect(null));
    assertThrows(IllegalStateException.class, () -> tableBuilder.autoBuild());
    assertThrows(IllegalStateException.class, () -> tableBuilder.columns());
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
    assertThrows(NullPointerException.class, () -> foreignKeyBuilder.name(null));
    assertThrows(NullPointerException.class, () -> foreignKeyBuilder.table(null));
    assertThrows(NullPointerException.class, () -> foreignKeyBuilder.referencedTable(null));
    assertThrows(NullPointerException.class, () -> foreignKeyBuilder.dialect(null));
    assertThrows(IllegalStateException.class, () -> foreignKeyBuilder.build());
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
    assertThrows(NullPointerException.class, () -> checkConstraintBuilder.name(null));
    assertThrows(NullPointerException.class, () -> checkConstraintBuilder.expression(null));
    assertThrows(NullPointerException.class, () -> checkConstraintBuilder.dialect(null));
    assertThrows(IllegalStateException.class, () -> checkConstraintBuilder.build());
    CheckConstraint checkConstraint = checkConstraintBuilder.name("ck").expression("1<2").build();
    assertTrue(checkConstraint.equals(checkConstraint));
    assertFalse(checkConstraint.equals(Boolean.TRUE));
    CheckConstraint checkConstraint1 = checkConstraintBuilder.name("ck").expression("1<2").build();
    assertTrue(checkConstraint.equals(checkConstraint1));
  }
}
