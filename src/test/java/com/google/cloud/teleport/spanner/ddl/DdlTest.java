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
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Test coverage for {@link Ddl}. */
public class DdlTest {

  @Test
  public void emptyDb() {
    Ddl empty = Ddl.builder().build();
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
  }

  @Test
  public void pgEmptyDb() {
    Ddl empty = Ddl.builder(Dialect.POSTGRESQL).build();
    assertEquals(empty.dialect(), Dialect.POSTGRESQL);
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
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
  }
}
