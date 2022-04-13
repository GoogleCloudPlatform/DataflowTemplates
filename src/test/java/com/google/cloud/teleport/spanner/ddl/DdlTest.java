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
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.common.Type;
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
