/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner.ddl;

import static com.google.cloud.teleport.spanner.Matchers.equalsIgnoreWhitespace;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Type;
import org.junit.Test;

/**
 * Test coverage for {@link Ddl}.
 */
public class DdlTest {

  @Test
  public void emptyDb() {
    Ddl empty = Ddl.builder().build();
    assertThat(empty.allTables(), empty());
    assertThat(empty.prettyPrint(), equalTo(""));
  }

  @Test
  public void simple() {
    Ddl empty = Ddl.builder()
        .createTable("Users")
        .column("id").int64().notNull().endColumn()
        .column("first_name").string().size(10).endColumn()
        .column("last_name").type(Type.string()).max().endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .build();
    assertThat(empty.prettyPrint(), equalsIgnoreWhitespace("CREATE TABLE `Users` ("
        + "`id`                                    INT64 NOT NULL,"
        + "`first_name`                             STRING(10),"
        + "`last_name`                             STRING(MAX),"
        + ") PRIMARY KEY (`id` ASC)"));
  }

  @Test
  public void interleaves() {
    Ddl empty = Ddl.builder()
        .createTable("Users")
        .column("id").int64().notNull().endColumn()
        .column("first_name").string().size(10).endColumn()
        .column("last_name").type(Type.string()).max().endColumn()
        .primaryKey().asc("id").end()
        .endTable()
        .createTable("Account")
        .column("id").int64().notNull().endColumn()
        .column("balanceId").int64().notNull().endColumn()
        .column("balance").float64().notNull().endColumn()
        .primaryKey().asc("id").end()
        .interleaveInParent("Users")
        .endTable()
        .build();
    assertThat(empty.prettyPrint(), equalsIgnoreWhitespace("CREATE TABLE `Users` ("
        + "`id`                                    INT64 NOT NULL,"
        + "`first_name`                            STRING(10),"
        + "`last_name`                             STRING(MAX),) PRIMARY KEY (`id` ASC)"
        + "CREATE TABLE `Account` ("
        + "`id`                                    INT64 NOT NULL,"
        + "`balanceId`                             INT64 NOT NULL,"
        + "`balance`                               FLOAT64 NOT NULL,"
        + ") PRIMARY KEY (`id` ASC), INTERLEAVE IN PARENT `Users`"
    ));
  }
}
