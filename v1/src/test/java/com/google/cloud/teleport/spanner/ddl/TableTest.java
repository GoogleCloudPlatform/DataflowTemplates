/*
 * Copyright (C) 2026 Google LLC
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.ddl.Table.InterleaveType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

/** Unit tests for {@link Table} class. */
public class TableTest {

  @Test
  public void pgTablePrintsLocalityGroupBeforeInterleave() {
    assertThat(
        interleavedTable(Dialect.POSTGRESQL, ImmutableList.of("locality_group='lg'")).prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE \"child\" ( \"id\" bigint NOT NULL, PRIMARY KEY (\"id\") )"
                + " LOCALITY GROUP \"lg\" INTERLEAVE IN PARENT \"parent\" ON DELETE CASCADE"));
  }

  @Test
  public void gsqlTablePrintsOptionsAfterInterleave() {
    assertThat(
        interleavedTable(Dialect.GOOGLE_STANDARD_SQL, ImmutableList.of("locality_group=\"lg\""))
            .prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `child` ( `id` INT64 NOT NULL, ) PRIMARY KEY (`id` ASC),"
                + " INTERLEAVE IN PARENT `parent` ON DELETE CASCADE,"
                + " OPTIONS (locality_group=\"lg\")"));
  }

  @Test
  public void pgTableNormalizesLocalityGroupName() {
    assertThat(
        pgTableWithOptions(ImmutableList.of("locality_group=\"lg\"")),
        equalToCompressingWhiteSpace(pgTablePrefix() + " LOCALITY GROUP \"lg\""));
    assertThat(
        pgTableWithOptions(ImmutableList.of("locality_group=lg")),
        equalToCompressingWhiteSpace(pgTablePrefix() + " LOCALITY GROUP \"lg\""));
    assertThat(
        pgTableWithOptions(ImmutableList.of("locality_group=x")),
        equalToCompressingWhiteSpace(pgTablePrefix() + " LOCALITY GROUP \"x\""));
    assertThat(
        pgTableWithOptions(ImmutableList.of("locality_group=NULL")),
        equalToCompressingWhiteSpace(pgTablePrefix() + " LOCALITY GROUP NULL"));
    // Only a matching pair of quotes is stripped.
    assertThat(
        pgTableWithOptions(ImmutableList.of("locality_group='lg")),
        equalToCompressingWhiteSpace(pgTablePrefix() + " LOCALITY GROUP \"'lg\""));
  }

  @Test
  public void pgTableIgnoresOtherOptions() {
    assertThat(
        pgTableWithOptions(ImmutableList.of("other_option='value'", "malformed_option")),
        equalToCompressingWhiteSpace(pgTablePrefix()));
  }

  @Test
  public void tableWithoutOptionsHasNoLocalityGroupClause() {
    assertThat(pgTableWithOptions(null), equalToCompressingWhiteSpace(pgTablePrefix()));
    assertThat(
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("t")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .options(null)
            .endTable()
            .build()
            .table("t")
            .prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `t` ( `id` INT64 NOT NULL, ) PRIMARY KEY (`id` ASC)"));
  }

  private static Table interleavedTable(Dialect dialect, ImmutableList<String> childOptions) {
    Ddl.Builder builder = Ddl.builder(dialect);
    Table.Builder parent = builder.createTable("parent");
    Table.Builder child = builder.createTable("child");
    if (dialect == Dialect.POSTGRESQL) {
      parent.column("id").pgInt8().notNull().endColumn();
      child.column("id").pgInt8().notNull().endColumn();
    } else {
      parent.column("id").int64().notNull().endColumn();
      child.column("id").int64().notNull().endColumn();
    }
    parent.primaryKey().asc("id").end().endTable();
    child
        .primaryKey()
        .asc("id")
        .end()
        .interleaveInParent("parent")
        .interleaveType(InterleaveType.IN_PARENT)
        .onDeleteCascade()
        .options(childOptions)
        .endTable();
    return builder.build().table("child");
  }

  private static String pgTablePrefix() {
    return "CREATE TABLE \"t\" ( \"id\" bigint NOT NULL, PRIMARY KEY (\"id\") )";
  }

  private static String pgTableWithOptions(ImmutableList<String> options) {
    return Ddl.builder(Dialect.POSTGRESQL)
        .createTable("t")
        .column("id")
        .pgInt8()
        .notNull()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .options(options)
        .endTable()
        .build()
        .table("t")
        .prettyPrint();
  }
}
