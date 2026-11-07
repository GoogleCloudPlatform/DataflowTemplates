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
import com.google.common.collect.ImmutableList;
import org.junit.Test;

/** Unit tests for {@link Column} class. */
public class ColumnTest {

  @Test
  public void pgColumnPrintsLocalityGroupAfterColumnConstraints() {
    Column column =
        Column.builder(Dialect.POSTGRESQL)
            .name("g")
            .pgInt8()
            .generatedAs("\"id\" + 1")
            .stored()
            .columnOptions(ImmutableList.of("locality_group='lg'"))
            .autoBuild();

    assertThat(
        column.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\"g\" bigint GENERATED ALWAYS AS (\"id\" + 1) STORED LOCALITY GROUP \"lg\""));
  }

  @Test
  public void pgColumnNormalizesLocalityGroupName() {
    assertThat(
        pgColumnWithOptions(ImmutableList.of("locality_group=\"lg\"")),
        equalToCompressingWhiteSpace("\"c\" bigint LOCALITY GROUP \"lg\""));
    assertThat(
        pgColumnWithOptions(ImmutableList.of("locality_group=lg")),
        equalToCompressingWhiteSpace("\"c\" bigint LOCALITY GROUP \"lg\""));
    assertThat(
        pgColumnWithOptions(ImmutableList.of("locality_group=x")),
        equalToCompressingWhiteSpace("\"c\" bigint LOCALITY GROUP \"x\""));
    assertThat(
        pgColumnWithOptions(ImmutableList.of("locality_group=NULL")),
        equalToCompressingWhiteSpace("\"c\" bigint LOCALITY GROUP NULL"));
    // Only a matching pair of quotes is stripped.
    assertThat(
        pgColumnWithOptions(ImmutableList.of("locality_group='lg")),
        equalToCompressingWhiteSpace("\"c\" bigint LOCALITY GROUP \"'lg\""));
  }

  @Test
  public void pgColumnIgnoresOtherOptions() {
    assertThat(
        pgColumnWithOptions(ImmutableList.of("allow_commit_timestamp=TRUE", "malformed_option")),
        equalToCompressingWhiteSpace("\"c\" bigint"));
  }

  @Test
  public void gsqlColumnPrintsOptionsLast() {
    Column column =
        Column.builder(Dialect.GOOGLE_STANDARD_SQL)
            .name("c")
            .string()
            .max()
            .columnOptions(ImmutableList.of("locality_group=\"lg\""))
            .autoBuild();

    assertThat(
        column.prettyPrint(),
        equalToCompressingWhiteSpace("`c` STRING(MAX) OPTIONS (locality_group=\"lg\")"));
  }

  @Test
  public void gsqlColumnSkipsBlankOptions() {
    Column column =
        Column.builder(Dialect.GOOGLE_STANDARD_SQL)
            .name("c")
            .string()
            .max()
            .columnOptions(ImmutableList.of(""))
            .autoBuild();

    assertThat(column.prettyPrint(), equalToCompressingWhiteSpace("`c` STRING(MAX)"));
  }

  private static String pgColumnWithOptions(ImmutableList<String> options) {
    return Column.builder(Dialect.POSTGRESQL)
        .name("c")
        .pgInt8()
        .columnOptions(options)
        .autoBuild()
        .prettyPrint();
  }
}
