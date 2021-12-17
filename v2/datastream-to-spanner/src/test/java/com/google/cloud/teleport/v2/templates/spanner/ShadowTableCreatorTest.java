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

import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Table;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

/** Unit tests ShadowTableCreator class. */
public class ShadowTableCreatorTest {

  @Test
  public void canConstructShadowTableForOracle() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdl();

    ShadowTableCreator shadowTableCreator = new ShadowTableCreator("oracle", "shadow_");
    Table shadowTable = shadowTableCreator.constructShadowTable(testDdl, "Users_interleaved");

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
  public void canConstructShadowTableForMySql() {
    Ddl testDdl = ProcessInformationSchemaTest.getTestDdl();

    ShadowTableCreator shadowTableCreator = new ShadowTableCreator("mysql", "shadow_");
    Table shadowTable = shadowTableCreator.constructShadowTable(testDdl, "Users_interleaved");

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
}
