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
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Test;

/** Unit tests for ProcessInformationSchema class. */
public class ProcessInformationSchemaTest {

  static Ddl getTestDdl() {
    /* Creates DDL with 2 tables with the same fields but with different primary key
     * columns and their associated shadow tables.
     */
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int64_field")
            .asc("float64_field")
            .asc("string_field")
            .asc("bytes_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("version")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int64_field")
            .asc("float64_field")
            .asc("string_field")
            .asc("bytes_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .createTable("Users_interleaved")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int64_field")
            .asc("float64_field")
            .asc("string_field")
            .asc("bytes_field")
            .asc("timestamp_field")
            .asc("date_field")
            .asc("id")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  @Test
  public void canListShadowTablesInDdl() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, /*shouldCreateShadowTables=*/ true, "shadow_", "oracle");
    Set<String> shadowTables = processInformationSchema.getShadowTablesInDdl(getTestDdl());
    assertThat(shadowTables, is(new HashSet<String>(Arrays.asList("shadow_Users"))));
  }

  @Test
  public void canListDataTablesWithNoShadowTablesInDdl() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, /*shouldCreateShadowTables=*/ true, "shadow_", "oracle");
    List<String> dataTablesWithNoShadowTables =
        processInformationSchema.getDataTablesWithNoShadowTables(getTestDdl());
    assertThat(dataTablesWithNoShadowTables, is(Arrays.asList("Users_interleaved")));
  }
}
