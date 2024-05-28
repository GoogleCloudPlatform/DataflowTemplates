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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Test;

/** Unit tests for ProcessInformationSchema class. */
public class ProcessInformationSchemaTest {

  static Ddl getTestDdlWithGSqlDialect() {
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

  static Ddl getTestDdlWithPostgresDialect() {
    /* Creates DDL with 2 tables with the same fields but with different primary key
     * columns and their associated shadow tables.
     */
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int8_field")
            .pgInt8()
            .endColumn()
            .column("float8_field")
            .pgFloat8()
            .endColumn()
            .column("varchar_field")
            .pgVarchar()
            .max()
            .endColumn()
            .column("bytea_field")
            .pgBytea()
            .max()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("date_field")
            .pgDate()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int8_field")
            .asc("float8_field")
            .asc("varchar_field")
            .asc("bytea_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int8_field")
            .pgInt8()
            .endColumn()
            .column("float8_field")
            .pgFloat8()
            .endColumn()
            .column("varchar_field")
            .pgVarchar()
            .max()
            .endColumn()
            .column("bytea_field")
            .pgBytea()
            .max()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("date_field")
            .pgDate()
            .endColumn()
            .column("version")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int8_field")
            .asc("float8_field")
            .asc("varchar_field")
            .asc("bytea_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .createTable("Users_interleaved")
            .column("first_name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("last_name")
            .pgVarchar()
            .size(5)
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int8_field")
            .pgInt8()
            .endColumn()
            .column("float8_field")
            .pgFloat8()
            .endColumn()
            .column("varchar_field")
            .pgVarchar()
            .max()
            .endColumn()
            .column("bytea_field")
            .pgBytea()
            .max()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
            .endColumn()
            .column("date_field")
            .pgDate()
            .endColumn()
            .column("id")
            .pgInt8()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .asc("age")
            .asc("bool_field")
            .asc("int8_field")
            .asc("float8_field")
            .asc("varchar_field")
            .asc("bytea_field")
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
            spannerConfig, /* shouldCreateShadowTables= */ true, "shadow_", "oracle");
    Set<String> shadowTables =
        processInformationSchema.getShadowTablesInDdl(getTestDdlWithGSqlDialect());
    assertThat(shadowTables, is(new HashSet<String>(Arrays.asList("shadow_Users"))));
  }

  @Test
  public void canListDataTablesWithNoShadowTablesInDdl() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, /* shouldCreateShadowTables= */ true, "shadow_", "oracle");
    List<String> dataTablesWithNoShadowTables =
        processInformationSchema.getDataTablesWithNoShadowTables(getTestDdlWithGSqlDialect());
    assertThat(dataTablesWithNoShadowTables, is(Arrays.asList("Users_interleaved")));
  }

  @Test
  public void canCreateShadowTablesInSpanner() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    OperationFuture<Void, UpdateDatabaseDdlMetadata> operationFuture = mock(OperationFuture.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, /* shouldCreateShadowTables= */ true, "shadow_", "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);
    processInformationSchema.setSpannerAccessor(spannerAccessor);

    // Mock method calls
    when(databaseAdminClient.updateDatabaseDdl(anyString(), anyString(), any(), any()))
        .thenReturn(operationFuture);
    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(operationFuture.get(anyLong(), any())).thenReturn(null);
    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);

    processInformationSchema.createShadowTablesInSpanner(getTestDdlWithGSqlDialect());

    List<String> createShadowTableStatements =
        Collections.singletonList(
            "CREATE TABLE `shadow_Users_interleaved` (\n"
                + "\t`first_name`                            STRING(MAX),\n"
                + "\t`last_name`                             STRING(5),\n"
                + "\t`age`                                   INT64,\n"
                + "\t`bool_field`                            BOOL,\n"
                + "\t`int64_field`                           INT64,\n"
                + "\t`float64_field`                         FLOAT64,\n"
                + "\t`string_field`                          STRING(MAX),\n"
                + "\t`bytes_field`                           BYTES(MAX),\n"
                + "\t`timestamp_field`                       TIMESTAMP,\n"
                + "\t`date_field`                            DATE,\n"
                + "\t`id`                                    INT64,\n"
                + "\t`timestamp`                             INT64,\n"
                + "\t`log_file`                              STRING(MAX),\n"
                + "\t`log_position`                          INT64,\n"
                + ") PRIMARY KEY (`first_name` ASC, `last_name` DESC, `age` ASC, `bool_field` ASC, `int64_field` ASC, `float64_field` ASC, `string_field` ASC, `bytes_field` ASC, `timestamp_field` ASC, `date_field` ASC, `id` ASC)");
    // Verify method calls
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(
            eq("sample-value"), eq("sample-value"), eq(createShadowTableStatements), eq(null));
    verify(operationFuture, times(1)).get(anyLong(), any());
  }
}
