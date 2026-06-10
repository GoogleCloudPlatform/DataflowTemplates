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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.mockito.Mockito;

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
            spannerConfig,
            spannerConfig,
            /* shouldCreateShadowTables= */ true,
            "shadow_",
            "oracle");
    Set<String> shadowTables =
        processInformationSchema.getShadowTablesInDdl(getTestDdlWithGSqlDialect());
    assertThat(shadowTables, is(new HashSet<String>(Arrays.asList("shadow_Users"))));
  }

  @Test
  public void canListDataTablesWithNoShadowTablesInDdl() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig,
            spannerConfig,
            /* shouldCreateShadowTables= */ true,
            "shadow_",
            "oracle");
    List<String> dataTablesWithNoShadowTables =
        processInformationSchema.getDataTablesWithNoShadowTables(
            getTestDdlWithGSqlDialect(), getTestDdlWithGSqlDialect());
    assertThat(dataTablesWithNoShadowTables, is(Arrays.asList("Users_interleaved")));
  }

  @Test
  public void canListDataTablesWithNoShadowTables_separateDatabase() {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig,
            spannerConfig,
            /* shouldCreateShadowTables= */ true,
            "shadow_",
            "oracle");

    List<String> dataTablesWithNoShadowTables =
        processInformationSchema.getDataTablesWithNoShadowTables(
            getMiniMainDdl(), getMiniShadowDdl());
    assertThat(dataTablesWithNoShadowTables, is(Arrays.asList("table2")));
  }

  @Test
  public void canCreateShadowTablesInSpanner() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    OperationFuture<Void, UpdateDatabaseDdlMetadata> operationFuture = mock(OperationFuture.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, /* shouldCreateShadowTables= */ true, "shadow_", "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);
    processInformationSchema.setshadowTableSpannerAccessor(spannerAccessor);

    // Mock method calls
    when(databaseAdminClient.updateDatabaseDdl(anyString(), anyString(), any(), any()))
        .thenReturn(operationFuture);
    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(operationFuture.get(anyLong(), any())).thenReturn(null);
    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);

    processInformationSchema.createShadowTablesInSpanner(
        getTestDdlWithGSqlDialect(), getTestDdlWithGSqlDialect());

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

  @Test
  public void canCreateShadowTablesInSpanner_separateDb() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor shadowSpannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    OperationFuture<Void, UpdateDatabaseDdlMetadata> operationFuture = mock(OperationFuture.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig,
            shadowSpannerConfig,
            /* shouldCreateShadowTables= */ true,
            "shadow_",
            "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);
    processInformationSchema.setshadowTableSpannerAccessor(shadowSpannerAccessor);

    // Mock method calls
    when(databaseAdminClient.updateDatabaseDdl(anyString(), anyString(), any(), any()))
        .thenReturn(operationFuture);
    when(shadowSpannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(operationFuture.get(anyLong(), any())).thenReturn(null);
    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(shadowSpannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);

    processInformationSchema.createShadowTablesInSpanner(getMiniMainDdl(), getMiniShadowDdl());

    List<String> createShadowTableStatements =
        Collections.singletonList(
            "CREATE TABLE `shadow_table2` (\n"
                + "\t`id`                                    INT64,\n"
                + "\t`timestamp`                             INT64,\n"
                + "\t`log_file`                              STRING(MAX),\n"
                + "\t`log_position`                          INT64,\n"
                + ") PRIMARY KEY (`id` ASC)");
    // Verify method calls
    verify(databaseAdminClient, times(1))
        .updateDatabaseDdl(
            eq("sample-value"), eq("sample-value"), eq(createShadowTableStatements), eq(null));
    verify(operationFuture, times(1)).get(anyLong(), any());
  }

  private Ddl getMiniShadowDdl() {
    // Create shadow DDL with 1 shadow table
    return Ddl.builder()
        .createTable("shadow_table1")
        .column("id")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .endTable()
        .build();
  }

  private Ddl getMiniMainDdl() {
    // Create main DDL with 2 tables
    return Ddl.builder()
        .createTable("table1")
        .column("id")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .endTable()
        .createTable("table2")
        .column("id")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("id")
        .end()
        .endTable()
        .build();
  }

  @Test
  public void testCleanupDdl_sameDatabaseScenario() {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);

    // Create test DDL with both main and shadow tables
    Ddl testDdl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("customers")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_users")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_customers")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    // Test cleanup for main DDL (should only keep non-shadow tables)
    Ddl cleanedMainDdl = processInformationSchema.cleanupDdl(testDdl, "shadow_", true);
    assertThat(
        cleanedMainDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toSet()),
        is(new HashSet<>(Arrays.asList("users", "customers"))));

    // Test cleanup for shadow DDL (should only keep shadow tables)
    Ddl cleanedShadowDdl = processInformationSchema.cleanupDdl(testDdl, "shadow_", false);
    assertThat(
        cleanedShadowDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toSet()),
        is(new HashSet<>(Arrays.asList("shadow_users", "shadow_customers"))));
  }

  @Test
  public void testCleanupDdl_differentDatabaseScenario() {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);

    // Create main DDL with an extra shadow table that shouldn't be there
    Ddl mainDdl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_unexpected")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    // Create shadow DDL with an extra non-shadow table that shouldn't be there
    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_users")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("unexpected_table")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    // Test cleanup for main DDL (should remove the unexpected shadow table)
    Ddl cleanedMainDdl = processInformationSchema.cleanupDdl(mainDdl, "shadow_", true);
    assertThat(
        cleanedMainDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toSet()),
        is(new HashSet<>(Collections.singletonList("users"))));

    // Test cleanup for shadow DDL (should remove the unexpected non-shadow table)
    Ddl cleanedShadowDdl = processInformationSchema.cleanupDdl(shadowDdl, "shadow_", false);
    assertThat(
        cleanedShadowDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toSet()),
        is(new HashSet<>(Collections.singletonList("shadow_users"))));
  }

  @Test
  public void testProcessElement() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");

    ProcessInformationSchema.ProcessInformationSchemaFn spyFn =
        Mockito.spy(processInformationSchema);

    Ddl mainDdl = getMiniMainDdl();
    Ddl shadowDdl = getMiniShadowDdl();

    doReturn(mainDdl)
        .doReturn(shadowDdl)
        .doReturn(shadowDdl)
        .when(spyFn)
        .getInformationSchemaAsDdl(any());

    doNothing().when(spyFn).createShadowTablesInSpanner(any(), any());

    DoFn.ProcessContext mockProcessContext = mock(DoFn.ProcessContext.class);

    spyFn.setDialect(Dialect.GOOGLE_STANDARD_SQL);

    spyFn.processElement(mockProcessContext);

    verify(mockProcessContext).output(eq(ProcessInformationSchema.MAIN_DDL_TAG), any(Ddl.class));
    verify(mockProcessContext)
        .output(eq(ProcessInformationSchema.SHADOW_TABLE_DDL_TAG), any(Ddl.class));
  }

  @Test
  public void testSetupWithMocks() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    Database database = mock(Database.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setSpannerAccessor(spannerAccessor);

    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);

    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(databaseAdminClient.getDatabase("sample-value", "sample-value")).thenReturn(database);
    when(database.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    processInformationSchema.setup();

    verify(spannerAccessor, times(1)).getDatabaseAdminClient();
  }

  @Test
  public void testSetupWithMocks_SeparateDb() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerAccessor shadowSpannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);
    Database database = mock(Database.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, shadowSpannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setSpannerAccessor(spannerAccessor);
    processInformationSchema.setshadowTableSpannerAccessor(shadowSpannerAccessor);

    ValueProvider<String> sampleValueProvider1 =
        ValueProvider.StaticValueProvider.of("sample-value-1");
    ValueProvider<String> sampleValueProvider2 =
        ValueProvider.StaticValueProvider.of("sample-value-2");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider1);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider1);
    when(shadowSpannerConfig.getInstanceId()).thenReturn(sampleValueProvider2);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(sampleValueProvider2);

    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(databaseAdminClient.getDatabase("sample-value-1", "sample-value-1")).thenReturn(database);
    when(database.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    processInformationSchema.setup();

    verify(spannerAccessor, times(1)).getDatabaseAdminClient();
  }

  @Test
  public void testGetInformationSchemaAsDdl() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    BatchClient batchClient = mock(BatchClient.class);
    BatchReadOnlyTransaction context = mock(BatchReadOnlyTransaction.class);
    InformationSchemaScanner scanner = mock(InformationSchemaScanner.class);
    Ddl expectedDdl = mock(Ddl.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");

    ProcessInformationSchema.ProcessInformationSchemaFn spyFn =
        Mockito.spy(processInformationSchema);

    when(spannerAccessor.getBatchClient()).thenReturn(batchClient);
    when(batchClient.batchReadOnlyTransaction(any(TimestampBound.class))).thenReturn(context);
    doReturn(scanner).when(spyFn).getInformationSchemaScanner(eq(context), any());
    when(scanner.scan()).thenReturn(expectedDdl);

    Ddl actualDdl = spyFn.getInformationSchemaAsDdl(spannerAccessor);

    assertThat(actualDdl, is(expectedDdl));
  }

  @Test
  public void testProcessElement_shouldNotCreateShadowTables() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, false, "shadow_", "mysql");

    ProcessInformationSchema.ProcessInformationSchemaFn spyFn =
        Mockito.spy(processInformationSchema);

    Ddl mainDdl = getMiniMainDdl();
    Ddl shadowDdl = getMiniShadowDdl();

    doReturn(mainDdl).doReturn(shadowDdl).when(spyFn).getInformationSchemaAsDdl(any());

    DoFn.ProcessContext mockProcessContext = mock(DoFn.ProcessContext.class);

    spyFn.setDialect(Dialect.GOOGLE_STANDARD_SQL);

    spyFn.processElement(mockProcessContext);

    verify(spyFn, times(0)).createShadowTablesInSpanner(any(), any());
    verify(mockProcessContext).output(eq(ProcessInformationSchema.MAIN_DDL_TAG), any(Ddl.class));
    verify(mockProcessContext)
        .output(eq(ProcessInformationSchema.SHADOW_TABLE_DDL_TAG), any(Ddl.class));
  }

  @Test
  public void canCreateShadowTablesInSpanner_noOp() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, /* shouldCreateShadowTables= */ true, "shadow_", "mysql");
    processInformationSchema.setDialect(Dialect.GOOGLE_STANDARD_SQL);
    processInformationSchema.setshadowTableSpannerAccessor(spannerAccessor);

    Ddl mainDdl = getMiniMainDdl();
    // Create shadow DDL that already has shadow tables for both table1 and table2
    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_table1")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_table2")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    processInformationSchema.createShadowTablesInSpanner(mainDdl, shadowDdl);

    // Verify that updateDatabaseDdl was NEVER called because no new shadow tables are needed
    verify(databaseAdminClient, times(0)).updateDatabaseDdl(anyString(), anyString(), any(), any());
  }

  @Test
  public void testTeardown() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerAccessor shadowSpannerAccessor = mock(SpannerAccessor.class);
    Database database = mock(Database.class);
    DatabaseAdminClient databaseAdminClient = mock(DatabaseAdminClient.class);

    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);
    when(spannerAccessor.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(databaseAdminClient.getDatabase("sample-value", "sample-value")).thenReturn(database);
    when(database.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setSpannerAccessor(spannerAccessor);

    // Same database scenario
    processInformationSchema.setup(); // This will set useSeparateShadowTableDb to false
    processInformationSchema.teardown();

    verify(spannerAccessor, times(1)).close();

    // Different database scenario
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    ValueProvider<String> sampleValueProvider2 =
        ValueProvider.StaticValueProvider.of("sample-value-2");
    when(shadowSpannerConfig.getInstanceId()).thenReturn(sampleValueProvider2);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(sampleValueProvider2);

    processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, shadowSpannerConfig, true, "shadow_", "mysql");
    processInformationSchema.setSpannerAccessor(spannerAccessor);
    processInformationSchema.setshadowTableSpannerAccessor(shadowSpannerAccessor);

    processInformationSchema.setup(); // This will set useSeparateShadowTableDb to true
    processInformationSchema.teardown();

    verify(spannerAccessor, times(2)).close(); // Called again
    verify(shadowSpannerAccessor, times(1)).close();
  }

  @Test
  public void testSetupWithoutInjectedAccessor() throws Exception {
    SpannerConfig spannerConfig = mock(SpannerConfig.class);

    ValueProvider<String> sampleValueProvider =
        ValueProvider.StaticValueProvider.of("sample-value");
    when(spannerConfig.getInstanceId()).thenReturn(sampleValueProvider);
    when(spannerConfig.getDatabaseId()).thenReturn(sampleValueProvider);

    ProcessInformationSchema.ProcessInformationSchemaFn processInformationSchema =
        new ProcessInformationSchema.ProcessInformationSchemaFn(
            spannerConfig, spannerConfig, true, "shadow_", "mysql");

    // Expected to fail because SpannerAccessor.getOrCreate will fail to create a client
    // without real credentials or project ID set up in environment.
    // But it should still exercise the branch `if (this.spannerAccessor == null)`!
    assertThrows(Exception.class, () -> processInformationSchema.setup());
  }
}
