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
package com.google.cloud.teleport.v2.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior
    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, TableConfiguration.empty());

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table1' as __tableName__ FROM `Table1`"));
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table2' as __tableName__ FROM `Table2`"));
  }

  @Test
  public void testProcessElementPostgres() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior for Postgres
    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.POSTGRESQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, TableConfiguration.empty());

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments (expecting double quotes for Postgres)
    verify(context)
        .output(
            ReadOperation.create()
                .withQuery("SELECT *, 'Table1' as __tableName__ FROM \"Table1\""));
    verify(context)
        .output(
            ReadOperation.create()
                .withQuery("SELECT *, 'Table2' as __tableName__ FROM \"Table2\""));
  }

  @Test
  public void testProcessElementWithConfiguredSubset() {
    // Spanner DDL contains TableA, TableB, TableC. The config specifies TableA, TableC.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference())
        .thenReturn(ImmutableList.of("TableA", "TableB", "TableC"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTables("TableA,TableC");
    TableConfiguration tableConfig = TableConfiguration.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Only TableA and TableC ReadOperations are generated. TableB is skipped.
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'TableA' as __tableName__ FROM `TableA`"));
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'TableC' as __tableName__ FROM `TableC`"));
  }

  @Test
  public void testProcessElementWithMissingSpannerTable() {
    // Configured Table Missing in Spanner: DDL contains TableA, TableB. Config specifies TableA,
    // TableC.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("TableA", "TableB"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTables("TableA,TableC");
    TableConfiguration tableConfig = TableConfiguration.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(1)).output(argument.capture());

    // Only TableA is queried. TableC is naturally skipped because it's not in the DDL.
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'TableA' as __tableName__ FROM `TableA`"));
  }

  @Test
  public void testProcessElementCompleteMismatch() {
    // DDL contains TableA. Config specifies TableB.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("TableA"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTables("TableB");
    TableConfiguration tableConfig = TableConfiguration.parseFromOptions(options);

    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, IdentityMapper::new, tableConfig);

    doFn.processElement(context);

    // Completes successfully with zero ReadOperations output.
    verify(context, org.mockito.Mockito.never()).output(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testProcessElementWithSchemaMapper() {
    // Table Config specifies source_table which was renamed to spanner_table in Spanner.
    // SchemaMapper should successfully map spanner_table to source_table.
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    when(ddl.dialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("spanner_table"));
    when(context.sideInput(ddlView)).thenReturn(ddl);

    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTables("source_table");
    TableConfiguration tableConfig = TableConfiguration.parseFromOptions(options);

    ISchemaMapper mockMapper = mock(ISchemaMapper.class);
    when(mockMapper.getSourceTableName(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq("spanner_table")))
        .thenReturn("source_table");

    CreateSpannerReadOpsFn doFn =
        new CreateSpannerReadOpsFn(ddlView, (d) -> mockMapper, tableConfig);

    doFn.processElement(context);

    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(1)).output(argument.capture());

    verify(context)
        .output(
            ReadOperation.create()
                .withQuery("SELECT *, 'spanner_table' as __tableName__ FROM `spanner_table`"));
  }

  @Test
  public void testShardIdColumnFilterGoogleSqlBindsShardIdsAsOneArrayParameter() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "Users");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), eq("Users"))).thenReturn("Users");
    when(mapper.getShardIdColumnName(anyString(), eq("Users"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setShardIds("s1,s2");
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
            .buildReadOperations(ddl, mapper);

    Statement expected =
        Statement.newBuilder(
                "SELECT *, 'Users' as __tableName__ FROM `Users`"
                    + " WHERE `migration_shard_id` IN UNNEST(@p1)")
            .bind("p1")
            .toStringArray(Arrays.asList("s1", "s2"))
            .build();
    assertEquals(Collections.singletonList(ReadOperation.create().withQuery(expected)), result);
  }

  @Test
  public void testShardIdColumnFilterPostgresBindsShardIdsAsOneArrayParameter() {
    Ddl ddl = ddl(Dialect.POSTGRESQL, "Users");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), eq("Users"))).thenReturn("Users");
    when(mapper.getShardIdColumnName(anyString(), eq("Users"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setShardIds("s1,s2");
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
            .buildReadOperations(ddl, mapper);

    Statement expected =
        Statement.newBuilder(
                "SELECT *, 'Users' as __tableName__ FROM \"Users\""
                    + " WHERE \"migration_shard_id\" = ANY($1)")
            .bind("p1")
            .toStringArray(Arrays.asList("s1", "s2"))
            .build();
    assertEquals(Collections.singletonList(ReadOperation.create().withQuery(expected)), result);
  }

  @Test
  public void testSeveralTablesWithoutShardIdColumnReportedInOneError() {
    // Only Users has a shard ID column; every offender is reported in one error, sorted.
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "BetaTable", "Users", "AlphaTable");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), anyString())).thenAnswer(inv -> inv.getArgument(1));
    when(mapper.getShardIdColumnName(anyString(), eq("Users"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setShardIds("s1");
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
                    .buildReadOperations(ddl, mapper));
    String message = e.getMessage();
    assertTrue(message, message.contains("AlphaTable"));
    assertTrue(message, message.contains("BetaTable"));
    assertTrue(message, message.indexOf("AlphaTable") < message.indexOf("BetaTable"));
    assertFalse(message, message.contains("Users"));
  }

  @Test
  public void testShardIdsSkipsUnmappableSpannerTable() {
    // SpannerOnly has no source table and no shard ID column; with --shardIds it's skipped
    // silently.
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "Users", "SpannerOnly");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), eq("Users"))).thenReturn("Users");
    when(mapper.getSourceTableName(anyString(), eq("SpannerOnly")))
        .thenThrow(new NoSuchElementException("no source table for SpannerOnly"));
    when(mapper.getShardIdColumnName(anyString(), eq("Users"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions allTablesOptions = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    allTablesOptions.setShardIds("s1,s2");
    TableConfiguration allTables = TableConfiguration.parseFromOptions(allTablesOptions);
    GCSSpannerDVOptions usersOnlyOptions = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    usersOnlyOptions.setTables("Users");
    usersOnlyOptions.setShardIds("s1,s2");
    TableConfiguration usersOnly = TableConfiguration.parseFromOptions(usersOnlyOptions);

    List<ReadOperation> allTablesResult =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, allTables)
            .buildReadOperations(ddl, mapper);
    List<ReadOperation> usersOnlyResult =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, usersOnly)
            .buildReadOperations(ddl, mapper);

    List<ReadOperation> expected =
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    Statement.newBuilder(
                            "SELECT *, 'Users' as __tableName__ FROM `Users`"
                                + " WHERE `migration_shard_id` IN UNNEST(@p1)")
                        .bind("p1")
                        .toStringArray(Arrays.asList("s1", "s2"))
                        .build()));
    assertEquals("no --tables", expected, allTablesResult);
    assertEquals("--tables=Users", expected, usersOnlyResult);
  }

  @Test
  public void testShardIdsWithRenamedSourceTableFiltersBySourceNameAndQueriesSpannerName() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "Users", "AccountRoles");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), eq("Users"))).thenReturn("users_src");
    when(mapper.getSourceTableName(anyString(), eq("AccountRoles")))
        .thenReturn("account_roles_src");
    when(mapper.getShardIdColumnName(anyString(), eq("Users"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTables("users_src");
    options.setShardIds("s1,s2");
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
            .buildReadOperations(ddl, mapper);

    // Users is kept via its source name "users_src" and queried as `Users`; AccountRoles is out
    // of scope, so it is skipped before the shard ID column check despite having no column.
    assertEquals(
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    Statement.newBuilder(
                            "SELECT *, 'Users' as __tableName__ FROM `Users`"
                                + " WHERE `migration_shard_id` IN UNNEST(@p1)")
                        .bind("p1")
                        .toStringArray(Arrays.asList("s1", "s2"))
                        .build())),
        result);
  }

  @Test
  public void testSpannerQueryWithoutShardIdsIsUsed() throws IOException {
    // Without --shardIds the query is still used; other tables keep the baseline query.
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1", "table2");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":"
                + "{\"table1\":{\"spannerQuery\":\"SELECT * FROM table1 WHERE id < 10\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), IdentityMapper::new, config)
            .buildReadOperations(ddl, new IdentityMapper(ddl));

    assertEquals(
        Arrays.asList(
            ReadOperation.create()
                .withQuery(
                    "SELECT *, 'table1' AS __tableName__ FROM (\n"
                        + "SELECT * FROM table1 WHERE id < 10\n"
                        + ") AS __dv_src__"),
            ReadOperation.create().withQuery("SELECT *, 'table2' as __tableName__ FROM `table2`")),
        result);
  }

  @Test
  public void testSpannerQueryOverridesShardIdColumn() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSpannerTableName(anyString(), eq("table1"))).thenReturn("table1");
    when(mapper.getSourceTableName(anyString(), eq("table1"))).thenReturn("table1");
    when(mapper.getShardIdColumnName(anyString(), eq("table1"))).thenReturn("migration_shard_id");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":{\"table1\":"
                + "{\"spannerQuery\":\"SELECT * FROM table1 WHERE migration_shard_id = 's1'\"}}}"));
    options.setShardIds("s1,s2");
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
            .buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    "SELECT *, 'table1' AS __tableName__ FROM (\n"
                        + "SELECT * FROM table1 WHERE migration_shard_id = 's1'\n"
                        + ") AS __dv_src__")),
        result);
  }

  @Test
  public void testSpannerQueryTagIsSpannerTableNameNotConfigKey() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "Orders");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSpannerTableName(anyString(), eq("orders_src"))).thenReturn("Orders");
    when(mapper.getSourceTableName(anyString(), eq("Orders"))).thenReturn("orders_src");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":"
                + "{\"orders_src\":{\"spannerQuery\":\"SELECT * FROM Orders\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
            .buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    "SELECT *, 'Orders' AS __tableName__ FROM (\n"
                        + "SELECT * FROM Orders\n"
                        + ") AS __dv_src__")),
        result);
  }

  @Test
  public void testSpannerQueryKeyNotInTableNamesFails() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1", "table2");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"tableNames\":[\"table1\"],\"optionalConfigurations\":"
                + "{\"table2\":{\"spannerQuery\":\"SELECT * FROM table2\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new CreateSpannerReadOpsFn(mock(PCollectionView.class), IdentityMapper::new, config)
                    .buildReadOperations(ddl, new IdentityMapper(ddl)));
    assertTrue(e.getMessage(), e.getMessage().contains("table2"));
  }

  @Test
  public void testSpannerQueryKeyWithoutSpannerMappingFails() throws IOException {
    // IdentityMapper.getSpannerTableName throws for "ghost" because the DDL has no such table.
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":{\"ghost\":{\"spannerQuery\":\"SELECT * FROM ghost\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new CreateSpannerReadOpsFn(mock(PCollectionView.class), IdentityMapper::new, config)
                    .buildReadOperations(ddl, new IdentityMapper(ddl)));
    assertTrue(e.getMessage(), e.getMessage().contains("ghost"));
  }

  @Test
  public void testTwoSpannerQueryKeysMappedToSameTableFailNamingBoth() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1", "table2");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), anyString())).thenAnswer(inv -> inv.getArgument(1));
    when(mapper.getSpannerTableName(anyString(), eq("table1"))).thenReturn("table1");
    when(mapper.getSpannerTableName(anyString(), eq("t1_copy"))).thenReturn("table1");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":{"
                + "\"table1\":{\"spannerQuery\":\"SELECT * FROM table1\"},"
                + "\"t1_copy\":{\"spannerQuery\":\"SELECT * FROM table1 WHERE id < 5\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config)
                    .buildReadOperations(ddl, mapper));
    assertTrue(e.getMessage(), e.getMessage().contains("table1"));
    assertTrue(e.getMessage(), e.getMessage().contains("t1_copy"));
  }

  @Test
  public void testSpannerQueryTrailingSemicolonAndWhitespaceStripped() throws IOException {
    // The maximal trailing run of ';' and whitespace is stripped.
    assertEquals(
        "SELECT * FROM table1",
        CreateSpannerReadOpsFn.normalizeSpannerQuery("SELECT * FROM table1 ;; \n "));
    assertEquals(
        "SELECT * FROM table1",
        CreateSpannerReadOpsFn.normalizeSpannerQuery("  SELECT * FROM table1;"));

    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    // "\\n" is a JSON-escaped newline, so the query is "SELECT * FROM table1 ;; \n ".
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":"
                + "{\"table1\":{\"spannerQuery\":\"SELECT * FROM table1 ;; \\n \"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), IdentityMapper::new, config)
            .buildReadOperations(ddl, new IdentityMapper(ddl));

    assertEquals(
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    "SELECT *, 'table1' AS __tableName__ FROM (\n"
                        + "SELECT * FROM table1\n"
                        + ") AS __dv_src__")),
        result);
  }

  @Test
  public void testSpannerQueryTrailingLineCommentKeptAndParenOnNextLine() throws IOException {
    // A trailing line comment can't swallow the closing parenthesis.
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, "table1");
    GCSSpannerDVOptions options = PipelineOptionsFactory.as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"optionalConfigurations\":"
                + "{\"table1\":{\"spannerQuery\":\"SELECT * FROM table1 WHERE id < 5 -- note\"}}}"));
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    List<ReadOperation> result =
        new CreateSpannerReadOpsFn(mock(PCollectionView.class), IdentityMapper::new, config)
            .buildReadOperations(ddl, new IdentityMapper(ddl));

    assertEquals(
        Collections.singletonList(
            ReadOperation.create()
                .withQuery(
                    "SELECT *, 'table1' AS __tableName__ FROM (\n"
                        + "SELECT * FROM table1 WHERE id < 5 -- note\n"
                        + ") AS __dv_src__")),
        result);
  }

  /** Builds a DDL with empty tables; CreateSpannerReadOpsFn reads only table names and dialect. */
  private static Ddl ddl(Dialect dialect, String... tableNames) {
    Ddl.Builder builder = Ddl.builder(dialect);
    for (String tableName : tableNames) {
      builder = builder.createTable(tableName).endTable();
    }
    return builder.build();
  }

  /** Writes {@code json} to a new table configuration file and returns its path. */
  private String writeTableConfigFile(String json) throws IOException {
    File file = tempFolder.newFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(json);
    }
    return file.getAbsolutePath();
  }
}
