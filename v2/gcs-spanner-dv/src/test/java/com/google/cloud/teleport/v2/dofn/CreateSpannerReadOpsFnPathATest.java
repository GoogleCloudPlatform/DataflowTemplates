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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for read-op construction in {@link CreateSpannerReadOpsFn} when {@code --shardIds} is set:
 * scope rules, Path A (shard ID column), check 1 and the aggregated error. Also guards that runs
 * without the new configuration emit exactly today's queries.
 */
@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnPathATest {

  private static final String SHARD_COL = "migration_shard_id";

  private static final String USERS_GSQL_PATH_A =
      "SELECT *, 'Users' as __tableName__ FROM `Users` WHERE `migration_shard_id` IN UNNEST(@p1)";

  private static final String USERS_PG_PATH_A =
      "SELECT *, 'Users' as __tableName__ FROM \"Users\" WHERE \"migration_shard_id\" = ANY($1)";

  private static TableConfiguration config(String tables, String shardIds) {
    GCSSpannerDVOptions options = PipelineOptionsFactory.create().as(GCSSpannerDVOptions.class);
    if (tables != null) {
      options.setTables(tables);
    }
    if (shardIds != null) {
      options.setShardIds(shardIds);
    }
    return TableConfiguration.parseFromOptions(options);
  }

  /**
   * Builds a real DDL. Tables named in {@code withShardColumn} get a leading {@code
   * migration_shard_id} key column; every table has an {@code id} key column.
   */
  private static Ddl ddl(Dialect dialect, Set<String> withShardColumn, String... tables) {
    Ddl.Builder builder = Ddl.builder(dialect);
    boolean pg = dialect == Dialect.POSTGRESQL;
    for (String name : tables) {
      Table.Builder table = builder.createTable(name);
      boolean hasShardCol = withShardColumn.contains(name);
      if (hasShardCol) {
        table =
            pg
                ? table.column(SHARD_COL).pgVarchar().notNull().endColumn()
                : table.column(SHARD_COL).string().max().notNull().endColumn();
      }
      table =
          pg
              ? table.column("id").pgInt8().notNull().endColumn()
              : table.column("id").int64().notNull().endColumn();
      table =
          hasShardCol
              ? table.primaryKey().asc(SHARD_COL).asc("id").end()
              : table.primaryKey().asc("id").end();
      builder = table.endTable();
    }
    return builder.build();
  }

  /**
   * A mapper that maps every Spanner table to the same source name, except the {@code unmappable}
   * ones (which throw {@link NoSuchElementException}), and reports {@code migration_shard_id} as
   * the shard ID column only for {@code withShardColumn}.
   */
  private static ISchemaMapper mapper(Set<String> withShardColumn, Set<String> unmappable) {
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              String table = inv.getArgument(1);
              if (unmappable.contains(table)) {
                throw new NoSuchElementException("no source table for " + table);
              }
              return table;
            });
    when(mapper.getShardIdColumnName(anyString(), anyString()))
        .thenAnswer(inv -> withShardColumn.contains(inv.getArgument(1)) ? SHARD_COL : null);
    return mapper;
  }

  @SuppressWarnings("unchecked")
  private static CreateSpannerReadOpsFn fn(ISchemaMapper mapper, TableConfiguration config) {
    return new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config);
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  private static ReadOperation pathAOp(String sql, List<String> shardIds) {
    return ReadOperation.create()
        .withQuery(Statement.newBuilder(sql).bind("p1").toStringArray(shardIds).build());
  }

  // R1 (H7 guard): no --shardIds → exactly today's query strings, in DDL order.
  @Test
  public void testNoNewConfigurationEmitsBaselineQueries() {
    assertEquals(
        "SELECT *, 'Users' as __tableName__ FROM `Users`",
        CreateSpannerReadOpsFn.baselineQuery(Dialect.GOOGLE_STANDARD_SQL, "Users"));
    assertEquals(
        "SELECT *, 'Users' as __tableName__ FROM \"Users\"",
        CreateSpannerReadOpsFn.baselineQuery(Dialect.POSTGRESQL, "Users"));

    for (Dialect dialect : Arrays.asList(Dialect.GOOGLE_STANDARD_SQL, Dialect.POSTGRESQL)) {
      String q = dialect == Dialect.POSTGRESQL ? "\"" : "`";
      Ddl ddl = ddl(dialect, set(), "Users", "AccountRoles", "Extra");

      // --tables filter, no shards.
      TableConfiguration filtered = config("Users,AccountRoles", null);
      List<ReadOperation> result =
          fn(new IdentityMapper(ddl), filtered).buildReadOperations(ddl, new IdentityMapper(ddl));
      List<ReadOperation> expected = new ArrayList<>();
      for (String t : ddl.getTablesOrderedByReference()) {
        if (!t.equals("Extra")) {
          expected.add(
              ReadOperation.create()
                  .withQuery(
                      String.format("SELECT *, '%s' as __tableName__ FROM %s%s%s", t, q, t, q)));
        }
      }
      assertEquals(dialect + " filtered", expected, result);

      // No configuration at all.
      List<ReadOperation> all =
          fn(new IdentityMapper(ddl), TableConfiguration.empty())
              .buildReadOperations(ddl, new IdentityMapper(ddl));
      List<ReadOperation> expectedAll = new ArrayList<>();
      for (String t : ddl.getTablesOrderedByReference()) {
        expectedAll.add(
            ReadOperation.create()
                .withQuery(
                    String.format("SELECT *, '%s' as __tableName__ FROM %s%s%s", t, q, t, q)));
      }
      assertEquals(dialect + " unfiltered", expectedAll, all);
    }
  }

  // R2
  @Test
  public void testPathAGoogleSqlBindsShardIdsAsOneArrayParameter() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users"), "Users");
    ISchemaMapper mapper = mapper(set("Users"), set());
    TableConfiguration config = config(null, "s1,s2");

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(pathAOp(USERS_GSQL_PATH_A, Arrays.asList("s1", "s2"))), result);
    assertEquals(
        Statement.newBuilder(USERS_GSQL_PATH_A)
            .bind("p1")
            .toStringArray(Arrays.asList("s1", "s2"))
            .build(),
        CreateSpannerReadOpsFn.pathAStatement(
            Dialect.GOOGLE_STANDARD_SQL, "Users", SHARD_COL, config.getShardIds()));
  }

  // R2
  @Test
  public void testPathAPostgresBindsShardIdsAsOneArrayParameter() {
    Ddl ddl = ddl(Dialect.POSTGRESQL, set("Users"), "Users");
    ISchemaMapper mapper = mapper(set("Users"), set());
    TableConfiguration config = config(null, "s1,s2");

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(pathAOp(USERS_PG_PATH_A, Arrays.asList("s1", "s2"))), result);
    assertEquals(
        Statement.newBuilder(USERS_PG_PATH_A)
            .bind("p1")
            .toStringArray(Arrays.asList("s1", "s2"))
            .build(),
        CreateSpannerReadOpsFn.pathAStatement(
            Dialect.POSTGRESQL, "Users", SHARD_COL, config.getShardIds()));
  }

  // R2 (§4.1: one bound array parameter, whatever the shard count)
  @Test
  public void testPathAQuerySizeIndependentOfShardCount() {
    List<String> many = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      many.add("shard_" + i);
    }
    for (Dialect dialect : Arrays.asList(Dialect.GOOGLE_STANDARD_SQL, Dialect.POSTGRESQL)) {
      Statement one =
          CreateSpannerReadOpsFn.pathAStatement(
              dialect, "Users", SHARD_COL, Collections.singletonList("shard_0"));
      Statement fifty = CreateSpannerReadOpsFn.pathAStatement(dialect, "Users", SHARD_COL, many);

      assertEquals(dialect.toString(), one.getSql(), fifty.getSql());
      assertEquals(1, one.getParameters().size());
      assertEquals(1, fifty.getParameters().size());
      assertEquals(Value.stringArray(many), fifty.getParameters().get("p1"));
    }
  }

  // R6
  @Test
  public void testShardIdsWithoutShardIdColumnFailsNamingTable() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users"), "Users", "Orders");
    ISchemaMapper mapper = mapper(set("Users"), set());
    TableConfiguration config = config(null, "s1");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> fn(mapper, config).buildReadOperations(ddl, mapper));
    assertTrue(e.getMessage(), e.getMessage().contains("Orders"));
    assertFalse(e.getMessage(), e.getMessage().contains("Users"));

    // processElement: fails and emits nothing (not even the valid Users op).
    @SuppressWarnings("unchecked")
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    @SuppressWarnings("unchecked")
    DoFn<Void, ReadOperation>.ProcessContext ctx = mock(DoFn.ProcessContext.class);
    when(ctx.sideInput(ddlView)).thenReturn(ddl);
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, d -> mapper, config);

    IllegalArgumentException pe =
        assertThrows(IllegalArgumentException.class, () -> doFn.processElement(ctx));
    assertTrue(pe.getMessage(), pe.getMessage().contains("Orders"));
    verify(ctx, never()).output(any());
  }

  // R8: an unmappable Spanner table is skipped silently when --shardIds is set.
  @Test
  public void testShardIdsSkipsUnmappableSpannerTable() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users"), "Users", "SpannerOnly");
    ISchemaMapper mapper = mapper(set("Users"), set("SpannerOnly"));

    for (String tables : Arrays.asList(null, "Users")) {
      TableConfiguration config = config(tables, "s1,s2");

      List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

      assertEquals(
          "tables=" + tables,
          Collections.singletonList(pathAOp(USERS_GSQL_PATH_A, Arrays.asList("s1", "s2"))),
          result);
    }
  }

  // R7 (check 1 only): every offender is reported in one error, one sorted line each.
  @Test
  public void testSeveralTablesFailingCheckOneReportedInOneError() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users"), "BetaTable", "Users", "AlphaTable");
    ISchemaMapper mapper = mapper(set("Users"), set());
    TableConfiguration config = config(null, "s1");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> fn(mapper, config).buildReadOperations(ddl, mapper));
    String message = e.getMessage();
    assertTrue(message, message.contains("AlphaTable"));
    assertTrue(message, message.contains("BetaTable"));
    assertTrue(message, message.indexOf("AlphaTable") < message.indexOf("BetaTable"));
    assertFalse(message, message.contains("Users"));
  }

  // H6: --tables and --shardIds intersect.
  @Test
  public void testShardIdsIntersectWithTablesFilter() {
    Ddl ddl =
        ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users", "AccountRoles"), "Users", "AccountRoles");
    ISchemaMapper mapper = mapper(set("Users", "AccountRoles"), set());
    TableConfiguration config = config("Users", "s1,s2");

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(pathAOp(USERS_GSQL_PATH_A, Arrays.asList("s1", "s2"))), result);
  }

  // D-054: --tables filters by the source name; the query reads the Spanner table name.
  @Test
  public void testShardIdsWithRenamedSourceTableFiltersBySourceNameAndQueriesSpannerName() {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("Users"), "Users", "AccountRoles");
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              String table = inv.getArgument(1);
              if (table.equals("Users")) {
                return "users_src";
              }
              if (table.equals("AccountRoles")) {
                return "account_roles_src";
              }
              throw new NoSuchElementException("no source table for " + table);
            });
    when(mapper.getShardIdColumnName(anyString(), anyString()))
        .thenAnswer(inv -> "Users".equals(inv.getArgument(1)) ? SHARD_COL : null);
    TableConfiguration config = config("users_src", "s1,s2");

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    // Users is kept via its source name "users_src" and queried as `Users`; AccountRoles is out
    // of scope, so it is skipped before check 1 despite having no shard ID column.
    assertEquals(
        Collections.singletonList(pathAOp(USERS_GSQL_PATH_A, Arrays.asList("s1", "s2"))), result);
  }
}
