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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for read-op construction in {@link CreateSpannerReadOpsFn} when a {@code spannerQuery} is
 * configured (Path B): the wrapper text, normalisation, precedence over the shard ID column, the
 * bad-key check and the aggregated error.
 */
@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnPathBTest {

  private static final String SHARD_COL = "migration_shard_id";

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Writes a table configuration file with {@code tableNames} (null = no table filter) and one
   * {@code spannerQuery} per entry of {@code spannerQueries}, then parses it with {@code shardIds}
   * (null = unset).
   */
  private TableConfiguration config(
      List<String> tableNames, Map<String, String> spannerQueries, String shardIds)
      throws IOException {
    Map<String, Object> file = new LinkedHashMap<>();
    if (tableNames != null) {
      file.put("tableNames", tableNames);
    }
    Map<String, Object> optional = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : spannerQueries.entrySet()) {
      optional.put(entry.getKey(), Collections.singletonMap("spannerQuery", entry.getValue()));
    }
    file.put("optionalConfigurations", optional);
    File json = tempFolder.newFile();
    Files.write(json.toPath(), new Gson().toJson(file).getBytes(StandardCharsets.UTF_8));

    GCSSpannerDVOptions options = PipelineOptionsFactory.create().as(GCSSpannerDVOptions.class);
    options.setTableConfigurationFilePath(json.getAbsolutePath());
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
   * A mapper backed by {@code sourceToSpanner}: {@code getSpannerTableName} looks the source name
   * up, {@code getSourceTableName} returns the first source name mapped to the Spanner table. Both
   * throw {@link NoSuchElementException} when there's no entry. {@code migration_shard_id} is the
   * shard ID column only for {@code withShardColumn}.
   */
  private static ISchemaMapper mapper(
      Map<String, String> sourceToSpanner, Set<String> withShardColumn) {
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSpannerTableName(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              String source = inv.getArgument(1);
              if (!sourceToSpanner.containsKey(source)) {
                throw new NoSuchElementException("no Spanner table for " + source);
              }
              return sourceToSpanner.get(source);
            });
    when(mapper.getSourceTableName(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              String spanner = inv.getArgument(1);
              for (Map.Entry<String, String> entry : sourceToSpanner.entrySet()) {
                if (entry.getValue().equals(spanner)) {
                  return entry.getKey();
                }
              }
              throw new NoSuchElementException("no source table for " + spanner);
            });
    when(mapper.getShardIdColumnName(anyString(), anyString()))
        .thenAnswer(inv -> withShardColumn.contains(inv.getArgument(1)) ? SHARD_COL : null);
    return mapper;
  }

  /** Builds a map from alternating key/value arguments, in order. */
  private static Map<String, String> map(String... keysAndValues) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private static CreateSpannerReadOpsFn fn(ISchemaMapper mapper, TableConfiguration config) {
    return new CreateSpannerReadOpsFn(mock(PCollectionView.class), d -> mapper, config);
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  private static ReadOperation op(String sql) {
    return ReadOperation.create().withQuery(sql);
  }

  private static String wrapped(String table, String query) {
    return "SELECT *, '" + table + "' AS __tableName__ FROM (\n" + query + "\n) AS __dv_src__";
  }

  private static IllegalArgumentException assertFails(
      ISchemaMapper mapper, TableConfiguration config, Ddl ddl) {
    return assertThrows(
        IllegalArgumentException.class, () -> fn(mapper, config).buildReadOperations(ddl, mapper));
  }

  private static void assertContains(String message, String... names) {
    for (String name : names) {
      assertTrue(message, message.contains(name));
    }
  }

  // R3
  @Test
  public void testPathBWrapsUserQueryWithDdlTagOnOwnLinesGoogleSql() throws IOException {
    String expected =
        "SELECT *, 'table1' AS __tableName__ FROM (\n"
            + "SELECT * FROM table1 WHERE id < 10\n"
            + ") AS __dv_src__";
    assertEquals(
        expected,
        CreateSpannerReadOpsFn.wrapSpannerQuery("table1", "SELECT * FROM table1 WHERE id < 10"));

    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set());
    TableConfiguration config =
        config(null, map("table1", "SELECT * FROM table1 WHERE id < 10"), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(Collections.singletonList(op(expected)), result);
  }

  // R3: the wrapper is dialect-independent.
  @Test
  public void testPathBWrapsUserQueryPostgres() throws IOException {
    Ddl ddl = ddl(Dialect.POSTGRESQL, set(), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set());
    TableConfiguration config =
        config(null, map("table1", "SELECT * FROM table1 WHERE id < 10"), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(
        Collections.singletonList(op(wrapped("table1", "SELECT * FROM table1 WHERE id < 10"))),
        result);
  }

  // R3 (§4.2.1): the tag is the DDL table name, not the config key.
  @Test
  public void testPathBTagComesFromDdlNameNotConfigKey() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "Orders");
    ISchemaMapper mapper = mapper(map("orders_src", "Orders"), set());
    TableConfiguration config = config(null, map("orders_src", "SELECT * FROM Orders"), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(Collections.singletonList(op(wrapped("Orders", "SELECT * FROM Orders"))), result);
  }

  // R4: spannerQuery wins over the session ShardIdColumn.
  @Test
  public void testSpannerQueryOverridesShardIdColumn() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("table1"), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set("table1"));
    String query = "SELECT * FROM table1 WHERE migration_shard_id = 's1'";
    TableConfiguration config = config(null, map("table1", query), "s1,s2");

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(Collections.singletonList(op(wrapped("table1", query))), result);
  }

  // R5: without --shardIds the query is still used; other tables keep the baseline query.
  @Test
  public void testSpannerQueryWithoutShardIdsIsUsed() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1", "table2");
    ISchemaMapper mapper = mapper(map("table1", "table1", "table2", "table2"), set());
    String query = "SELECT * FROM table1 WHERE id < 10";
    TableConfiguration config = config(null, map("table1", query), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    List<ReadOperation> expected = new ArrayList<>();
    for (String t : ddl.getTablesOrderedByReference()) {
      expected.add(
          t.equals("table1")
              ? op(wrapped("table1", query))
              : op("SELECT *, 'table2' as __tableName__ FROM `table2`"));
    }
    assertEquals(expected, result);
  }

  // R7: check 1 and a bad key are reported together; nothing is emitted.
  @Test
  public void testSeveralFailuresReportedInOneError() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "NoShardCol", "table1");
    ISchemaMapper mapper = mapper(map("NoShardCol", "NoShardCol", "table1", "table1"), set());
    TableConfiguration config =
        config(
            null,
            map("Typo", "SELECT * FROM Typo", "table1", "SELECT * FROM table1 WHERE id < 10"),
            "s1");

    IllegalArgumentException e = assertFails(mapper, config, ddl);
    assertContains(e.getMessage(), "NoShardCol", "Typo");

    // processElement: fails and emits nothing (not even the valid table1 op).
    @SuppressWarnings("unchecked")
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    @SuppressWarnings("unchecked")
    DoFn<Void, ReadOperation>.ProcessContext ctx = mock(DoFn.ProcessContext.class);
    when(ctx.sideInput(ddlView)).thenReturn(ddl);
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView, d -> mapper, config);

    IllegalArgumentException pe =
        assertThrows(IllegalArgumentException.class, () -> doFn.processElement(ctx));
    assertContains(pe.getMessage(), "NoShardCol", "Typo");
    verify(ctx, never()).output(any());
  }

  // R9: a key excluded by tableNames is a bad key, with or without --shardIds.
  @Test
  public void testSpannerQueryKeyNotInTableNamesFails() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("table1"), "table1", "table2");
    ISchemaMapper mapper = mapper(map("table1", "table1", "table2", "table2"), set("table1"));
    for (String shardIds : Arrays.asList(null, "s1")) {
      TableConfiguration config =
          config(
              Collections.singletonList("table1"), map("table2", "SELECT * FROM table2"), shardIds);

      IllegalArgumentException e = assertFails(mapper, config, ddl);
      assertTrue("shardIds=" + shardIds + ": " + e.getMessage(), e.getMessage().contains("table2"));
    }
  }

  // R9: a key with no Spanner mapping is a bad key.
  @Test
  public void testSpannerQueryKeyWithoutSpannerMappingFails() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("table1"), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set("table1"));
    for (String shardIds : Arrays.asList(null, "s1")) {
      TableConfiguration config = config(null, map("ghost", "SELECT * FROM ghost"), shardIds);

      IllegalArgumentException e = assertFails(mapper, config, ddl);
      assertTrue("shardIds=" + shardIds + ": " + e.getMessage(), e.getMessage().contains("ghost"));
    }
  }

  // R9: a key mapped to a Spanner table that isn't in the DDL is a bad key (the session mapper
  // doesn't check the DDL).
  @Test
  public void testSpannerQueryKeyMappedToTableMissingFromDdlFails() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("table1"), "table1");
    ISchemaMapper mapper =
        mapper(map("table1", "table1", "legacy_src", "LegacyTable"), set("table1"));
    for (String shardIds : Arrays.asList(null, "s1")) {
      TableConfiguration config =
          config(null, map("legacy_src", "SELECT * FROM LegacyTable"), shardIds);

      IllegalArgumentException e = assertFails(mapper, config, ddl);
      assertTrue(
          "shardIds=" + shardIds + ": " + e.getMessage(), e.getMessage().contains("legacy_src"));
    }
  }

  // R9: with --shardIds, a key whose table was skipped as unmappable is a bad key.
  @Test
  public void testSpannerQueryKeyForSkippedUnmappableTableFails() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set("table1"), "table1", "SpannerOnly");
    ISchemaMapper mapper =
        mapper(map("table1", "table1", "spanner_only_src", "SpannerOnly"), set("table1"));
    // The key resolves forwards to SpannerOnly, but SpannerOnly has no source table.
    doThrow(new NoSuchElementException("no source table for SpannerOnly"))
        .when(mapper)
        .getSourceTableName(anyString(), eq("SpannerOnly"));
    TableConfiguration config =
        config(null, map("spanner_only_src", "SELECT * FROM SpannerOnly"), "s1");

    IllegalArgumentException e = assertFails(mapper, config, ddl);
    assertContains(e.getMessage(), "spanner_only_src");
  }

  // Q-7 / D-009: two keys resolving to the same Spanner table fail, naming both keys.
  @Test
  public void testTwoKeysMappedToSameTableFailNamingBoth() throws IOException {
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1", "table2");
    ISchemaMapper mapper =
        mapper(map("table1", "table1", "t1_copy", "table1", "table2", "table2"), set());
    TableConfiguration config =
        config(
            null,
            map("table1", "SELECT * FROM table1", "t1_copy", "SELECT * FROM table1 WHERE id < 5"),
            null);

    IllegalArgumentException e = assertFails(mapper, config, ddl);
    assertContains(e.getMessage(), "table1", "t1_copy");
  }

  // R10: the maximal trailing run of ';' and whitespace is stripped.
  @Test
  public void testTrailingSemicolonAndWhitespaceStripped() throws IOException {
    String raw = "SELECT * FROM table1 ;; \n ";
    assertEquals("SELECT * FROM table1", CreateSpannerReadOpsFn.normalizeSpannerQuery(raw));
    assertEquals(
        "SELECT * FROM table1",
        CreateSpannerReadOpsFn.normalizeSpannerQuery("  SELECT * FROM table1;"));

    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set());
    TableConfiguration config = config(null, map("table1", raw), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(1, result.size());
    String sql = result.get(0).getQuery().getSql();
    assertTrue(sql, sql.contains("SELECT * FROM table1\n) AS"));
    assertEquals(wrapped("table1", "SELECT * FROM table1"), sql);
  }

  // R10: a trailing line comment can't swallow the closing parenthesis.
  @Test
  public void testTrailingLineCommentKeptAndParenOnNextLine() throws IOException {
    String raw = "SELECT * FROM table1 WHERE id < 5 -- note";
    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set());
    TableConfiguration config = config(null, map("table1", raw), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(1, result.size());
    String sql = result.get(0).getQuery().getSql();
    assertTrue(sql, sql.endsWith("-- note\n) AS __dv_src__"));
    assertEquals(wrapped("table1", raw), sql);
  }

  // R10 (§4.2.1): a terminator followed by a comment isn't at the end, so nothing is stripped.
  @Test
  public void testTerminatorFollowedByCommentNotStripped() throws IOException {
    String raw = "SELECT * FROM table1; /* c */";
    assertEquals(raw, CreateSpannerReadOpsFn.normalizeSpannerQuery(raw));

    Ddl ddl = ddl(Dialect.GOOGLE_STANDARD_SQL, set(), "table1");
    ISchemaMapper mapper = mapper(map("table1", "table1"), set());
    TableConfiguration config = config(null, map("table1", raw), null);

    List<ReadOperation> result = fn(mapper, config).buildReadOperations(ddl, mapper);

    assertEquals(Collections.singletonList(op(wrapped("table1", raw))), result);
  }
}
