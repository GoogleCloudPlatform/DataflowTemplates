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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSpannerReadOpsFn extends DoFn<Void, ReadOperation> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSpannerReadOpsFn.class);

  private final PCollectionView<Ddl> ddlView;
  private final SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider;
  private final TableConfiguration tableConfig;

  public CreateSpannerReadOpsFn(
      PCollectionView<Ddl> ddlView,
      SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider,
      TableConfiguration tableConfig) {
    this.ddlView = ddlView;
    this.schemaMapperProvider = schemaMapperProvider;
    this.tableConfig = tableConfig;
  }

  // TODO: @aasthabharill to check if there's a better way to generalize dialect specific changes
  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    ISchemaMapper schemaMapper = schemaMapperProvider.apply(ddl);
    for (ReadOperation readOperation : buildReadOperations(ddl, schemaMapper)) {
      c.output(readOperation);
    }
  }

  @VisibleForTesting
  List<ReadOperation> buildReadOperations(Ddl ddl, ISchemaMapper schemaMapper) {
    if (tableConfig == null
        || (!tableConfig.hasShardFilter() && !tableConfig.hasSpannerQueries())) {
      return readAllRows(ddl, schemaMapper);
    }
    return readSelectedRows(ddl, schemaMapper);
  }

  private List<ReadOperation> readAllRows(Ddl ddl, ISchemaMapper schemaMapper) {
    List<ReadOperation> readOperations = new ArrayList<>();
    for (String tableName : ddl.getTablesOrderedByReference()) {
      if (tableConfig != null && !tableConfig.isSpannerTableAllowed(tableName, schemaMapper)) {
        continue;
      }
      // We encode the tableName in the query itself to push table information dynamically
      // and avoid table level stages.
      readOperations.add(ReadOperation.create().withQuery(baselineQuery(ddl.dialect(), tableName)));
    }
    return readOperations;
  }

  /**
   * Reads each table with its {@code spannerQuery} if it has one, else only the rows of the
   * selected shards (or the whole table if {@code --shardIds} is unset). Fails with one error
   * listing every in-scope table without a shard ID column and every bad {@code spannerQuery} key.
   */
  private List<ReadOperation> readSelectedRows(Ddl ddl, ISchemaMapper schemaMapper) {
    boolean hasShardFilter = tableConfig.hasShardFilter();
    Map<String, String> spannerQueries = tableConfig.getSpannerQueries();
    List<ReadOperation> readOperations = new ArrayList<>();
    List<String> errors = new ArrayList<>();

    Map<String, String> keyBySpannerTable =
        resolveSpannerQueryKeys(ddl, schemaMapper, spannerQueries, errors);

    for (String tableName : ddl.getTablesOrderedByReference()) {
      // With --shardIds, skip a Spanner-only table silently: it has no source rows for any shard.
      if (hasShardFilter && !hasSourceTable(schemaMapper, tableName)) {
        continue;
      }
      // Skip a table excluded by tableNames.
      if (!tableConfig.isSpannerTableAllowed(tableName, schemaMapper)) {
        continue;
      }
      // remove() also marks the query as used; queries left over are reported after the loop.
      String key = keyBySpannerTable.remove(tableName);
      String spannerQuery = key == null ? null : spannerQueries.get(key);
      Statement statement;
      if (spannerQuery != null && !hasShardFilter) {
        // spannerQuery without --shardIds: use the query, but the GCS side reads every shard.
        LOG.warn(
            "Table '{}' is read from Spanner with its spannerQuery, but --shardIds isn't set, so"
                + " the GCS side reads every shard",
            tableName);
        statement = Statement.of(wrapSpannerQuery(tableName, normalizeSpannerQuery(spannerQuery)));
      } else if (spannerQuery != null) {
        // spannerQuery with --shardIds: the query wins over any ShardIdColumn in the session file.
        if (schemaMapper.getShardIdColumnName("", tableName) != null) {
          LOG.warn(
              "Table '{}': the spannerQuery overrides the ShardIdColumn from the session file",
              tableName);
        }
        statement = Statement.of(wrapSpannerQuery(tableName, normalizeSpannerQuery(spannerQuery)));
      } else if (!hasShardFilter) {
        // No spannerQuery and no --shardIds: read the whole table.
        statement = Statement.of(baselineQuery(ddl.dialect(), tableName));
      } else {
        // --shardIds without a spannerQuery: filter on the ShardIdColumn, or report the table.
        String shardIdColumn = schemaMapper.getShardIdColumnName("", tableName);
        if (shardIdColumn == null) {
          errors.add(
              String.format(
                  "table '%s': --shardIds is set, but the table has no ShardIdColumn in the session"
                      + " file and no spannerQuery",
                  tableName));
          continue;
        }
        statement =
            pathAStatement(ddl.dialect(), tableName, shardIdColumn, tableConfig.getShardIds());
      }
      LOG.info("Spanner query for table '{}': {}", tableName, statement.getSql());
      readOperations.add(ReadOperation.create().withQuery(statement));
    }

    // A key whose table wasn't read (out of scope) would silently validate nothing.
    for (Map.Entry<String, String> entry : keyBySpannerTable.entrySet()) {
      errors.add(
          String.format(
              "spannerQuery for '%s': maps to Spanner table '%s', which isn't validated",
              entry.getValue(), entry.getKey()));
    }
    if (!errors.isEmpty()) {
      Collections.sort(errors);
      throw new IllegalArgumentException(
          "Cannot build the Spanner reads:\n" + String.join("\n", errors));
    }
    return readOperations;
  }

  /**
   * Resolves each {@code spannerQuery} key (a source table name) to its Spanner table. Returns the
   * key for each Spanner table, and adds an error for every bad key.
   */
  private Map<String, String> resolveSpannerQueryKeys(
      Ddl ddl,
      ISchemaMapper schemaMapper,
      Map<String, String> spannerQueries,
      List<String> errors) {
    Map<String, String> keyBySpannerTable = new HashMap<>();
    for (String key : spannerQueries.keySet()) {
      if (!tableConfig.isSourceTableAllowed(key)) {
        errors.add(String.format("spannerQuery for '%s': the table isn't in tableNames", key));
        continue;
      }
      String spannerTableName;
      try {
        spannerTableName = schemaMapper.getSpannerTableName("", key);
      } catch (NoSuchElementException e) {
        errors.add(String.format("spannerQuery for '%s': no Spanner table is mapped to it", key));
        continue;
      }
      Table table = ddl.table(spannerTableName);
      if (table == null) {
        errors.add(
            String.format(
                "spannerQuery for '%s': maps to Spanner table '%s', which doesn't exist",
                key, spannerTableName));
        continue;
      }
      // Key by the DDL's own name, so a mapper name differing only in case still matches.
      String otherKey = keyBySpannerTable.putIfAbsent(table.name(), key);
      if (otherKey != null) {
        errors.add(
            String.format(
                "spannerQuery for '%s' and '%s': both map to Spanner table '%s'",
                otherKey, key, table.name()));
      }
    }
    return keyBySpannerTable;
  }

  /** Returns the full-table query for {@code spannerTableName}, tagged with its table name. */
  @VisibleForTesting
  static String baselineQuery(Dialect dialect, String spannerTableName) {
    String quote = quote(dialect);
    return String.format(
        "SELECT *, '%s' as __tableName__ FROM %s%s%s",
        spannerTableName, quote, spannerTableName, quote);
  }

  /**
   * Returns the baseline query filtered to rows whose {@code shardIdColumn} is in {@code shardIds}.
   * The IDs are bound as one array parameter, so the SQL doesn't depend on the number of shards.
   */
  @VisibleForTesting
  static Statement pathAStatement(
      Dialect dialect, String spannerTableName, String shardIdColumn, Collection<String> shardIds) {
    String quote = quote(dialect);
    String filter =
        dialect == Dialect.POSTGRESQL
            ? String.format(" WHERE %s%s%s = ANY($1)", quote, shardIdColumn, quote)
            : String.format(" WHERE %s%s%s IN UNNEST(@p1)", quote, shardIdColumn, quote);
    return Statement.newBuilder(baselineQuery(dialect, spannerTableName) + filter)
        .bind("p1")
        .toStringArray(shardIds)
        .build();
  }

  /** Trims {@code rawQuery} and strips its trailing run of {@code ;} and whitespace. */
  @VisibleForTesting
  static String normalizeSpannerQuery(String rawQuery) {
    return rawQuery.trim().replaceAll("[;\\s]+$", "");
  }

  /**
   * Wraps a user query so its rows are tagged with {@code spannerTableName}. The query goes on its
   * own lines so a trailing {@code --} comment can't swallow the closing parenthesis.
   */
  @VisibleForTesting
  static String wrapSpannerQuery(String spannerTableName, String normalizedQuery) {
    // readAll merges all tables; ComparisonRecordMapper reads this tag to find and hash the table.
    return "SELECT *, '"
        + spannerTableName
        + "' AS __tableName__ FROM (\n"
        + normalizedQuery
        // Spanner's PostgreSQL dialect requires an alias on a subquery in FROM.
        + "\n) AS __dv_src__";
  }

  /** Returns true iff {@code spannerTableName} maps back to a source table. */
  private static boolean hasSourceTable(ISchemaMapper schemaMapper, String spannerTableName) {
    try {
      schemaMapper.getSourceTableName("", spannerTableName);
      return true;
    } catch (NoSuchElementException e) {
      return false;
    }
  }

  private static String quote(Dialect dialect) {
    return dialect == Dialect.POSTGRESQL ? "\"" : "`";
  }
}
