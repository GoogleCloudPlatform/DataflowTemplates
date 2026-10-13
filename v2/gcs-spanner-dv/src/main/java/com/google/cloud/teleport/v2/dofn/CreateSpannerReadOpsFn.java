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

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl ddl = c.sideInput(ddlView);
    ISchemaMapper schemaMapper = schemaMapperProvider.apply(ddl);
    for (ReadOperation readOperation : buildReadOperations(ddl, schemaMapper)) {
      c.output(readOperation);
    }
  }

  List<ReadOperation> buildReadOperations(Ddl ddl, ISchemaMapper schemaMapper) {
    if (tableConfig == null
        || (!tableConfig.hasShardFilter() && !tableConfig.hasSpannerQueries())) {
      return readAllRows(ddl, schemaMapper);
    }
    return readSelectedRows(ddl, schemaMapper);
  }

  private List<ReadOperation> readAllRows(Ddl ddl, ISchemaMapper schemaMapper) {
    List<ReadOperation> readOperations = new ArrayList<>();
    for (String spannerTableName : ddl.getTablesOrderedByReference()) {
      if (tableConfig != null
          && !tableConfig.isSourceTableAllowed(sourceTableName(schemaMapper, spannerTableName))) {
        continue;
      }
      readOperations.add(
          ReadOperation.create().withQuery(baselineQuery(ddl.dialect(), spannerTableName)));
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

    Map<String, String> queryBySpannerTable =
        resolveSpannerQueries(ddl, schemaMapper, spannerQueries, errors);

    for (String spannerTableName : ddl.getTablesOrderedByReference()) {
      String sourceTableName = sourceTableName(schemaMapper, spannerTableName);
      // Skip a table excluded by tableNames.
      if (!tableConfig.isSourceTableAllowed(sourceTableName)) {
        continue;
      }
      String spannerQuery = queryBySpannerTable.get(spannerTableName);
      Statement statement;
      if (spannerQuery != null) {
        if (!hasShardFilter) {
          // spannerQuery without --shardIds: use the query, but the GCS side reads every shard.
          LOG.warn(
              "Table '{}' is read from Spanner with its spannerQuery, but --shardIds isn't set, so"
                  + " the GCS side reads every shard",
              spannerTableName);
        }
        // With --shardIds, the query wins over any ShardIdColumn in the session file.
        statement =
            Statement.of(wrapSpannerQuery(spannerTableName, normalizeSpannerQuery(spannerQuery)));
      } else if (!hasShardFilter) {
        // No spannerQuery and no --shardIds: read the whole table.
        statement = Statement.of(baselineQuery(ddl.dialect(), spannerTableName));
      } else {
        // --shardIds without a spannerQuery: filter on the ShardIdColumn, or report the table.
        String shardIdColumn;
        try {
          shardIdColumn = schemaMapper.getShardIdColumnName("", spannerTableName);
        } catch (NoSuchElementException e) {
          // A Spanner-only table isn't in the session file, so it has no ShardIdColumn.
          shardIdColumn = null;
        }
        if (shardIdColumn == null) {
          errors.add(
              String.format(
                  "table '%s': --shardIds is set, but the table has no ShardIdColumn in the session"
                      + " file and no spannerQuery for '%s'",
                  spannerTableName, sourceTableName));
          continue;
        }
        statement =
            shardFilterStatement(
                ddl.dialect(), spannerTableName, shardIdColumn, tableConfig.getShardIds());
      }
      LOG.info("Spanner query for table '{}': {}", spannerTableName, statement.getSql());
      readOperations.add(ReadOperation.create().withQuery(statement));
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
   * {@code spannerQuery} for each Spanner table, and adds an error for every bad key.
   */
  private Map<String, String> resolveSpannerQueries(
      Ddl ddl,
      ISchemaMapper schemaMapper,
      Map<String, String> spannerQueries,
      List<String> errors) {
    Map<String, String> queryBySpannerTable = new HashMap<>();
    for (String key : spannerQueries.keySet()) {
      // A query for a table left out of tableNames is unused, not wrong.
      if (!tableConfig.isSourceTableAllowed(key)) {
        LOG.warn("Ignoring the spannerQuery for '{}': the table isn't in tableNames", key);
        continue;
      }
      String spannerTableName;
      try {
        spannerTableName = schemaMapper.getSpannerTableName("", key);
      } catch (NoSuchElementException e) {
        // Not a source table: it may be a Spanner-only table, which is keyed by its Spanner name.
        // A table with a source table must be keyed by that source name instead.
        Table spannerOnlyTable = ddl.table(key);
        if (spannerOnlyTable == null
            || !sourceTableName(schemaMapper, spannerOnlyTable.name())
                .equals(spannerOnlyTable.name())) {
          errors.add(String.format("spannerQuery for '%s': no Spanner table is mapped to it", key));
          continue;
        }
        spannerTableName = spannerOnlyTable.name();
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
      queryBySpannerTable.put(table.name(), spannerQueries.get(key));
    }
    return queryBySpannerTable;
  }

  /** Returns the full-table query for {@code spannerTableName}, tagged with its table name. */
  static String baselineQuery(Dialect dialect, String spannerTableName) {
    // We encode the tableName in the query itself to push table information dynamically and avoid
    // table level stages.
    String quote = quote(dialect);
    return String.format(
        "SELECT *, '%s' as __tableName__ FROM %s%s%s",
        spannerTableName, quote, spannerTableName, quote);
  }

  /**
   * Returns the baseline query filtered to rows whose {@code shardIdColumn} is in {@code shardIds}.
   * The IDs are bound as one array parameter, so the SQL doesn't depend on the number of shards.
   */
  static Statement shardFilterStatement(
      Dialect dialect, String spannerTableName, String shardIdColumn, Collection<String> shardIds) {
    String column = quote(dialect) + shardIdColumn + quote(dialect);
    String filter =
        dialect == Dialect.POSTGRESQL
            ? " WHERE " + column + " = ANY($1)"
            : " WHERE " + column + " IN UNNEST(@p1)";
    return Statement.newBuilder(baselineQuery(dialect, spannerTableName) + filter)
        .bind("p1")
        .toStringArray(shardIds)
        .build();
  }

  /** Trims {@code rawQuery} and strips its trailing run of {@code ;} and whitespace. */
  static String normalizeSpannerQuery(String rawQuery) {
    return rawQuery.trim().replaceAll("[;\\s]+$", "");
  }

  /**
   * Wraps a user query so its rows are tagged with {@code spannerTableName}. The query goes on its
   * own lines so a trailing {@code --} comment can't swallow the closing parenthesis.
   */
  static String wrapSpannerQuery(String spannerTableName, String normalizedQuery) {
    // readAll merges all tables; ComparisonRecordMapper reads this tag to find and hash the table.
    // Spanner's PostgreSQL dialect requires an alias on a subquery in FROM.
    return String.format(
        "SELECT *, '%s' AS __tableName__ FROM (\n%s\n) AS __dv_src__",
        spannerTableName, normalizedQuery);
  }

  /**
   * Returns the source table name {@code spannerTableName} maps to, which is the name tableNames
   * and spannerQuery keys use. A Spanner-only table has no source table, so its Spanner name is
   * used.
   */
  private static String sourceTableName(ISchemaMapper schemaMapper, String spannerTableName) {
    try {
      return schemaMapper.getSourceTableName("", spannerTableName);
    } catch (NoSuchElementException e) {
      return spannerTableName;
    }
  }

  // TODO: @aasthabharill to check if there's a better way to generalize dialect specific changes
  private static String quote(Dialect dialect) {
    return dialect == Dialect.POSTGRESQL ? "\"" : "`";
  }
}
