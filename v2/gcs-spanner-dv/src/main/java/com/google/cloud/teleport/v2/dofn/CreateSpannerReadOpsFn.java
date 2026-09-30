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
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
    if (tableConfig == null || !tableConfig.hasShardFilter()) {
      return readAllRows(ddl, schemaMapper);
    }
    return readSelectedShards(ddl, schemaMapper);
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
   * Reads only the rows of the selected shards. Fails with one error naming every in-scope table
   * that has no shard ID column.
   */
  private List<ReadOperation> readSelectedShards(Ddl ddl, ISchemaMapper schemaMapper) {
    List<ReadOperation> readOperations = new ArrayList<>();
    List<String> errors = new ArrayList<>();
    for (String tableName : ddl.getTablesOrderedByReference()) {
      String sourceTableName;
      try {
        sourceTableName = schemaMapper.getSourceTableName("", tableName);
      } catch (NoSuchElementException e) {
        // A Spanner-only table has no source rows to validate for any shard.
        continue;
      }
      if (!tableConfig.isSourceTableAllowed(sourceTableName)) {
        continue;
      }
      String shardIdColumn = schemaMapper.getShardIdColumnName("", tableName);
      if (shardIdColumn == null) {
        errors.add(
            String.format(
                "table '%s': --shardIds is set, but the table has no ShardIdColumn in the session"
                    + " file and no spannerQuery",
                tableName));
        continue;
      }
      Statement statement =
          pathAStatement(ddl.dialect(), tableName, shardIdColumn, tableConfig.getShardIds());
      LOG.info("Spanner query for table '{}': {}", tableName, statement.getSql());
      readOperations.add(ReadOperation.create().withQuery(statement));
    }
    if (!errors.isEmpty()) {
      Collections.sort(errors);
      throw new IllegalArgumentException(
          "Cannot read the selected shards from Spanner:\n" + String.join("\n", errors));
    }
    return readOperations;
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

  private static String quote(Dialect dialect) {
    return dialect == Dialect.POSTGRESQL ? "\"" : "`";
  }
}
