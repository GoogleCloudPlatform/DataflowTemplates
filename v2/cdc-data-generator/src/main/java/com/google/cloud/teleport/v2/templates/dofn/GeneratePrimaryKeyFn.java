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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import net.datafaker.Faker;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a synthetic primary-key Row for each incoming {@link DataGeneratorTable}.
 *
 * <p>The emitted {@link Row} has the table's PK columns followed by a synthesised logical shard id
 * column (name controlled by {@link Constants#SHARD_ID_COLUMN_NAME}). The shard id is either
 * sampled uniformly from {@code [0, maxShards)} or, for MySQL sinks with a shard config file,
 * sampled uniformly from the configured logical shard ids so the same shard ids used by downstream
 * writers are reused here.
 *
 * <p>Randomness: {@link Faker} is constructed with its default no-arg constructor (which seeds
 * itself from {@code System.nanoTime()} plus a per-instance counter and shard-id selection uses
 * {@link ThreadLocalRandom#current()} to avoid contention under high fan-out.
 *
 * <p>Output: {@code KV<tableName, pkRow>} so downstream transforms can shuffle by table while
 * keeping the row schema per-table.
 */
public class GeneratePrimaryKeyFn extends DoFn<DataGeneratorTable, KV<String, Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(GeneratePrimaryKeyFn.class);

  private final String sinkOptionsPath;
  private final String sinkType;

  private transient Faker faker;
  private transient List<String> logicalShardIds;

  /** Per-table PK schema cache. Schemas are static per pipeline run so this is write-once. */
  private transient Map<String, Schema> schemaCache;

  public GeneratePrimaryKeyFn(String sinkOptionsPath, String sinkType) {
    this.sinkOptionsPath = sinkOptionsPath;
    this.sinkType = sinkType;
  }

  @Setup
  public void setup() {
    faker = new Faker();
    schemaCache = new HashMap<>();

    if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)
        && sinkOptionsPath != null
        && !sinkOptionsPath.isEmpty()) {
      try {
        ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
        List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkOptionsPath);
        this.logicalShardIds =
            shards == null || shards.isEmpty()
                ? null
                : shards.stream().map(Shard::getLogicalShardId).collect(Collectors.toList());
      } catch (Exception e) {
        throw new RuntimeException("Failed to read shards from " + sinkOptionsPath, e);
      }
    }
  }

  @ProcessElement
  public void processElement(
      @Element DataGeneratorTable table, OutputReceiver<KV<String, Row>> out) {
    List<DataGeneratorColumn> pkColumns = primaryKeyColumns(table);

    if (pkColumns.isEmpty()) {
      // A table without a PK cannot have a unique row key synthesised for it. This is
      // a
      // schema-definition problem, so log + skip rather than emit an empty Row (which
      // would
      // corrupt downstream joins on the PK).
      LOG.error(
          "Table {} has no primary-key columns, or its PK list references unknown columns — "
              + "skipping PK generation.",
          table.name());
      return;
    }

    Schema schema = schemaCache.computeIfAbsent(table.name(), k -> buildSchema(pkColumns));
    Row.Builder rowBuilder = Row.withSchema(schema);

    for (DataGeneratorColumn column : pkColumns) {
      rowBuilder.addValue(DataGeneratorUtils.generateValue(column, faker));
    }
    rowBuilder.addValue(pickShardId());

    out.output(KV.of(table.name(), rowBuilder.build()));
  }

  /** Resolve the PK column list for a table. */
  @VisibleForTesting
  public static List<DataGeneratorColumn> primaryKeyColumns(DataGeneratorTable table) {
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      return ImmutableList.of();
    }
    ImmutableMap<String, DataGeneratorColumn> byName =
        table.columns().stream()
            .collect(ImmutableMap.toImmutableMap(DataGeneratorColumn::name, col -> col));

    return table.primaryKeys().stream()
        .filter(
            pkName -> {
              if (!byName.containsKey(pkName)) {
                LOG.error(
                    "PK column {} declared on table {} not present in columns list — dropping.",
                    pkName,
                    table.name());
                return false;
              }
              return true;
            })
        .map(byName::get)
        .collect(Collectors.toList());
  }

  private Schema buildSchema(List<DataGeneratorColumn> columns) {
    Schema.Builder builder = Schema.builder();
    for (DataGeneratorColumn col : columns) {
      builder.addField(Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col)));
    }
    builder.addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
    return builder.build();
  }

  /**
   * Select a shard id for this row. Prefer a configured logical shard id, otherwise synthesise a
   * placeholder {@code shard<N>}. Bounded at maxShards >= 1 so {@code nextInt} never throws.
   */
  private String pickShardId() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    if (logicalShardIds != null && !logicalShardIds.isEmpty()) {
      return logicalShardIds.get(rng.nextInt(logicalShardIds.size()));
    }
    return "shard0";
  }
}
