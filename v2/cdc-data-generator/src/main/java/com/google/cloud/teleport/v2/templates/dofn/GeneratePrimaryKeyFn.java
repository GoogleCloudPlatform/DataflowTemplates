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
import com.google.cloud.teleport.v2.spanner.utils.CustomDataGenerator;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.CustomDataGeneratorFetcher;
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
 * sampled uniformly from the configured logical shard ids (for MySQL sinks with a shard config
 * file), or defaults to {@code shard0}.
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

  private final SinkConfig sinkConfig;
  private final String sinkType;

  private transient Faker faker;
  private transient CustomDataGenerator customGenerator;
  private transient List<String> logicalShardIds;
  private final String customJarPath;
  private final String customClassName;

  /** Per-table PK schema cache. Schemas are static per pipeline run so this is write-once. */
  private transient Map<String, Schema> schemaCache;

  public GeneratePrimaryKeyFn(
      SinkConfig sinkConfig, String sinkType, String customJarPath, String customClassName) {
    this.sinkConfig = sinkConfig;
    this.sinkType = sinkType;
    this.customJarPath = customJarPath;
    this.customClassName = customClassName;
  }

  @Setup
  public void setup() {
    customGenerator =
        CustomDataGeneratorFetcher.getCustomDataGenerator(customJarPath, customClassName);
    faker = new Faker();
    schemaCache = new HashMap<>();

    if (sinkConfig instanceof MySqlSinkConfig) {
      MySqlSinkConfig mySqlSinkConfig = (MySqlSinkConfig) sinkConfig;
      if (mySqlSinkConfig.getShards() != null) {
        this.logicalShardIds =
            mySqlSinkConfig.getShards().stream()
                .map(Shard::getLogicalShardId)
                .collect(Collectors.toList());
      }
    }
  }

  @ProcessElement
  public void processElement(
      @Element DataGeneratorTable table, OutputReceiver<KV<String, Row>> out) {
    List<DataGeneratorColumn> pkColumns = primaryKeyColumns(table);

    Schema schema = schemaCache.computeIfAbsent(table.name(), k -> buildSchema(pkColumns));
    Row.Builder rowBuilder = Row.withSchema(schema);

    for (DataGeneratorColumn column : pkColumns) {
      rowBuilder.addValue(
          DataGeneratorUtils.generateValue(table.name(), column, faker, customGenerator));
    }
    rowBuilder.addValue(pickShardId());

    try {
      out.output(KV.of(table.name(), rowBuilder.build()));
    } catch (IllegalArgumentException | ClassCastException e) {
      throw new RuntimeException(
          "Failed to assemble root primary key for table '"
              + table.name()
              + "'. (If using a CustomDataGenerator, check its return types). Expected schema: "
              + schema,
          e);
    }
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
        .filter(byName::containsKey)
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
   * placeholder {@code shard0}.
   */
  private String pickShardId() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    if (logicalShardIds != null && !logicalShardIds.isEmpty()) {
      return logicalShardIds.get(rng.nextInt(logicalShardIds.size()));
    }
    return "shard0";
  }
}
