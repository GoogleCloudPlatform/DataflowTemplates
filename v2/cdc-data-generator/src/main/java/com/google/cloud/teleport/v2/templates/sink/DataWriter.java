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
package com.google.cloud.teleport.v2.templates.sink;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import java.util.List;
import org.apache.beam.sdk.values.Row;

/**
 * Contract for writing a batch of rows to a sink on behalf of the CDC Data Generator.
 *
 * <p>Writers are obtained lazily inside DoFns (typically in {@code @Setup}). They should be
 * idempotent with respect to initialization - calling {@link #write(List, DataGeneratorTable,
 * String, String)} with an empty batch is a no-op.
 */
public interface DataWriter extends AutoCloseable {

  /**
   * Inserts {@code rows} into the sink for the given {@code table}, routing by {@code shardId}.
   *
   * @param rows rows to insert. The caller should group rows by (shard, table) so that a single
   *     call can use one prepared statement / mutation batch.
   * @param table schema metadata for the target table. Must not be {@code null}.
   * @param shardId logical shard identifier. Ignored by sinks that are not sharded.
   * @param maxShardConnections maximum number of connections per shard.
   */
  void insert(List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections);

  /**
   * Updates {@code rows} in the sink for the given {@code table}, routing by {@code shardId}.
   *
   * @param rows rows to update. The caller should group rows by (shard, table) so that a single
   *     call can use one prepared statement / mutation batch.
   * @param table schema metadata for the target table. Must not be {@code null}.
   * @param shardId logical shard identifier. Ignored by sinks that are not sharded.
   * @param maxShardConnections maximum number of connections per shard.
   */
  void update(List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections);

  /**
   * Deletes {@code rows} from the sink for the given {@code table}, routing by {@code shardId}.
   *
   * @param rows rows to delete. The caller should group rows by (shard, table) so that a single
   *     call can use one prepared statement / mutation batch.
   * @param table schema metadata for the target table. Must not be {@code null}.
   * @param shardId logical shard identifier. Ignored by sinks that are not sharded.
   * @param maxShardConnections maximum number of connections per shard.
   */
  void delete(List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections);

  /**
   * Releases sink-specific resources (Spanner accessor handles, test-owned pools, etc.). The
   * default implementation is a no-op.
   */
  @Override
  default void close() {
    // No-op by default.
  }
}
