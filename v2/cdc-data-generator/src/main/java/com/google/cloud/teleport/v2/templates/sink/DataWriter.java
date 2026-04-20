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
   * Writes {@code rows} to the sink for the given {@code table}, routing by {@code shardId} and
   * applying the given {@code operation} (INSERT / UPDATE / DELETE).
   *
   * @param rows rows to write. The caller should group rows by (shard, table, operation) so that a
   *     single call can use one prepared statement / mutation batch.
   * @param table schema metadata for the target table. Must not be {@code null}.
   * @param shardId logical shard identifier. Ignored by sinks that are not sharded. For MySQL sinks
   *     this must match a {@code logicalShardId} in the shard configuration.
   * @param operation one of {@link
   *     com.google.cloud.teleport.v2.templates.utils.Constants#MUTATION_INSERT}, {@code
   *     MUTATION_UPDATE}, or {@code MUTATION_DELETE}.
   * @param maxShardConnections maximum number of connections per shard.
   */
  void write(
      List<Row> rows,
      DataGeneratorTable table,
      String shardId,
      String operation,
      int maxShardConnections);

  /**
   * Releases sink-specific resources (Spanner accessor handles, test-owned pools, etc.). The
   * default implementation is a no-op.
   */
  @Override
  default void close() {
    // No-op by default.
  }
}
