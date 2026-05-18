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

import com.google.cloud.teleport.v2.templates.model.BufferKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects, groups, and buffers generated records in transient worker memory, chunking mutations
 * into batches based on size thresholds before executing database sink writes.
 *
 * <p>Crucially, the configured {@code batchSize} threshold from pipeline options is applied <b>per
 * table</b> (specifically partitioned by {@link BufferKey} groups of table, shard, and operation
 * type). This prevents large tables from starving smaller ones or forcing premature cross-table
 * flushes, ensuring maximum throughput and proper database transactional sizing.
 */
public class MutationBatcher {
  private static final Logger LOG = LoggerFactory.getLogger(MutationBatcher.class);

  public static final String MUTATION_INSERT = "INSERT";
  public static final String MUTATION_UPDATE = "UPDATE";
  public static final String MUTATION_DELETE = "DELETE";

  private final Integer batchSize;
  private final Integer jdbcPoolSize;
  private final DataWriter writer;

  private transient Map<BufferKey, BufferValue> buffers;
  private transient List<String> failedRecords;

  private final Counter batchesWritten = Metrics.counter(MutationBatcher.class, "batchesWritten");
  private final Counter recordsWritten = Metrics.counter(MutationBatcher.class, "recordsWritten");
  private final Counter writeFailures = Metrics.counter(MutationBatcher.class, "writeFailures");

  public MutationBatcher(int batchSize, Integer jdbcPoolSize, DataWriter writer) {
    this.batchSize = batchSize;
    this.jdbcPoolSize = jdbcPoolSize;
    this.writer = writer;
  }

  public void startBundle() {
    this.buffers = new HashMap<>();
    this.failedRecords = new ArrayList<>();
  }

  public List<String> getFailedRecords() {
    return failedRecords;
  }

  public void clearDlq() {
    if (this.failedRecords != null) {
      this.failedRecords.clear();
    }
  }

  public void bufferRow(
      String tableName,
      Row row,
      String operation,
      DataGeneratorTable table,
      String shardIdHint,
      List<String> insertTopoOrder) {
    String shardId = shardIdHint == null ? "" : shardIdHint;
    if (shardId.isEmpty() && row.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
      shardId = row.getString(Constants.SHARD_ID_COLUMN_NAME);
    }
    BufferKey bufferKey = BufferKey.create(tableName, shardId, operation);
    buffers.computeIfAbsent(bufferKey, k -> new BufferValue(table)).rows.add(row);

    if (buffers.get(bufferKey).rows.size() >= batchSize) {
      if (MUTATION_INSERT.equals(operation)) {
        flushInsertsInTopoOrder(insertTopoOrder);
      } else if (MUTATION_DELETE.equals(operation)) {
        flushDeletesInReverseTopoOrder(insertTopoOrder);
      } else {
        flush(bufferKey);
      }
    }
  }

  public void flushInsertsInTopoOrder(List<String> insertTopoOrder) {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
    for (String table : order) {
      flushByTableAndOp(table, MUTATION_INSERT);
    }
  }

  public void flushDeletesInReverseTopoOrder(List<String> insertTopoOrder) {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
    for (int i = order.size() - 1; i >= 0; i--) {
      flushByTableAndOp(order.get(i), MUTATION_DELETE);
    }
  }

  public void flushUpdates() {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    for (BufferKey bufferKey : new ArrayList<>(buffers.keySet())) {
      if (MUTATION_UPDATE.equals(bufferKey.operation())) {
        flush(bufferKey);
      }
    }
  }

  private void flushByTableAndOp(String tableName, String op) {
    for (BufferKey bufferKey : new ArrayList<>(buffers.keySet())) {
      if (bufferKey.tableName().equals(tableName) && bufferKey.operation().equals(op)) {
        flush(bufferKey);
      }
    }
  }

  private void flush(BufferKey key) {
    String tableName = key.tableName();
    String shardId = key.shardId();
    String operation = key.operation();

    BufferValue bv = buffers.get(key);
    List<Row> batch = bv != null ? bv.rows : null;
    DataGeneratorTable table = bv != null ? bv.table : null;
    if (batch == null || batch.isEmpty()) {
      if (bv != null && bv.rows.isEmpty()) {
        buffers.remove(key);
      }
      return;
    }

    int maxShardConnections =
        (this.jdbcPoolSize != null && this.jdbcPoolSize > 0)
            ? this.jdbcPoolSize
            : Constants.DEFAULT_JDBC_POOL_SIZE;
    try {
      switch (operation) {
        case MUTATION_INSERT:
          writer.insert(batch, table, shardId, maxShardConnections);
          break;
        case MUTATION_UPDATE:
          writer.update(batch, table, shardId, maxShardConnections);
          break;
        case MUTATION_DELETE:
          writer.delete(batch, table, shardId, maxShardConnections);
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + operation);
      }
      batchesWritten.inc();
      recordsWritten.inc(batch.size());
      if (!shardId.isEmpty()) {
        Metrics.counter(MutationBatcher.class, "recordsWritten_" + shardId).inc(batch.size());
      }
      Metrics.counter(MutationBatcher.class, operation.toLowerCase() + "_" + tableName)
          .inc(batch.size());
    } catch (Exception writeError) {
      LOG.error(
          "Sink write failed for table {} ({}); {} rows routed to DLQ",
          tableName,
          operation,
          batch.size(),
          writeError);
      writeFailures.inc(batch.size());
      for (Row r : batch) {
        failedRecords.add(FailureRecord.toJson(tableName, operation, r, writeError));
      }
    } finally {
      batch.clear();
      buffers.remove(key);
    }
  }

  private static final class BufferValue {
    final DataGeneratorTable table;
    final List<Row> rows;

    BufferValue(DataGeneratorTable table) {
      this.table = table;
      this.rows = new ArrayList<>();
    }
  }
}
