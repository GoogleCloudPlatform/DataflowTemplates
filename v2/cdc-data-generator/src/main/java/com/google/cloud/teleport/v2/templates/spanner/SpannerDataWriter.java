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
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableInstant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spanner implementation of {@link DataWriter}.
 *
 * <p>Writes batches of Beam {@link Row}s to a Spanner database using {@link
 * SpannerAccessor#writeAtLeastOnce}. Supported operations are {@code INSERT} and {@code UPDATE}
 * (see {@link MutationType}) - both are translated to {@link Mutation#newInsertOrUpdateBuilder} so
 * that transient retries under {@code writeAtLeastOnce} are idempotent - and {@code DELETE} which
 * uses {@link Mutation#delete} with a key constructed from {@link DataGeneratorTable#primaryKeys}.
 *
 * <p>The Spanner connection details are loaded from a JSON configuration file with the shape {@code
 * {"projectId": ..., "instanceId": ..., "databaseId": ...}} - the same format accepted by {@link
 * SpannerSchemaFetcher}.
 */
public class SpannerDataWriter implements DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerDataWriter.class);

  @FunctionalInterface
  interface SpannerAccessorFactory {
    SpannerAccessor getOrCreate(SpannerConfig spannerConfig);
  }

  enum MutationType {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String sinkConfigPath;
  private final SpannerAccessorFactory accessorFactory;

  private transient SpannerConfig spannerConfig;
  private transient SpannerAccessor spannerAccessor;
  private transient com.google.cloud.spanner.Dialect dialect;

  public SpannerDataWriter(String sinkConfigPath) {
    this(sinkConfigPath, SpannerAccessor::getOrCreate);
  }

  @VisibleForTesting
  SpannerDataWriter(String sinkConfigPath, SpannerAccessorFactory accessorFactory) {
    this.sinkConfigPath = sinkConfigPath;
    this.accessorFactory = accessorFactory;
  }

  @VisibleForTesting
  SpannerDataWriter(SpannerConfig spannerConfig, SpannerAccessorFactory accessorFactory) {
    this.sinkConfigPath = null;
    this.accessorFactory = accessorFactory;
    this.spannerConfig = spannerConfig;
  }

  @Override
  public void insert(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    executeWrite(rows, table, MutationType.INSERT);
  }

  @Override
  public void update(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    executeWrite(rows, table, MutationType.UPDATE);
  }

  @Override
  public void delete(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    executeWrite(rows, table, MutationType.DELETE);
  }

  private void executeWrite(List<Row> rows, DataGeneratorTable table, MutationType operation) {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    ensureInitialized();

    List<Mutation> mutations = new ArrayList<>(rows.size());
    for (Row row : rows) {
      mutations.add(rowToMutation(table, row, operation));
    }

    try {
      spannerAccessor.getDatabaseClient().writeAtLeastOnce(mutations);
    } catch (RuntimeException e) {
      LOG.error(
          "Failed to write {} mutations to Spanner table {} (operation={})",
          mutations.size(),
          table.name(),
          operation,
          e);
      throw e;
    }
  }

  @VisibleForTesting
  synchronized void ensureInitialized() {
    if (spannerConfig == null) {
      spannerConfig = loadSpannerConfig();
    }
    if (spannerAccessor == null) {
      spannerAccessor = accessorFactory.getOrCreate(spannerConfig);
    }
    if (dialect == null) {
      dialect =
          spannerAccessor
              .getDatabaseAdminClient()
              .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
              .getDialect();
      LOG.info("Detected Spanner database dialect: {}", dialect);
    }
  }

  private SpannerConfig loadSpannerConfig() {
    if (sinkConfigPath == null || sinkConfigPath.isEmpty()) {
      throw new IllegalArgumentException("Spanner sink requires a valid configuration file path.");
    }
    try {
      MatchResult match = FileSystems.match(sinkConfigPath);
      if (match.metadata().isEmpty()) {
        throw new RuntimeException("Spanner sink config file not found: " + sinkConfigPath);
      }
      ResourceId resourceId = match.metadata().get(0).resourceId();
      ReadableByteChannel channel = FileSystems.open(resourceId);
      String content;
      try (BufferedReader reader = new BufferedReader(Channels.newReader(channel, "UTF-8"))) {
        content = reader.lines().collect(Collectors.joining(System.lineSeparator()));
      }
      JSONObject json = new JSONObject(content);
      return SpannerConfig.create()
          .withProjectId(
              StaticValueProvider.of(json.getString(Constants.SPANNER_CONFIG_PROJECT_ID_KEY)))
          .withInstanceId(
              StaticValueProvider.of(json.getString(Constants.SPANNER_CONFIG_INSTANCE_ID_KEY)))
          .withDatabaseId(
              StaticValueProvider.of(json.getString(Constants.SPANNER_CONFIG_DATABASE_ID_KEY)));
    } catch (java.io.IOException e) {
      throw new RuntimeException("Error reading Spanner sink config file: " + sinkConfigPath, e);
    }
  }

  @VisibleForTesting
  Mutation rowToMutation(DataGeneratorTable table, Row row, MutationType operation) {
    if (operation == MutationType.DELETE) {
      return rowToDeleteMutation(table, row);
    }
    // INSERT and UPDATE are both translated to INSERT_OR_UPDATE so that transient
    // retries of
    // writeAtLeastOnce do not fail when the row already exists (for INSERT) or no
    // longer exists
    // (for UPDATE). DELETE is handled above.
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());
    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped() || col.isGenerated()) {
        continue;
      }
      Object val = fetchFromRow(row, col.name());
      setColumnValue(builder, col, val);
    }
    return builder.build();
  }

  private Mutation rowToDeleteMutation(DataGeneratorTable table, Row row) {
    List<String> pks = table.primaryKeys();
    Key.Builder keyBuilder = Key.newBuilder();
    for (String pkName : pks) {
      Object val = fetchFromRow(row, pkName);
      if (val == null) {
        throw new IllegalStateException(
            "Primary key value missing for column '" + pkName + "' in table " + table.name());
      }
      DataGeneratorColumn col = findColumn(table, pkName);
      appendToKey(keyBuilder, col.logicalType(), val);
    }
    return Mutation.delete(table.name(), keyBuilder.build());
  }

  private static DataGeneratorColumn findColumn(DataGeneratorTable table, String name) {
    for (DataGeneratorColumn col : table.columns()) {
      if (col.name().equals(name)) {
        return col;
      }
    }
    throw new IllegalStateException(
        "Primary key column '" + name + "' not found on table " + table.name());
  }

  private static void appendToKey(Key.Builder builder, LogicalType type, Object value) {
    switch (type) {
      case INT64:
        builder.append(((Number) value).longValue());
        break;
      case FLOAT64:
        builder.append(((Number) value).doubleValue());
        break;
      case BOOLEAN:
        builder.append((Boolean) value);
        break;
      case STRING:
      case JSON:
        builder.append(value.toString());
        break;
      case NUMERIC:
        builder.append(
            value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString()));
        break;
      case BYTES:
        builder.append(ByteArray.copyFrom((byte[]) value));
        break;
      case DATE:
        builder.append(toSpannerDate(value));
        break;
      case TIMESTAMP:
        builder.append(toSpannerTimestamp(value));
        break;
      default:
        builder.append(value.toString());
    }
  }

  private void setColumnValue(
      Mutation.WriteBuilder builder, DataGeneratorColumn column, Object value) {
    String name = column.name();
    LogicalType type = column.logicalType();
    if (value == null) {
      // Emit an explicit typed null so Spanner doesn't complain about missing
      // non-null columns
      // being skipped on UPDATE mutations.
      setNull(builder, name, type);
      return;
    }
    switch (type) {
      case STRING:
        builder.set(name).to(value.toString());
        break;
      case JSON:
        if (dialect == com.google.cloud.spanner.Dialect.POSTGRESQL) {
          builder.set(name).to(Value.pgJsonb(value.toString()));
        } else {
          builder.set(name).to(Value.json(value.toString()));
        }
        break;
      case INT64:
        builder.set(name).to(((Number) value).longValue());
        break;
      case FLOAT64:
        builder.set(name).to(((Number) value).doubleValue());
        break;
      case NUMERIC:
        if (dialect == com.google.cloud.spanner.Dialect.POSTGRESQL) {
          builder.set(name).to(Value.pgNumeric(value.toString()));
        } else {
          builder
              .set(name)
              .to(
                  value instanceof BigDecimal
                      ? (BigDecimal) value
                      : new BigDecimal(value.toString()));
        }
        break;
      case BOOLEAN:
        builder.set(name).to((Boolean) value);
        break;
      case BYTES:
        builder.set(name).to(Value.bytes(ByteArray.copyFrom((byte[]) value)));
        break;
      case DATE:
        builder.set(name).to(toSpannerDate(value));
        break;
      case TIMESTAMP:
        builder.set(name).to(toSpannerTimestamp(value));
        break;
      default:
        builder.set(name).to(value.toString());
    }
  }

  private void setNull(Mutation.WriteBuilder builder, String name, LogicalType type) {
    switch (type) {
      case STRING:
        builder.set(name).to((String) null);
        break;
      case JSON:
        if (dialect == com.google.cloud.spanner.Dialect.POSTGRESQL) {
          builder.set(name).to(Value.pgJsonb((String) null));
        } else {
          builder.set(name).to(Value.json((String) null));
        }
        break;
      case INT64:
        builder.set(name).to((Long) null);
        break;
      case FLOAT64:
        builder.set(name).to((Double) null);
        break;
      case NUMERIC:
        if (dialect == com.google.cloud.spanner.Dialect.POSTGRESQL) {
          builder.set(name).to(Value.pgNumeric((String) null));
        } else {
          builder.set(name).to((BigDecimal) null);
        }
        break;
      case BOOLEAN:
        builder.set(name).to((Boolean) null);
        break;
      case BYTES:
        builder.set(name).to((ByteArray) null);
        break;
      case DATE:
        builder.set(name).to((Date) null);
        break;
      case TIMESTAMP:
        builder.set(name).to((Timestamp) null);
        break;
      default:
        builder.set(name).to((String) null);
    }
  }

  private static Date toSpannerDate(Object value) {
    return Date.fromJavaUtilDate(new java.util.Date(((ReadableInstant) value).getMillis()));
  }

  private static Timestamp toSpannerTimestamp(Object value) {
    return Timestamp.ofTimeMicroseconds(((ReadableInstant) value).getMillis() * 1000L);
  }

  private static Object fetchFromRow(Row row, String fieldName) {
    if (row == null) {
      return null;
    }
    if (!row.getSchema().hasField(fieldName)) {
      return null;
    }
    return row.getValue(fieldName);
  }

  @Override
  public void close() {
    if (spannerAccessor != null) {
      spannerAccessor.close();
      spannerAccessor = null;
    }
  }
}
