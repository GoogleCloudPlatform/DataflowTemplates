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
package com.google.cloud.teleport.v2.templates.mysql;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL implementation of {@link DataWriter}.
 *
 * <p>Writes batches of Beam {@link Row}s to one or more sharded MySQL instances described by a JSON
 * shard configuration file. Connections are pooled per logical shard via {@link
 * JdbcConnectionHelper}, and writes are routed to a specific shard using the {@code shardId}
 * argument of {@link #write(List, DataGeneratorTable, String, String)}.
 *
 * <p>Supported operations are {@code MutationType.INSERT}, {@code MutationType.UPDATE} and {@code
 * MutationType.DELETE}. INSERT and UPDATE are both issued as {@code INSERT ... ON DUPLICATE KEY
 * UPDATE} upserts so that transient retries of a batch are idempotent. DELETE uses {@link
 * DataGeneratorTable#primaryKeys} to construct the {@code WHERE} clause.
 */
public class MySqlDataWriter implements DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlDataWriter.class);

  enum MutationType {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String sinkConfigPath;
  private final ShardFileReader shardFileReader;

  private transient IConnectionHelper<Connection> connectionHelper;
  private transient Map<String, Shard> shardsByLogicalId;

  public MySqlDataWriter(String sinkConfigPath) {
    this(sinkConfigPath, new ShardFileReader(new SecretManagerAccessorImpl()), null);
  }

  @VisibleForTesting
  MySqlDataWriter(
      String sinkConfigPath,
      ShardFileReader shardFileReader,
      IConnectionHelper<Connection> connectionHelper) {
    this.sinkConfigPath = sinkConfigPath;
    this.shardFileReader = shardFileReader;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void insert(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(
        rows, table, shardId, maxShardConnections, buildUpsertSql(table), MutationType.INSERT);
  }

  @Override
  public void update(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(
        rows, table, shardId, maxShardConnections, buildUpsertSql(table), MutationType.UPDATE);
  }

  @Override
  public void delete(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(
        rows, table, shardId, maxShardConnections, buildDeleteSql(table), MutationType.DELETE);
  }

  private void executeWrite(
      List<Row> rows,
      DataGeneratorTable table,
      String shardId,
      int maxShardConnections,
      String sql,
      MutationType operation) {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    ensureInitialized(maxShardConnections);

    Shard shard = resolveShard(shardId);
    String connectionKey = buildConnectionKey(shard);

    try (Connection connection = connectionHelper.getConnection(connectionKey)) {
      if (connection == null) {
        throw new RuntimeException(
            "No MySQL connection available for shard '"
                + shard.getLogicalShardId()
                + "' (key="
                + connectionKey
                + ")");
      }
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        for (Row row : rows) {
          setStatementParameters(statement, row, table, operation);
          statement.addBatch();
        }
        statement.executeBatch();
      }
    } catch (ConnectionException | SQLException e) {
      LOG.error(
          "Failed to execute write (operation: {}) for {} rows on MySQL shard {}",
          operation,
          rows.size(),
          shardId,
          e);
      throw new RuntimeException("Failed to write to MySQL shard " + shardId, e);
    }
  }

  @VisibleForTesting
  synchronized void ensureInitialized(int maxShardConnections) {
    if (connectionHelper == null) {
      connectionHelper = new JdbcConnectionHelper();
    }
    if (shardsByLogicalId == null) {
      List<Shard> shards = loadShards();
      Map<String, Shard> byId = new LinkedHashMap<>();
      for (Shard shard : shards) {
        byId.put(shard.getLogicalShardId(), shard);
      }
      this.shardsByLogicalId = Collections.unmodifiableMap(byId);
    }
    if (!connectionHelper.isConnectionPoolInitialized()) {
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              ImmutableList.copyOf(shardsByLogicalId.values()),
              "",
              maxShardConnections,
              Constants.MYSQL_JDBC_DRIVER,
              "",
              Constants.JDBC_MYSQL_URL_PREFIX);
      connectionHelper.init(request);
    }
  }

  private List<Shard> loadShards() {
    if (sinkConfigPath == null || sinkConfigPath.isEmpty()) {
      throw new IllegalArgumentException(
          "MySQL sink requires a valid shard configuration file path.");
    }
    List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkConfigPath);
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided shard file: " + sinkConfigPath);
    }
    return shards;
  }

  @VisibleForTesting
  Shard resolveShard(String shardId) {
    if (shardsByLogicalId == null || shardsByLogicalId.isEmpty()) {
      throw new IllegalStateException("Shard configuration has not been loaded.");
    }
    if (shardId == null || shardId.isEmpty()) {
      // No shard specified - fall back to the first shard.
      return shardsByLogicalId.values().iterator().next();
    }
    Shard shard = shardsByLogicalId.get(shardId);
    if (shard == null) {
      throw new IllegalArgumentException(
          "Shard ID '" + shardId + "' not found in sink configuration.");
    }
    return shard;
  }

  @VisibleForTesting
  static String buildConnectionKey(Shard shard) {
    return Constants.JDBC_MYSQL_URL_PREFIX
        + shard.getHost()
        + ":"
        + shard.getPort()
        + "/"
        + (shard.getDbName() == null ? "" : shard.getDbName())
        + "/"
        + shard.getUserName();
  }

  /**
   * Builds an {@code INSERT INTO t (...) VALUES (...) ON DUPLICATE KEY UPDATE ...} statement that
   * is used for both {@link Constants#MUTATION_INSERT} and {@link Constants#MUTATION_UPDATE}
   * operations. The statement binds a single set of parameters (all writable columns, in table
   * order). For tables that only consist of primary-key columns the {@code ON DUPLICATE KEY UPDATE}
   * clause degrades to a self-assignment on the first PK column, which is still a valid no-op
   * upsert.
   */
  @VisibleForTesting
  static String buildUpsertSql(DataGeneratorTable table) {
    List<DataGeneratorColumn> cols = writableColumns(table);
    if (cols.isEmpty()) {
      throw new IllegalArgumentException(
          "Table " + table.name() + " has no writable columns for INSERT / UPDATE.");
    }
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(quote(table.name())).append(" (");
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(quote(cols.get(i).name()));
    }
    sql.append(") VALUES (");
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("?");
    }
    sql.append(") ON DUPLICATE KEY UPDATE ");

    List<DataGeneratorColumn> setCols = nonPrimaryKeyWritableColumns(table);
    if (setCols.isEmpty()) {
      // PK-only table: use a harmless self-assignment on the first column so MySQL still parses
      // the clause and retries remain idempotent.
      String firstCol = quote(cols.get(0).name());
      sql.append(firstCol).append(" = ").append(firstCol);
    } else {
      for (int i = 0; i < setCols.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        String c = quote(setCols.get(i).name());
        sql.append(c).append(" = VALUES(").append(c).append(")");
      }
    }
    return sql.toString();
  }

  @VisibleForTesting
  static String buildDeleteSql(DataGeneratorTable table) {
    List<String> pkNames = table.primaryKeys();
    if (pkNames == null || pkNames.isEmpty()) {
      throw new IllegalStateException(
          "Table " + table.name() + " has no primary key; cannot build DELETE.");
    }
    StringBuilder sql = new StringBuilder("DELETE FROM ");
    sql.append(quote(table.name())).append(" WHERE ");
    for (int i = 0; i < pkNames.size(); i++) {
      if (i > 0) {
        sql.append(" AND ");
      }
      sql.append(quote(pkNames.get(i))).append(" = ?");
    }
    return sql.toString();
  }

  private static List<DataGeneratorColumn> writableColumns(DataGeneratorTable table) {
    ImmutableList.Builder<DataGeneratorColumn> builder = ImmutableList.builder();
    for (DataGeneratorColumn col : table.columns()) {
      if (col.isSkipped() || col.isGenerated()) {
        continue;
      }
      builder.add(col);
    }
    return builder.build();
  }

  private static List<DataGeneratorColumn> nonPrimaryKeyWritableColumns(DataGeneratorTable table) {
    List<String> pkNames = table.primaryKeys();
    ImmutableList.Builder<DataGeneratorColumn> builder = ImmutableList.builder();
    for (DataGeneratorColumn col : writableColumns(table)) {
      if (pkNames != null && pkNames.contains(col.name())) {
        continue;
      }
      builder.add(col);
    }
    return builder.build();
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

  @VisibleForTesting
  void setStatementParameters(
      PreparedStatement statement, Row row, DataGeneratorTable table, MutationType operation)
      throws SQLException {
    if (operation == MutationType.DELETE) {
      int idx = 1;
      for (String pkName : table.primaryKeys()) {
        DataGeneratorColumn pkCol = findColumn(table, pkName);
        setParameter(statement, idx++, pkCol, fetchFromRow(row, pkName));
      }
    } else {
      // INSERT and UPDATE both use the upsert statement built by buildUpsertSql(...), which binds
      // all writable columns in table order once (the ON DUPLICATE KEY UPDATE clause references
      // VALUES(col) on the server side, so no extra parameters are needed).
      int idx = 1;
      for (DataGeneratorColumn col : writableColumns(table)) {
        setParameter(statement, idx++, col, fetchFromRow(row, col.name()));
      }
    }
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

  private static void setParameter(
      PreparedStatement statement, int index, DataGeneratorColumn column, Object value)
      throws SQLException {
    if (value == null) {
      statement.setNull(index, jdbcTypeFor(column.logicalType()));
      return;
    }
    switch (column.logicalType()) {
      case STRING:
      case JSON:
        statement.setString(index, value.toString());
        break;
      case INT64:
        statement.setLong(index, ((Number) value).longValue());
        break;
      case FLOAT64:
        statement.setDouble(index, ((Number) value).doubleValue());
        break;
      case BOOLEAN:
        statement.setBoolean(index, (Boolean) value);
        break;
      case BYTES:
        statement.setBytes(index, (byte[]) value);
        break;
      case NUMERIC:
        if (value instanceof BigDecimal) {
          statement.setBigDecimal(index, (BigDecimal) value);
        } else {
          statement.setBigDecimal(index, new BigDecimal(value.toString()));
        }
        break;
      case DATE:
        statement.setDate(index, toSqlDate(value));
        break;
      case TIMESTAMP:
        statement.setTimestamp(index, toSqlTimestamp(value));
        break;
      default:
        statement.setObject(index, value);
    }
  }

  private static java.sql.Date toSqlDate(Object value) {
    return new java.sql.Date(((ReadableInstant) value).getMillis());
  }

  private static java.sql.Timestamp toSqlTimestamp(Object value) {
    return new java.sql.Timestamp(((ReadableInstant) value).getMillis());
  }

  private static int jdbcTypeFor(LogicalType type) {
    switch (type) {
      case STRING:
      case JSON:
      case UUID:
        return Types.VARCHAR;
      case INT64:
        return Types.BIGINT;
      case FLOAT64:
        return Types.DOUBLE;
      case BOOLEAN:
        return Types.BOOLEAN;
      case BYTES:
        return Types.VARBINARY;
      case NUMERIC:
        return Types.NUMERIC;
      case DATE:
        return Types.DATE;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      default:
        return Types.OTHER;
    }
  }

  private static String quote(String identifier) {
    // Back-tick quote identifiers so that reserved keywords and mixed-case names are handled
    // correctly. Any embedded back-tick is doubled per MySQL escaping rules.
    return Constants.MYSQL_IDENTIFIER_QUOTE
        + identifier.replace(
            Constants.MYSQL_IDENTIFIER_QUOTE,
            Constants.MYSQL_IDENTIFIER_QUOTE + Constants.MYSQL_IDENTIFIER_QUOTE)
        + Constants.MYSQL_IDENTIFIER_QUOTE;
  }

  @Override
  public void close() {
    // The JdbcConnectionHelper is a per-worker singleton and is intentionally not closed here;
    // the transient reference is dropped along with the owning DoFn.
  }
}
