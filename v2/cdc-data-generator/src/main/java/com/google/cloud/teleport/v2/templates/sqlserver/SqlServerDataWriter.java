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
package com.google.cloud.teleport.v2.templates.sqlserver;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SqlServerSinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Types;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Server implementation of {@link DataWriter}.
 *
 * <p>Writes batches of Beam {@link Row}s to one or more sharded SQL Server instances described by a
 * JSON shard configuration file. Connections are pooled per logical shard via {@link
 * JdbcConnectionHelper}, and writes are routed to a specific shard using the {@code shardId}
 * argument of the {@link DataWriter} methods.
 *
 * <p>Supported operations are {@code MutationType.INSERT}, {@code MutationType.UPDATE} and {@code
 * MutationType.DELETE}. INSERT is issued as a plain batched {@code INSERT} (eligible for the
 * driver's bulk-copy batching) and falls back to a {@code MERGE} upsert on duplicate keys so that
 * retries stay idempotent; UPDATE always uses the {@code MERGE} upsert keyed on the table's primary
 * key. DELETE uses {@link DataGeneratorTable#primaryKeys} to construct the {@code WHERE} clause.
 * Each batch runs in one transaction and transient failures are retried.
 *
 * <p>{@code IDENTITY} columns are not supported: SQL Server rejects explicit values for them and
 * the generator always supplies primary-key values itself.
 */
public class SqlServerDataWriter implements DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerDataWriter.class);

  /**
   * Worker-wide connection helper. {@link JdbcConnectionHelper} keeps its pool map in a static
   * field but guards initialisation with an instance-level lock, so every writer must share the
   * same instance; otherwise the many DoFn instances on a worker race in {@code init()} and each
   * creates its own set of HikariCP pools.
   */
  private static final IConnectionHelper<Connection> SHARED_CONNECTION_HELPER =
      new JdbcConnectionHelper();

  /** Attempts per batch for transient failures (deadlock, lock/pool timeout). */
  @VisibleForTesting static final int MAX_ATTEMPTS = 3;

  private static final long RETRY_BACKOFF_MS = 500L;

  enum MutationType {
    INSERT,
    UPDATE,
    DELETE
  }

  private final SqlServerSinkConfig sqlServerSinkConfig;

  private transient IConnectionHelper<Connection> connectionHelper;
  private transient Map<String, Shard> shardsByLogicalId;

  public SqlServerDataWriter(SqlServerSinkConfig sqlServerSinkConfig) {
    this(sqlServerSinkConfig, null);
  }

  @VisibleForTesting
  SqlServerDataWriter(
      SqlServerSinkConfig sqlServerSinkConfig, IConnectionHelper<Connection> connectionHelper) {
    this.sqlServerSinkConfig = sqlServerSinkConfig;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void insert(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    // Fast path: a plain parameterised INSERT is eligible for the JDBC driver's bulk-copy batching
    // (useBulkCopyForBatchInsert=true) and avoids a MERGE seek per row. If the batch is a retry and
    // some rows already exist, the primary-key violation makes us fall back to the idempotent
    // MERGE for that batch.
    executeWrite(
        rows,
        table,
        shardId,
        maxShardConnections,
        buildInsertSql(table),
        buildUpsertSql(table),
        MutationType.INSERT);
  }

  @Override
  public void update(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(
        rows,
        table,
        shardId,
        maxShardConnections,
        buildUpsertSql(table),
        null,
        MutationType.UPDATE);
  }

  @Override
  public void delete(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(
        rows,
        table,
        shardId,
        maxShardConnections,
        buildDeleteSql(table),
        null,
        MutationType.DELETE);
  }

  /**
   * Executes {@code sql} for all rows as a single transaction, retrying transient failures
   * (deadlock victim, lock timeout, connection-pool timeout) up to {@link #MAX_ATTEMPTS} times. If
   * {@code duplicateKeyFallbackSql} is non-null and the statement fails with a primary/unique key
   * violation, the batch is re-executed with the fallback statement (an idempotent upsert).
   */
  private void executeWrite(
      List<Row> rows,
      DataGeneratorTable table,
      String shardId,
      int maxShardConnections,
      String sql,
      String duplicateKeyFallbackSql,
      MutationType operation) {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    ensureInitialized(maxShardConnections);

    Shard shard = resolveShard(shardId);
    String connectionKey = buildConnectionKey(shard);

    Exception lastFailure = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (Connection connection = connectionHelper.getConnection(connectionKey)) {
        if (connection == null) {
          throw new RuntimeException(
              "No SQL Server connection available for shard '"
                  + shard.getLogicalShardId()
                  + "' (key="
                  + connectionKey
                  + ")");
        }
        try {
          executeBatchInTransaction(connection, sql, rows, table, operation);
        } catch (SQLException e) {
          if (duplicateKeyFallbackSql != null && isDuplicateKey(e)) {
            LOG.info(
                "Duplicate key on {} {} rows for table {} on shard {}; re-running batch as upsert.",
                operation,
                rows.size(),
                table.name(),
                shardId);
            executeBatchInTransaction(connection, duplicateKeyFallbackSql, rows, table, operation);
          } else {
            throw e;
          }
        }
        return;
      } catch (ConnectionException | SQLException e) {
        lastFailure = e;
        if (attempt < MAX_ATTEMPTS && isTransient(e)) {
          LOG.warn(
              "Transient failure on {} {} rows for table {} on shard {} (attempt {}/{}): {}",
              operation,
              rows.size(),
              table.name(),
              shardId,
              attempt,
              MAX_ATTEMPTS,
              rootMessage(e));
          sleepBeforeRetry(attempt);
          continue;
        }
        break;
      }
    }
    LOG.error(
        "Failed to execute write (operation: {}) for {} rows on SQL Server shard {}",
        operation,
        rows.size(),
        shardId,
        lastFailure);
    throw new RuntimeException("Failed to write to SQL Server shard " + shardId, lastFailure);
  }

  /**
   * Runs the batch inside one transaction: one log flush per batch instead of one per row, which is
   * the dominant cost for high-volume inserts on SQL Server. Rolls back on failure and always
   * restores the connection's auto-commit mode before it goes back to the pool.
   */
  private void executeBatchInTransaction(
      Connection connection,
      String sql,
      List<Row> rows,
      DataGeneratorTable table,
      MutationType operation)
      throws SQLException {
    boolean previousAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (Row row : rows) {
        setStatementParameters(statement, row, table, operation);
        statement.addBatch();
      }
      statement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException rollbackError) {
        e.addSuppressed(rollbackError);
      }
      throw e;
    } finally {
      connection.setAutoCommit(previousAutoCommit);
    }
  }

  /**
   * Detects a primary-key / unique-constraint violation anywhere in the cause chain. SQL Server
   * reports error 2627 (constraint) or 2601 (unique index) with SQLState 23000; the bulk-copy code
   * path of the JDBC driver does not always propagate the vendor error code, so the message is
   * checked as well.
   */
  @VisibleForTesting
  static boolean isDuplicateKey(SQLException e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLException) {
        SQLException se = (SQLException) t;
        int code = se.getErrorCode();
        if (code == 2627 || code == 2601 || "23000".equals(se.getSQLState())) {
          return true;
        }
      }
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("duplicate key")
            || lower.contains("violation of primary key")
            || lower.contains("violation of unique")) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * SQL Server error numbers: 1205 = deadlock victim, 1222 = lock request timeout; plus connection
   * pool / network timeouts surfaced by HikariCP and the driver.
   */
  @VisibleForTesting
  static boolean isTransient(Exception e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLTransientException) {
        return true;
      }
      if (t instanceof SQLException) {
        int code = ((SQLException) t).getErrorCode();
        if (code == 1205 || code == 1222) {
          return true;
        }
      }
      if (t instanceof java.net.SocketTimeoutException) {
        return true;
      }
    }
    return false;
  }

  private static String rootMessage(Throwable e) {
    Throwable t = e;
    while (t.getCause() != null) {
      t = t.getCause();
    }
    return t.getMessage();
  }

  @VisibleForTesting
  protected void sleepBeforeRetry(int attempt) {
    try {
      Thread.sleep(RETRY_BACKOFF_MS * attempt);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  synchronized void ensureInitialized(int maxShardConnections) {
    if (connectionHelper == null) {
      connectionHelper = SHARED_CONNECTION_HELPER;
    }
    if (shardsByLogicalId == null) {
      List<Shard> shards =
          sqlServerSinkConfig != null ? sqlServerSinkConfig.getShards() : Collections.emptyList();
      if (shards == null || shards.isEmpty()) {
        throw new RuntimeException(
            "No shards found in the provided SQL Server sink configuration.");
      }
      Map<String, Shard> byId = new LinkedHashMap<>();
      for (Shard shard : shards) {
        byId.put(shard.getLogicalShardId(), shard);
      }
      this.shardsByLogicalId = Collections.unmodifiableMap(byId);
    }
    if (!connectionHelper.isConnectionPoolInitialized()) {
      for (Shard shard : shardsByLogicalId.values()) {
        shard.setConnectionUrl(buildConnectionUrl(shard));
      }
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              ImmutableList.copyOf(shardsByLogicalId.values()),
              "",
              maxShardConnections,
              Constants.SQLSERVER_JDBC_DRIVER,
              "");
      connectionHelper.init(request);
    }
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

  /**
   * Builds the JDBC URL for a shard, e.g. {@code
   * jdbc:sqlserver://host:1433;databaseName=db;trustServerCertificate=true;encrypt=false}.
   *
   * <p>{@code encrypt=false} and {@code trustServerCertificate=true} are applied as defaults
   * (matching the other SQL Server templates in this repository) unless the shard's {@code
   * connectionProperties} already specify them. Any additional {@code connectionProperties} are
   * appended verbatim as {@code ;key=value} pairs.
   */
  @VisibleForTesting
  static String buildConnectionUrl(Shard shard) {
    StringBuilder url =
        new StringBuilder(Constants.JDBC_SQLSERVER_URL_PREFIX)
            .append(shard.getHost())
            .append(":")
            .append(shard.getPort());
    if (shard.getDbName() != null && !shard.getDbName().isEmpty()) {
      url.append(";databaseName=").append(shard.getDbName());
    }
    String connectionProperties = shard.getConnectionProperties();
    String propsLower =
        connectionProperties != null ? connectionProperties.toLowerCase(Locale.ROOT) : "";
    if (!propsLower.contains("trustservercertificate")) {
      url.append(";trustServerCertificate=true");
    }
    if (!propsLower.contains("encrypt")) {
      url.append(";encrypt=false");
    }
    if (connectionProperties != null && !connectionProperties.trim().isEmpty()) {
      String props = connectionProperties.trim();
      if (!props.startsWith(";")) {
        url.append(";");
      }
      url.append(props);
    }
    return url.toString();
  }

  @VisibleForTesting
  static String buildConnectionKey(Shard shard) {
    return buildConnectionUrl(shard) + "/" + shard.getUserName();
  }

  /**
   * Builds a plain {@code INSERT INTO t (c1, c2, ...) VALUES (?, ?, ...)} statement over all
   * writable columns in table order. Unlike {@link #buildUpsertSql}, this statement is eligible for
   * the SQL Server JDBC driver's bulk-copy batching ({@code useBulkCopyForBatchInsert=true}), which
   * is dramatically faster for high-volume inserts; it is not idempotent on its own, so callers
   * fall back to the upsert on duplicate-key errors.
   */
  @VisibleForTesting
  static String buildInsertSql(DataGeneratorTable table) {
    List<DataGeneratorColumn> cols = writableColumns(table);
    if (cols.isEmpty()) {
      throw new IllegalArgumentException(
          "Table " + table.name() + " has no writable columns for INSERT.");
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
    sql.append(")");
    return sql.toString();
  }

  /**
   * Builds a {@code MERGE INTO t AS target USING (SELECT ? AS c1, ...) AS source ON (...) WHEN
   * MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...);} statement that is
   * used for both {@link Constants#MUTATION_INSERT} and {@link Constants#MUTATION_UPDATE}
   * operations. The statement binds a single set of parameters (all writable columns, in table
   * order) inside the {@code USING} clause. For tables that only consist of primary-key columns the
   * {@code WHEN MATCHED} clause is omitted, which is still a valid idempotent upsert.
   *
   * <p>No {@code HOLDLOCK} hint is used: the pipeline routes every primary key to a single stateful
   * key, so two writers never upsert the same row concurrently and the serializable range locks the
   * hint would take only add contention.
   */
  @VisibleForTesting
  static String buildUpsertSql(DataGeneratorTable table) {
    List<DataGeneratorColumn> cols = writableColumns(table);
    if (cols.isEmpty()) {
      throw new IllegalArgumentException(
          "Table " + table.name() + " has no writable columns for INSERT / UPDATE.");
    }
    List<String> pkNames = table.primaryKeys();
    if (pkNames == null || pkNames.isEmpty()) {
      throw new IllegalStateException(
          "Table " + table.name() + " has no primary key; cannot build MERGE.");
    }
    String tableName = quote(table.name());

    StringBuilder sql = new StringBuilder();
    sql.append("MERGE INTO ").append(tableName).append(" AS target USING (SELECT ");
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("? AS ").append(quote(cols.get(i).name()));
    }
    sql.append(") AS source ON (");
    for (int i = 0; i < pkNames.size(); i++) {
      if (i > 0) {
        sql.append(" AND ");
      }
      String pk = quote(pkNames.get(i));
      sql.append("target.").append(pk).append(" = source.").append(pk);
    }
    sql.append(")");

    List<DataGeneratorColumn> setCols = nonPrimaryKeyWritableColumns(table);
    if (!setCols.isEmpty()) {
      sql.append(" WHEN MATCHED THEN UPDATE SET ");
      for (int i = 0; i < setCols.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        String c = quote(setCols.get(i).name());
        sql.append("target.").append(c).append(" = source.").append(c);
      }
    }

    sql.append(" WHEN NOT MATCHED THEN INSERT (");
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
      sql.append("source.").append(quote(cols.get(i).name()));
    }
    sql.append(");");
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
      // INSERT and UPDATE both use the MERGE statement built by buildUpsertSql(...), which binds
      // all writable columns in table order once inside the USING (SELECT ...) clause.
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
      case UUID:
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
        return Types.BIT;
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

  /**
   * Quotes an identifier with square brackets so that reserved keywords and mixed-case names are
   * handled correctly. Any embedded closing bracket is doubled per SQL Server escaping rules.
   */
  @VisibleForTesting
  static String quote(String identifier) {
    return Constants.SQLSERVER_IDENTIFIER_QUOTE_START
        + identifier.replace(
            Constants.SQLSERVER_IDENTIFIER_QUOTE_END,
            Constants.SQLSERVER_IDENTIFIER_QUOTE_END + Constants.SQLSERVER_IDENTIFIER_QUOTE_END)
        + Constants.SQLSERVER_IDENTIFIER_QUOTE_END;
  }

  @Override
  public void close() {
    // The JdbcConnectionHelper is a per-worker singleton and is intentionally not closed here;
    // the transient reference is dropped along with the owning DoFn.
  }
}
