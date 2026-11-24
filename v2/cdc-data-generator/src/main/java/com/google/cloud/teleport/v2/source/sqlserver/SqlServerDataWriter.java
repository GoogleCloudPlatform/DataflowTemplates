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
package com.google.cloud.teleport.v2.source.sqlserver;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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
 * argument.
 *
 * <p>INSERT runs as a batched plain {@code INSERT} (bulk copy stays off). If the batch hits a
 * duplicate key (SQL Server error 2627 or 2601, e.g. a retried bundle or a reused key), the whole
 * batch is re-run as the idempotent {@code MERGE ... WITH (HOLDLOCK)} upsert. UPDATE always uses
 * that MERGE. DELETE uses {@link DataGeneratorTable#primaryKeys} to build the {@code WHERE} clause.
 * Server-generated (computed, ROWVERSION, non-PK IDENTITY) and skipped columns are never written.
 * If an IDENTITY column is written (IDENTITY primary key), the batch runs between {@code SET
 * IDENTITY_INSERT ... ON} and {@code OFF}. TIME columns (found via {@code sys.columns}) are bound
 * as {@code HH:mm:ss.SSS} strings.
 */
public class SqlServerDataWriter implements DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerDataWriter.class);

  @VisibleForTesting
  static final String COLUMN_INFO_QUERY =
      "SELECT c.name, TYPE_NAME(c.system_type_id) AS type_name, c.is_identity"
          + " FROM sys.columns c WHERE c.object_id = OBJECT_ID(?)";

  // TIME values are bound as strings: a DATETIME2 parameter cannot be compared with a TIME column
  // ("The data types time and datetime2 are incompatible in the equal to operator").
  private static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

  /** Target-table facts read from {@code sys.columns}, cached per connection key and table. */
  @VisibleForTesting
  static final class TableInfo {
    final String identityColumn;
    final Set<String> timeColumns;

    TableInfo(String identityColumn, Set<String> timeColumns) {
      this.identityColumn = identityColumn;
      this.timeColumns = timeColumns;
    }
  }

  private static final Object POOL_INIT_LOCK = new Object();

  // SQL Server error numbers for a duplicate key: 2627 (PRIMARY KEY / UNIQUE constraint) and 2601
  // (unique index).
  private static final int ERROR_DUPLICATE_KEY_CONSTRAINT = 2627;
  private static final int ERROR_DUPLICATE_KEY_INDEX = 2601;

  private final Counter insertDuplicateKeyFallbacks =
      Metrics.counter(SqlServerDataWriter.class, "sqlServerInsertDuplicateKeyFallbacks");

  enum MutationType {
    INSERT,
    UPDATE,
    DELETE
  }

  private final SqlServerSinkConfig sqlServerSinkConfig;

  private transient IConnectionHelper<Connection> connectionHelper;
  private transient Map<String, Shard> shardsByLogicalId;
  // connection key + qualified table name -> IDENTITY / TIME column facts.
  private transient Map<String, TableInfo> tableInfoCache;

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
    executeWrite(rows, table, shardId, maxShardConnections, MutationType.INSERT);
  }

  @Override
  public void update(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(rows, table, shardId, maxShardConnections, MutationType.UPDATE);
  }

  @Override
  public void delete(
      List<Row> rows, DataGeneratorTable table, String shardId, int maxShardConnections) {
    if (table == null) {
      throw new IllegalArgumentException("DataGeneratorTable must not be null");
    }
    executeWrite(rows, table, shardId, maxShardConnections, MutationType.DELETE);
  }

  private void executeWrite(
      List<Row> rows,
      DataGeneratorTable table,
      String shardId,
      int maxShardConnections,
      MutationType operation) {
    if (rows == null || rows.isEmpty()) {
      return;
    }
    ensureInitialized(maxShardConnections);

    Shard shard = resolveShard(shardId);
    String namespace = shard.getNamespace();
    String sql;
    switch (operation) {
      case INSERT:
        sql = buildInsertSql(table, namespace);
        break;
      case UPDATE:
        sql = buildUpsertSql(table, namespace);
        break;
      case DELETE:
      default:
        sql = buildDeleteSql(table, namespace);
        break;
    }
    String connectionKey = buildConnectionKey(shard);

    try (Connection connection = connectionHelper.getConnection(connectionKey)) {
      if (connection == null) {
        throw new RuntimeException(
            "No SQL Server connection available for shard '"
                + shard.getLogicalShardId()
                + "' (key="
                + connectionKey
                + ")");
      }
      List<DataGeneratorColumn> boundColumns = resolveBoundColumns(table, operation);
      String qualifiedTable = qualifiedTableName(table, namespace);
      TableInfo tableInfo = getTableInfo(connection, connectionKey, qualifiedTable);
      boolean identityInsert =
          operation != MutationType.DELETE && writesIdentityColumn(tableInfo, boundColumns);
      if (identityInsert) {
        setIdentityInsert(connection, qualifiedTable, true);
      }
      try {
        try {
          executeBatch(connection, sql, rows, boundColumns, tableInfo.timeColumns);
        } catch (SQLException e) {
          if (operation != MutationType.INSERT || !isDuplicateKey(e)) {
            throw e;
          }
          // Some rows of the batch may already exist (or were written before the failing row).
          // The MERGE binds the same columns in the same order and is idempotent, so re-run the
          // whole batch with it.
          LOG.info(
              "Duplicate key on INSERT of {} rows into {} on SQL Server shard {}; re-running the"
                  + " batch as MERGE.",
              rows.size(),
              qualifiedTable,
              shardId);
          insertDuplicateKeyFallbacks.inc();
          executeBatch(
              connection,
              buildUpsertSql(table, namespace),
              rows,
              boundColumns,
              tableInfo.timeColumns);
        }
      } finally {
        if (identityInsert) {
          setIdentityInsert(connection, qualifiedTable, false);
        }
      }
    } catch (ConnectionException | SQLException e) {
      LOG.error(
          "Failed to execute write (operation: {}) for {} rows on SQL Server shard {}",
          operation,
          rows.size(),
          shardId,
          e);
      throw new RuntimeException("Failed to write to SQL Server shard " + shardId, e);
    }
  }

  private void executeBatch(
      Connection connection,
      String sql,
      List<Row> rows,
      List<DataGeneratorColumn> boundColumns,
      Set<String> timeColumns)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (Row row : rows) {
        setStatementParameters(statement, row, boundColumns, timeColumns);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  /**
   * Returns true if SQL Server error 2627 or 2601 (duplicate key) appears anywhere in the exception
   * chain, following both {@link Throwable#getCause()} and {@link SQLException#getNextException()}
   * (a {@link java.sql.BatchUpdateException} may carry the server error in either). Other integrity
   * errors that share SQLState 23000 (e.g. 547 FOREIGN KEY, 515 NULL) do not match.
   */
  @VisibleForTesting
  static boolean isDuplicateKey(Throwable e) {
    Set<Throwable> seen = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
    pending.add(e);
    while (!pending.isEmpty()) {
      Throwable t = pending.poll();
      if (t == null || !seen.add(t)) {
        continue;
      }
      if (t instanceof SQLException) {
        SQLException sqlException = (SQLException) t;
        int code = sqlException.getErrorCode();
        if (code == ERROR_DUPLICATE_KEY_CONSTRAINT || code == ERROR_DUPLICATE_KEY_INDEX) {
          return true;
        }
        if (sqlException.getNextException() != null) {
          pending.add(sqlException.getNextException());
        }
      }
      if (t.getCause() != null) {
        pending.add(t.getCause());
      }
    }
    return false;
  }

  /** Reads (once per connection key and table) the IDENTITY column and TIME columns. */
  @VisibleForTesting
  TableInfo getTableInfo(Connection connection, String connectionKey, String qualifiedTable)
      throws SQLException {
    if (tableInfoCache == null) {
      tableInfoCache = new ConcurrentHashMap<>();
    }
    String cacheKey = connectionKey + "|" + qualifiedTable;
    TableInfo info = tableInfoCache.get(cacheKey);
    if (info != null) {
      return info;
    }
    String identityColumn = "";
    Set<String> timeColumns = new HashSet<>();
    try (PreparedStatement statement = connection.prepareStatement(COLUMN_INFO_QUERY)) {
      statement.setString(1, qualifiedTable);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs != null && rs.next()) {
          String name = rs.getString("name");
          if (name == null) {
            continue;
          }
          if (rs.getBoolean("is_identity")) {
            identityColumn = name;
          }
          if ("time".equalsIgnoreCase(rs.getString("type_name"))) {
            timeColumns.add(name.toLowerCase(Locale.ROOT));
          }
        }
      }
    }
    info = new TableInfo(identityColumn, Collections.unmodifiableSet(timeColumns));
    tableInfoCache.put(cacheKey, info);
    return info;
  }

  /** Returns true if the table's IDENTITY column is one of the bound columns. */
  @VisibleForTesting
  static boolean writesIdentityColumn(TableInfo info, List<DataGeneratorColumn> boundColumns) {
    if (info.identityColumn.isEmpty()) {
      return false;
    }
    for (DataGeneratorColumn col : boundColumns) {
      if (col.name().equalsIgnoreCase(info.identityColumn)) {
        return true;
      }
    }
    return false;
  }

  private static void setIdentityInsert(Connection connection, String qualifiedTable, boolean on)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("SET IDENTITY_INSERT " + qualifiedTable + (on ? " ON" : " OFF"));
    }
  }

  @VisibleForTesting
  synchronized void ensureInitialized(int maxShardConnections) {
    if (connectionHelper == null) {
      connectionHelper = new JdbcConnectionHelper();
    }
    if (shardsByLogicalId == null) {
      List<Shard> shards =
          sqlServerSinkConfig != null && sqlServerSinkConfig.getShards() != null
              ? sqlServerSinkConfig.getShards()
              : Collections.emptyList();
      if (shards.isEmpty()) {
        throw new RuntimeException(
            "No shards found in the provided SQL Server sink configuration.");
      }
      Map<String, Shard> byId = new LinkedHashMap<>();
      for (Shard shard : shards) {
        if (byId.put(shard.getLogicalShardId(), shard) != null) {
          throw new IllegalArgumentException(
              "Duplicate logicalShardId '"
                  + shard.getLogicalShardId()
                  + "' found in SQL Server sink configuration.");
        }
      }
      this.shardsByLogicalId = Collections.unmodifiableMap(byId);
    }
    // JdbcConnectionHelper keeps its pools in a static map but synchronizes per instance, so many
    // writers starting together on one worker would each build a pool (one pool per DoFn instance
    // seen in a live run: 100+ pools opening connections at once -> connect timeouts). Serialize
    // the check-and-init across all writers in the JVM.
    synchronized (POOL_INIT_LOCK) {
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
  }

  @VisibleForTesting
  Shard resolveShard(String shardId) {
    if (shardsByLogicalId == null) {
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
   * Builds {@code jdbc:sqlserver://host:port;databaseName=db}. Unless the shard's connection
   * properties set them, {@code trustServerCertificate=true} and {@code encrypt=false} are added
   * (same defaults as the spanner-to-sourcedb SQL Server connector). Other connection properties
   * are applied as data source properties by {@link JdbcConnectionHelper}.
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
    String props =
        shard.getConnectionProperties() == null
            ? ""
            : shard.getConnectionProperties().toLowerCase(Locale.ROOT);
    if (!props.contains("trustservercertificate")) {
      url.append(";trustServerCertificate=true");
    }
    if (!props.contains("encrypt")) {
      url.append(";encrypt=false");
    }
    return url.toString();
  }

  @VisibleForTesting
  static String buildConnectionKey(Shard shard) {
    return buildConnectionUrl(shard) + "/" + shard.getUserName();
  }

  @VisibleForTesting
  static String buildInsertSql(DataGeneratorTable table) {
    return buildInsertSql(table, null);
  }

  /**
   * Builds {@code INSERT INTO [s].[t] ([c1], ...) VALUES (?, ...)}. It binds the same writable
   * columns, in the same order, as {@link #buildUpsertSql(DataGeneratorTable, String)}, so a failed
   * INSERT batch can be re-run as MERGE with the same parameters.
   */
  @VisibleForTesting
  static String buildInsertSql(DataGeneratorTable table, String namespace) {
    List<DataGeneratorColumn> cols = writableColumns(table);
    if (cols.isEmpty()) {
      throw new IllegalArgumentException(
          "Table " + table.name() + " has no writable columns for INSERT.");
    }
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(qualifiedTableName(table, namespace)).append(" (");
    appendColumnList(sql, cols, "");
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

  @VisibleForTesting
  static String buildUpsertSql(DataGeneratorTable table) {
    return buildUpsertSql(table, null);
  }

  /**
   * Builds {@code MERGE INTO [s].[t] WITH (HOLDLOCK) AS t USING (VALUES (?, ...)) AS s ([c1], ...)
   * ON (t.[pk] = s.[pk]) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES
   * (...);}. It binds all writable columns once, in table order. For tables that only consist of
   * primary-key columns the {@code WHEN MATCHED} clause is omitted.
   */
  @VisibleForTesting
  static String buildUpsertSql(DataGeneratorTable table, String namespace) {
    List<DataGeneratorColumn> cols = writableColumns(table);
    if (cols.isEmpty()) {
      throw new IllegalArgumentException(
          "Table " + table.name() + " has no writable columns for INSERT / UPDATE.");
    }
    List<String> pkNames = table.primaryKeys();
    if (pkNames == null || pkNames.isEmpty()) {
      throw new IllegalStateException(
          "Table " + table.name() + " has no primary key; cannot build MERGE upsert.");
    }
    for (String pkName : pkNames) {
      if (cols.stream().noneMatch(c -> c.name().equals(pkName))) {
        throw new IllegalStateException(
            "Primary key column '"
                + pkName
                + "' not found in writable columns on table "
                + table.name());
      }
    }

    StringBuilder sql = new StringBuilder("MERGE INTO ");
    sql.append(qualifiedTableName(table, namespace))
        .append(" WITH (HOLDLOCK) AS t USING (VALUES (");
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("?");
    }
    sql.append(")) AS s (");
    appendColumnList(sql, cols, "");
    sql.append(") ON (");
    for (int i = 0; i < pkNames.size(); i++) {
      if (i > 0) {
        sql.append(" AND ");
      }
      String pk = quote(pkNames.get(i));
      sql.append("t.").append(pk).append(" = s.").append(pk);
    }
    sql.append(") ");

    List<DataGeneratorColumn> setCols = nonPrimaryKeyColumns(cols, pkNames);
    if (!setCols.isEmpty()) {
      sql.append("WHEN MATCHED THEN UPDATE SET ");
      for (int i = 0; i < setCols.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        String c = quote(setCols.get(i).name());
        sql.append("t.").append(c).append(" = s.").append(c);
      }
      sql.append(" ");
    }

    sql.append("WHEN NOT MATCHED THEN INSERT (");
    appendColumnList(sql, cols, "");
    sql.append(") VALUES (");
    appendColumnList(sql, cols, "s.");
    sql.append(");");
    return sql.toString();
  }

  @VisibleForTesting
  static String buildDeleteSql(DataGeneratorTable table) {
    return buildDeleteSql(table, null);
  }

  @VisibleForTesting
  static String buildDeleteSql(DataGeneratorTable table, String namespace) {
    List<String> pkNames = table.primaryKeys();
    if (pkNames == null || pkNames.isEmpty()) {
      throw new IllegalStateException(
          "Table " + table.name() + " has no primary key; cannot build DELETE.");
    }
    StringBuilder sql = new StringBuilder("DELETE FROM ");
    sql.append(qualifiedTableName(table, namespace)).append(" WHERE ");
    for (int i = 0; i < pkNames.size(); i++) {
      if (i > 0) {
        sql.append(" AND ");
      }
      sql.append(quote(pkNames.get(i))).append(" = ?");
    }
    return sql.toString();
  }

  private static void appendColumnList(
      StringBuilder sql, List<DataGeneratorColumn> cols, String prefix) {
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(prefix).append(quote(cols.get(i).name()));
    }
  }

  @VisibleForTesting
  static String qualifiedTableName(DataGeneratorTable table, String namespace) {
    if (namespace != null && !namespace.isBlank()) {
      return quote(unquoteIdentifier(namespace.trim())) + "." + quote(table.name());
    }
    return quote(table.name());
  }

  /** Strips one pair of surrounding square brackets or double quotes, if present. */
  static String unquoteIdentifier(String identifier) {
    if (identifier.length() >= 2
        && ((identifier.startsWith("[") && identifier.endsWith("]"))
            || (identifier.startsWith("\"") && identifier.endsWith("\"")))) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier;
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

  private static List<DataGeneratorColumn> nonPrimaryKeyColumns(
      List<DataGeneratorColumn> cols, List<String> pkNames) {
    ImmutableList.Builder<DataGeneratorColumn> builder = ImmutableList.builder();
    for (DataGeneratorColumn col : cols) {
      if (!pkNames.contains(col.name())) {
        builder.add(col);
      }
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

  private static List<DataGeneratorColumn> resolveBoundColumns(
      DataGeneratorTable table, MutationType operation) {
    if (operation == MutationType.DELETE) {
      ImmutableList.Builder<DataGeneratorColumn> builder = ImmutableList.builder();
      for (String pkName : table.primaryKeys()) {
        builder.add(findColumn(table, pkName));
      }
      return builder.build();
    }
    // INSERT (buildInsertSql) and UPDATE / INSERT fallback (buildUpsertSql) bind all writable
    // columns once, in table order.
    return writableColumns(table);
  }

  @VisibleForTesting
  void setStatementParameters(
      PreparedStatement statement, Row row, DataGeneratorTable table, MutationType operation)
      throws SQLException {
    setStatementParameters(
        statement, row, resolveBoundColumns(table, operation), Collections.emptySet());
  }

  private void setStatementParameters(
      PreparedStatement statement,
      Row row,
      List<DataGeneratorColumn> boundColumns,
      Set<String> timeColumns)
      throws SQLException {
    int idx = 1;
    for (DataGeneratorColumn col : boundColumns) {
      Object value = fetchFromRow(row, col.name());
      if (value != null
          && col.logicalType() == LogicalType.TIMESTAMP
          && timeColumns.contains(col.name().toLowerCase(Locale.ROOT))) {
        statement.setString(idx++, formatTime((ReadableInstant) value));
      } else {
        setParameter(statement, idx++, col, value);
      }
    }
  }

  /** Formats the time-of-day of {@code instant} (JVM zone, as for TIMESTAMP binds). */
  @VisibleForTesting
  static String formatTime(ReadableInstant instant) {
    return java.time.Instant.ofEpochMilli(instant.getMillis())
        .atZone(ZoneId.systemDefault())
        .toLocalTime()
        .format(TIME_FORMATTER);
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

  @VisibleForTesting
  static void setParameter(
      PreparedStatement statement, int index, DataGeneratorColumn column, Object value)
      throws SQLException {
    LogicalType type = column.logicalType();
    if (value == null) {
      // SQL Server has no BOOLEAN type; BIT is the matching JDBC type for NULL binds.
      int jdbcType = type == LogicalType.BOOLEAN ? Types.BIT : DataGeneratorUtils.jdbcTypeFor(type);
      statement.setNull(index, jdbcType);
      return;
    }
    switch (type) {
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
        statement.setDate(index, new java.sql.Date(((ReadableInstant) value).getMillis()));
        break;
      case TIMESTAMP:
        // Sent as DATETIME2; SQL Server converts it implicitly to DATETIME, SMALLDATETIME,
        // DATETIMEOFFSET (offset +00:00) and TIME (date part dropped).
        statement.setTimestamp(
            index, new java.sql.Timestamp(((ReadableInstant) value).getMillis()));
        break;
      case STRING:
      case JSON:
      case UUID:
      default:
        // Sent as NVARCHAR (driver default sendStringParametersAsUnicode=true), so N-types keep
        // Unicode; SQL Server converts it implicitly to UNIQUEIDENTIFIER, XML, GEOMETRY, etc.
        statement.setString(index, value.toString());
        break;
    }
  }

  @VisibleForTesting
  static String quote(String identifier) {
    // Bracket-quote identifiers so that reserved keywords and special characters are handled.
    // Any embedded closing bracket is doubled per SQL Server escaping rules.
    return Constants.SQLSERVER_IDENTIFIER_QUOTE_OPEN
        + identifier.replace(
            Constants.SQLSERVER_IDENTIFIER_QUOTE_CLOSE,
            Constants.SQLSERVER_IDENTIFIER_QUOTE_CLOSE + Constants.SQLSERVER_IDENTIFIER_QUOTE_CLOSE)
        + Constants.SQLSERVER_IDENTIFIER_QUOTE_CLOSE;
  }

  @Override
  public void close() {
    // The JdbcConnectionHelper is a per-worker singleton and is intentionally not closed here;
    // the transient reference is dropped along with the owning DoFn.
  }
}
