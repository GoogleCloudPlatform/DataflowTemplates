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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SQLServerInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceIndex;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.model.SqlServerSinkConfig;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SinkSchemaFetcher} implementation for SQL Server.
 *
 * <p>Connects to the first configured shard and converts the {@link SourceSchema} produced by
 * {@link SQLServerInformationSchemaScanner} into a {@link DataGeneratorSchema}.
 */
public class SqlServerSchemaFetcher implements SinkSchemaFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSchemaFetcher.class);

  @FunctionalInterface
  interface ConnectionProvider {
    Connection get() throws SQLException;
  }

  /**
   * Maximum declared length of a non-LOB string / binary column in SQL Server. Anything above this
   * (e.g. {@code nvarchar(max)}, {@code text}, {@code image}) is treated as unbounded so that the
   * generator falls back to its default length instead of synthesising gigantic values.
   */
  @VisibleForTesting static final long MAX_NON_LOB_SIZE = 8000L;

  private String connectionUrl;
  private String username;
  private String password;

  private final SqlServerTypeMapper typeMapper = new SqlServerTypeMapper();

  private final ConnectionProvider connectionProvider;

  public SqlServerSchemaFetcher() {
    this(null);
  }

  @VisibleForTesting
  SqlServerSchemaFetcher(ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @VisibleForTesting
  protected SQLServerInformationSchemaScanner createScanner(
      Connection connection, String databaseName) {
    return new SQLServerInformationSchemaScanner(connection, databaseName);
  }

  @Override
  public void init(SinkConfig sinkConfig) {
    if (sinkConfig == null) {
      throw new IllegalArgumentException("SinkConfig cannot be null");
    }
    if (!(sinkConfig instanceof SqlServerSinkConfig)) {
      throw new IllegalArgumentException(
          "Expected SqlServerSinkConfig but got " + sinkConfig.getClass().getName());
    }
    SqlServerSinkConfig sqlServerSinkConfig = (SqlServerSinkConfig) sinkConfig;
    List<Shard> shards = sqlServerSinkConfig.getShards();
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided SQL Server sink configuration.");
    }
    // Use the first shard to extract schema
    Shard firstShard = shards.get(0);
    this.username = firstShard.getUserName();
    this.password = firstShard.getPassword();
    this.connectionUrl = SqlServerDataWriter.buildConnectionUrl(firstShard);
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    try (Connection connection = getConnection()) {
      return scanSqlServerSchema(connection);
    } catch (SQLException e) {
      throw new IOException("Failed to fetch SQL Server schema", e);
    }
  }

  private DataGeneratorSchema scanSqlServerSchema(Connection connection) throws SQLException {
    String databaseName = connection.getCatalog();
    SQLServerInformationSchemaScanner scanner = createScanner(connection, databaseName);
    SourceSchema sourceSchema = scanner.scan();

    Map<String, DataGeneratorTable> tables =
        sourceSchema.tables().values().stream()
            .collect(Collectors.toMap(SourceTable::name, table -> mapSourceTable(table)));

    return DataGeneratorSchema.builder().tables(ImmutableMap.copyOf(tables)).build();
  }

  private DataGeneratorTable mapSourceTable(SourceTable table) {
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (SourceColumn column : table.columns()) {
      columnsBuilder.add(mapSourceColumn(column, table.primaryKeyColumns()));
    }

    ImmutableList.Builder<DataGeneratorForeignKey> foreignKeysBuilder = ImmutableList.builder();
    if (table.foreignKeys() != null) {
      for (SourceForeignKey fk : table.foreignKeys()) {
        foreignKeysBuilder.add(
            DataGeneratorForeignKey.builder()
                .name(fk.name())
                .referencedTable(fk.referencedTable())
                .keyColumns(fk.keyColumns())
                .referencedColumns(fk.referencedColumns())
                .build());
      }
    }

    ImmutableList.Builder<DataGeneratorUniqueKey> uniqueKeysBuilder = ImmutableList.builder();
    if (table.indexes() != null) {
      for (SourceIndex index : table.indexes()) {
        if (index.isUnique() && !index.isPrimary()) {
          uniqueKeysBuilder.add(
              DataGeneratorUniqueKey.builder().name(index.name()).columns(index.columns()).build());
        }
      }
    }

    return DataGeneratorTable.builder()
        .name(table.name())
        .columns(columnsBuilder.build())
        .primaryKeys(table.primaryKeyColumns())
        .foreignKeys(foreignKeysBuilder.build())
        .uniqueKeys(uniqueKeysBuilder.build())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0) // Default value
        .deleteQps(0) // Default value
        .recordsPerTick(1.0) // Default value
        .build();
  }

  @VisibleForTesting
  DataGeneratorColumn mapSourceColumn(SourceColumn column, List<String> primaryKeys) {
    LogicalType logicalType = typeMapper.getLogicalType(column.type(), null, column.size());
    if (isIdentity(column.type())) {
      // SQL Server rejects explicit values for IDENTITY columns unless IDENTITY_INSERT is enabled,
      // which the generator does not do. Writes to this table will fail and be routed to the DLQ.
      LOG.warn(
          "Column '{}' is an IDENTITY column (type '{}'). IDENTITY columns are not supported by"
              + " the SQL Server sink; inserts into this table will be rejected.",
          column.name(),
          column.type());
    }
    // rowversion / timestamp columns are always populated by the server and cannot be written.
    boolean isGenerated = column.isGenerated() || isRowVersion(column.type());
    return DataGeneratorColumn.builder()
        .name(column.name())
        .logicalType(logicalType)
        .isNullable(column.isNullable())
        .isGenerated(isGenerated)
        .isPrimaryKey(primaryKeys.contains(column.name()))
        .size(effectiveSize(logicalType, column.size()))
        .precision(effectivePrecision(column.type(), column.precision()))
        .scale(column.scale())
        .build();
  }

  /** The SQL Server JDBC driver reports identity columns with an {@code identity} type suffix. */
  @VisibleForTesting
  static boolean isIdentity(String typeName) {
    return typeName != null && typeName.toUpperCase(Locale.ROOT).trim().endsWith(" IDENTITY");
  }

  private static boolean isRowVersion(String typeName) {
    String upper = baseTypeName(typeName);
    return upper.startsWith("TIMESTAMP") || upper.startsWith("ROWVERSION");
  }

  /**
   * Returns the precision to drive data generation for a column. For most types the metadata
   * precision is used as-is, but the money types have a hard value range that is narrower than
   * their reported decimal precision:
   *
   * <ul>
   *   <li>{@code smallmoney} reports (10,4) but tops out at 214,748.3647 → generate up to
   *       99,999.9999.
   *   <li>{@code money} reports (19,4) but tops out at 922,337,203,685,477.5807 → generate up to
   *       99,999,999,999,999.9999.
   * </ul>
   */
  @VisibleForTesting
  static Integer effectivePrecision(String typeName, Integer precision) {
    switch (baseTypeName(typeName)) {
      case "SMALLMONEY":
        return 9;
      case "MONEY":
        return 18;
      default:
        return precision;
    }
  }

  /** Upper-cases a JDBC type name and strips any {@code identity} suffix, e.g. "int identity". */
  private static String baseTypeName(String typeName) {
    if (typeName == null) {
      return "";
    }
    String upper = typeName.toUpperCase(Locale.ROOT).trim();
    int space = upper.indexOf(' ');
    return space > 0 ? upper.substring(0, space) : upper;
  }

  /**
   * Returns the size to drive data generation for a column. LOB / {@code (max)} types report a 1-2
   * GiB {@code COLUMN_SIZE} which must not be used as a generation length.
   */
  @VisibleForTesting
  static Long effectiveSize(LogicalType logicalType, Long size) {
    if (size == null) {
      return null;
    }
    if ((logicalType == LogicalType.STRING || logicalType == LogicalType.BYTES)
        && size > MAX_NON_LOB_SIZE) {
      return null;
    }
    return size;
  }

  protected Connection getConnection() throws SQLException {
    if (this.connectionProvider != null) {
      return this.connectionProvider.get();
    }
    try {
      // Ensure the driver is registered with DriverManager before opening the connection.
      Class.forName(Constants.SQLSERVER_JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      throw new SQLException(
          "SQL Server JDBC driver not found: " + Constants.SQLSERVER_JDBC_DRIVER, e);
    }
    return DriverManager.getConnection(connectionUrl, username, password);
  }
}
