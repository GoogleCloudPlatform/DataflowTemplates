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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
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
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MySqlSchemaFetcher implements SinkSchemaFetcher {

  @FunctionalInterface
  interface ConnectionProvider {
    Connection get() throws SQLException;
  }

  private String driverClassName;
  private String connectionUrl;
  private String username;
  private String password;

  private final MySqlTypeMapper typeMapper = new MySqlTypeMapper();

  private final ShardFileReader shardFileReader;
  private final ConnectionProvider connectionProvider;

  public MySqlSchemaFetcher() {
    this(new ShardFileReader(new SecretManagerAccessorImpl()), null);
  }

  @VisibleForTesting
  MySqlSchemaFetcher(ShardFileReader shardFileReader, ConnectionProvider connectionProvider) {
    this.shardFileReader = shardFileReader;
    this.connectionProvider = connectionProvider;
  }

  @VisibleForTesting
  protected MySqlInformationSchemaScanner createScanner(
      Connection connection, String databaseName) {
    return new MySqlInformationSchemaScanner(connection, databaseName);
  }

  @Override
  public void init(String sinkConfigPath) {
    if (sinkConfigPath == null || sinkConfigPath.isEmpty()) {
      throw new IllegalArgumentException(
          "MySQL sink requires a valid shard configuration file path.");
    }

    List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkConfigPath);
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided shard file: " + sinkConfigPath);
    }
    // Use the first shard to extract schema
    Shard firstShard = shards.get(0);
    this.username = firstShard.getUserName();
    this.password = firstShard.getPassword();

    String host = firstShard.getHost();
    String port = firstShard.getPort();
    String dbName = "";
    if (!firstShard.getDbName().isEmpty()) {
      dbName = firstShard.getDbName();
    }

    this.connectionUrl = String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);
    if (firstShard.getConnectionProperties() != null
        && !firstShard.getConnectionProperties().isEmpty()) {
      this.connectionUrl += "?" + firstShard.getConnectionProperties();
    }
    this.driverClassName = "com.mysql.cj.jdbc.Driver";
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    try {
      if (driverClassName != null && !driverClassName.isEmpty()) {
        Class.forName(driverClassName);
      }
      try (Connection connection = getConnection()) {
        return scanMySqlSchema(connection);
      }
    } catch (ClassNotFoundException | SQLException e) {
      throw new IOException("Failed to fetch MySQL schema", e);
    }
  }

  private DataGeneratorSchema scanMySqlSchema(Connection connection) throws SQLException {
    String databaseName = connection.getCatalog();
    MySqlInformationSchemaScanner scanner = createScanner(connection, databaseName);
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

  private DataGeneratorColumn mapSourceColumn(SourceColumn column, List<String> primaryKeys) {
    return DataGeneratorColumn.builder()
        .name(column.name())
        .logicalType(typeMapper.getLogicalType(column.type(), null, column.size()))
        .isNullable(column.isNullable())
        .isGenerated(column.isGenerated())
        .isPrimaryKey(primaryKeys.contains(column.name()))
        .size(column.size())
        .precision(column.precision())
        .scale(column.scale())
        .build();
  }

  protected Connection getConnection() throws SQLException {
    if (this.connectionProvider != null) {
      return this.connectionProvider.get();
    }
    return DriverManager.getConnection(connectionUrl, username, password);
  }
}
