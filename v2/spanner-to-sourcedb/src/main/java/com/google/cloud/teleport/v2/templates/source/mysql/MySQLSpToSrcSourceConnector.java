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
package com.google.cloud.teleport.v2.templates.source.mysql;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLSpToSrcSourceConnector.class);

  private final IConnectionHelper connectionHelper;

  public MySQLSpToSrcSourceConnector() {
    this.connectionHelper = new JdbcConnectionHelper();
  }

  @VisibleForTesting
  MySQLSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new MySQLDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    return "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
  }

  @Override
  public IDao getDao(Shard shard) {
    return new JdbcDao(getConnectionUrl(shard), shard.getUserName(), getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              shards,
              null,
              maxConnections,
              "com.mysql.cj.jdbc.Driver",
              "SET SESSION net_read_timeout=1200", // To avoid timeouts at the network layer
              "jdbc:mysql://");
      connectionHelper.init(request);
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("mysql", shardFilePath);
    // TODO checks for null and minimum size of 1
    if (sourceConnectionConfig instanceof JdbcShardConfig) {
      return ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs();
    }
    throw new IllegalArgumentException(
        "Expected JdbcShardConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) throws Exception {
    for (Shard shard : shards) {
      try (Connection conn = createConnection(shard)) {
        if (conn != null) {
          try (Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT @@read_only")) {
            if (rs != null && rs.next() && rs.getInt(1) == 1) {
              throw new RuntimeException(
                  "MySQL destination is in read-only mode for shard: " + shard.getLogicalShardId());
            }
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Error checking MySQL read-only status for shard {}: {}",
            shard.getLogicalShardId(),
            e.getMessage());
        throw new RuntimeException("Error checking MySQL read-only status", e);
      }
    }
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    try (Connection connection = createConnection(shards.get(0))) {
      return new MySqlInformationSchemaScanner(connection, shards.get(0).getDbName()).scan();
    }
  }

  @VisibleForTesting
  Connection createConnection(Shard shard) throws Exception {
    // TODO this looks like a connection leak. Look to fix this.
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(getConnectionUrl(shard));
    config.setUsername(shard.getUserName());
    config.setPassword(shard.getPassword());
    config.setDriverClassName("com.mysql.cj.jdbc.Driver");
    HikariDataSource ds = new HikariDataSource(config);
    return ds.getConnection();
  }

  @Override
  public boolean supportsSharding() {
    return true;
  }

  @Override
  public boolean shouldUpdateReadValuesToSpannerRecord() {
    return true;
  }

  @Override
  public org.apache.beam.sdk.values.TupleTag<String> classifyException(Throwable cause) {
    if (cause instanceof java.sql.SQLSyntaxErrorException
        || cause instanceof java.sql.SQLDataException) {
      return com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
    }
    if (cause instanceof java.sql.SQLNonTransientConnectionException e) {
      if (e.getErrorCode() != 1053 && e.getErrorCode() != 1159 && e.getErrorCode() != 1161) {
        // https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
        // error codes 1053,1161 and 1159 can be retried
        return com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
      }
    }
    return null;
  }
}
