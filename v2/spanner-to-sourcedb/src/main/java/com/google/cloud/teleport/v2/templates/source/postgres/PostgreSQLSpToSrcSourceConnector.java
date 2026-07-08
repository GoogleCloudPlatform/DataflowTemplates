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
package com.google.cloud.teleport.v2.templates.source.postgres;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;

public class PostgreSQLSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private final IConnectionHelper connectionHelper;

  public PostgreSQLSpToSrcSourceConnector() {
    this.connectionHelper = new JdbcConnectionHelper();
  }

  @VisibleForTesting
  PostgreSQLSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new PostgreSQLDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    return "jdbc:postgresql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
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
              shards, null, maxConnections, "org.postgresql.Driver", null, "jdbc:postgresql://");
      connectionHelper.init(request);
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    // TODO checks for null and minimum size of 1
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("postgresql", shardFilePath);
    if (sourceConnectionConfig instanceof JdbcShardConfig) {
      return ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs();
    }
    throw new IllegalArgumentException(
        "Expected JdbcShardConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) throws Exception {
    // TODO- validate read only similar to mysql
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    try (Connection connection = createConnection(shards.get(0))) {
      return new PostgreSQLInformationSchemaScanner(
              connection, shards.get(0).getDbName(), shards.get(0).getNamespace())
          .scan();
    }
  }

  @VisibleForTesting
  Connection createConnection(Shard shard) throws Exception {
    // TODO this looks like a connection leak. Look to fix this.
    // Maybe create a connection directly without a pool
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(getConnectionUrl(shard));
    config.setUsername(shard.getUserName());
    config.setPassword(shard.getPassword());
    config.setDriverClassName("org.postgresql.Driver");
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
    return null;
  }
}
