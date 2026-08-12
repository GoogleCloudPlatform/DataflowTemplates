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
package com.google.cloud.teleport.v2.templates.source.oracle;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.OracleInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;

public class OracleSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private final IConnectionHelper connectionHelper;

  public OracleSpToSrcSourceConnector() {
    this.connectionHelper = new JdbcConnectionHelper();
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new OracleDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return this.connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    return "jdbc:oracle:thin:@" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
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
              shards, null, maxConnections, "oracle.jdbc.OracleDriver", null, "jdbc:oracle:thin:@");
      connectionHelper.init(request);
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    // Passing "oracle" to parseConfiguration assumes it supports it; fallback to just string
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("oracle", shardFilePath);
    if (sourceConnectionConfig instanceof JdbcShardConfig) {
      return ((JdbcShardConfig) sourceConnectionConfig).getShardConfigs();
    }
    throw new IllegalArgumentException(
        "Expected JdbcShardConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) {}

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    try (Connection connection = createConnection(shards.get(0))) {
      return new OracleInformationSchemaScanner(connection).scan();
    }
  }

  Connection createConnection(Shard shard) throws Exception {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(getConnectionUrl(shard));
    config.setUsername(shard.getUserName());
    config.setPassword(shard.getPassword());
    config.setDriverClassName("oracle.jdbc.OracleDriver");
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
    return null;
  }
}
