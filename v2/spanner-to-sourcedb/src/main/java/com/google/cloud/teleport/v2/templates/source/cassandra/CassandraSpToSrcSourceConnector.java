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
package com.google.cloud.teleport.v2.templates.source.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.CassandraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConfigParser;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraConfigFileReader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ISecretManagerAccessor;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;

public class CassandraSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private final IConnectionHelper connectionHelper;

  public CassandraSpToSrcSourceConnector() {
    this.connectionHelper = new CassandraConnectionHelper();
  }

  @VisibleForTesting
  CassandraSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new CassandraDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    CassandraShard cassandraShard = (CassandraShard) shard;
    return cassandraShard.getHost()
        + ":"
        + cassandraShard.getPort()
        + "/"
        + cassandraShard.getUserName()
        + "/"
        + cassandraShard.getKeySpaceName();
  }

  @Override
  public IDao getDao(Shard shard) {
    return new CassandraDao(getConnectionUrl(shard), shard.getUserName(), getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(
              shards,
              null,
              maxConnections,
              "com.datastax.oss.driver.api.core.CqlSession",
              null,
              null);
      connectionHelper.init(request);
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    ISecretManagerAccessor secretManagerAccessor = new SecretManagerAccessorImpl();
    SourceConfigParser sourceConfigParser = new SourceConfigParser(secretManagerAccessor);
    SourceConnectionConfig sourceConnectionConfig =
        sourceConfigParser.parseConfiguration("cassandra", shardFilePath);
    if (sourceConnectionConfig instanceof CassandraConnectionConfig) {
      CassandraConfigFileReader cassandraConfigFileReader = new CassandraConfigFileReader();
      return cassandraConfigFileReader.getCassandraShard(
          ((CassandraConnectionConfig) sourceConnectionConfig).getOptionsMap());
    }
    throw new IllegalArgumentException(
        "Expected CassandraConnectionConfig but got: " + sourceConnectionConfig.getClass());
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) throws Exception {
    if (shards.size() != 1) {
      throw new IllegalArgumentException("Cassandra migration must have exactly 1 shard.");
    }
    if (!(shards.get(0) instanceof CassandraShard)) {
      throw new IllegalArgumentException(
          "Expected CassandraShard but got: " + shards.get(0).getClass());
    }
    // TODO Check for valid connection as well
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    CassandraShard cassandraShard = (CassandraShard) shards.get(0);
    try (CqlSession session = createCqlSession(cassandraShard)) {
      return new CassandraInformationSchemaScanner(session, cassandraShard.getKeySpaceName())
          .scan();
    }
  }

  @VisibleForTesting
  CqlSession createCqlSession(CassandraShard cassandraShard) {
    CqlSessionBuilder builder = CqlSession.builder();
    DriverConfigLoader configLoader =
        CassandraDriverConfigLoader.fromOptionsMap(cassandraShard.getOptionsMap());
    builder.withConfigLoader(configLoader);
    return builder.build();
  }

  @Override
  public boolean supportsSharding() {
    return false;
  }

  @Override
  public boolean shouldUpdateReadValuesToSpannerRecord() {
    return false;
  }

  @Override
  public org.apache.beam.sdk.values.TupleTag<String> classifyException(Throwable cause) {
    if (cause instanceof com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException) {
      return com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
    }
    return null;
  }
}
