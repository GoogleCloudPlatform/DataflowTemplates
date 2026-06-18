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
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.connection.CassandraConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.CassandraDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISourceConnector;
import java.util.List;

public class CassandraSourceConnector implements ISourceConnector {

  private final IConnectionHelper connectionHelper = new CassandraConnectionHelper();

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new CassandraDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  @Override
  public String getConnectionUrl(Shard shard) {
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
  public SourceSchema getSourceSchema(Shard shard) {
    CassandraShard cassandraShard = (CassandraShard) shard;
    CqlSessionBuilder builder = CqlSession.builder();
    DriverConfigLoader configLoader =
        CassandraDriverConfigLoader.fromOptionsMap(cassandraShard.getOptionsMap());
    builder.withConfigLoader(configLoader);
    try (CqlSession session = builder.build()) {
      return new CassandraInformationSchemaScanner(session, cassandraShard.getKeySpaceName()).scan();
    }
  }

  @Override
  public void validateNotReadOnly(List<Shard> shards) {
    // Cassandra does not support a global read-only mode validation
  }

  @Override
  public boolean isShardingSupported() {
    return false;
  }
}
