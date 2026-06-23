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
package com.google.cloud.teleport.v2.templates.source.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

/**
 * Spanner implementation of {@link ISourceConnector}. Encapsulates connection management, DML
 * generation, and DAO initialization for Cloud Spanner target databases.
 */
public class SpannerSourceConnector implements ISourceConnector {

  private final IConnectionHelper<DatabaseClient> connectionHelper;

  public SpannerSourceConnector() {
    this.connectionHelper = new SpannerConnectionHelper();
  }

  @VisibleForTesting
  SpannerSourceConnector(IConnectionHelper<DatabaseClient> connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new SpannerDMLGenerator();
  }

  @Override
  public IConnectionHelper<DatabaseClient> getConnectionHelper() {
    return connectionHelper;
  }

  @Override
  public String getConnectionUrl(Shard shard) {
    if (!(shard instanceof SpannerShard)) {
      throw new IllegalArgumentException(
          "Expected SpannerShard but got: " + shard.getClass().getSimpleName());
    }
    return SpannerConnectionHelper.connectionKey((SpannerShard) shard);
  }

  @Override
  public IDao getDao(Shard shard) {
    return new SpannerTargetDao(getConnectionUrl(shard), getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    if (!connectionHelper.isConnectionPoolInitialized()) {
      ConnectionHelperRequest request =
          new ConnectionHelperRequest(shards, null, maxConnections, null, null, null);
      connectionHelper.init(request);
    }
  }
}
