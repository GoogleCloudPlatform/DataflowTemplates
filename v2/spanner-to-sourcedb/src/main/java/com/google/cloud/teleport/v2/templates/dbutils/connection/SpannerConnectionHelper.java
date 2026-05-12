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
package com.google.cloud.teleport.v2.templates.dbutils.connection;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages {@link DatabaseClient} connections to target Cloud Spanner databases. */
public class SpannerConnectionHelper implements IConnectionHelper<DatabaseClient> {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerConnectionHelper.class);

  private static Map<String, DatabaseClient> clientMap = new ConcurrentHashMap<>();
  private static Spanner spannerService;

  @Override
  public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
    if (!clientMap.isEmpty()) {
      LOG.info("Spanner connection pool is already initialized.");
      return;
    }

    List<Shard> shards = connectionHelperRequest.getShards();
    String projectId = ((SpannerShard) shards.get(0)).getProjectId();
    spannerService = SpannerOptions.newBuilder().setProjectId(projectId).build().getService();

    for (Shard shard : shards) {
      if (!(shard instanceof SpannerShard)) {
        throw new IllegalArgumentException(
            "Expected SpannerShard but got: " + shard.getClass().getSimpleName());
      }
      SpannerShard spannerShard = (SpannerShard) shard;
      String key = connectionKey(spannerShard);
      DatabaseClient client =
          spannerService.getDatabaseClient(
              DatabaseId.of(
                  spannerShard.getProjectId(),
                  spannerShard.getInstanceId(),
                  spannerShard.getDatabaseId()));
      clientMap.put(key, client);
      LOG.info("Initialized Spanner connection for key: {}", key);
    }
  }

  @Override
  public DatabaseClient getConnection(String connectionRequestKey) throws ConnectionException {
    if (clientMap.isEmpty()) {
      throw new ConnectionException("Spanner connection pool is not initialized.");
    }
    DatabaseClient client = clientMap.get(connectionRequestKey);
    if (client == null) {
      throw new ConnectionException("No Spanner connection found for key: " + connectionRequestKey);
    }
    return client;
  }

  @Override
  public boolean isConnectionPoolInitialized() {
    return !clientMap.isEmpty();
  }

  public static String connectionKey(SpannerShard shard) {
    return shard.getProjectId() + "/" + shard.getInstanceId() + "/" + shard.getDatabaseId();
  }

  /** For unit testing only. */
  public void setClientMap(Map<String, DatabaseClient> inputMap) {
    clientMap = inputMap;
  }
}
