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
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages {@link DatabaseClient} connections to target Cloud Spanner databases. */
public class SpannerConnectionHelper implements IConnectionHelper<DatabaseClient> {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerConnectionHelper.class);

  private static final Map<String, SpannerAccessor> accessorMap = new ConcurrentHashMap<>();

  @Override
  public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
    if (!accessorMap.isEmpty()) {
      LOG.info("Spanner connection pool is already initialized.");
      return;
    }

    List<Shard> shards = connectionHelperRequest.getShards();

    for (Shard shard : shards) {
      if (!(shard instanceof SpannerShard)) {
        throw new IllegalArgumentException(
            "Expected SpannerShard but got: " + shard.getClass().getSimpleName());
      }
      SpannerShard spannerShard = (SpannerShard) shard;
      String key = connectionKey(spannerShard);

      SpannerConfig config =
          SpannerConfig.create()
              .withProjectId(spannerShard.getProjectId())
              .withInstanceId(spannerShard.getInstanceId())
              .withDatabaseId(spannerShard.getDatabaseId());

      SpannerAccessor accessor = SpannerAccessor.getOrCreate(config);
      accessorMap.put(key, accessor);
      LOG.info("Initialized Spanner connection for key: {}", key);
    }
  }

  @Override
  public DatabaseClient getConnection(String connectionRequestKey) throws ConnectionException {
    if (accessorMap.isEmpty()) {
      throw new ConnectionException("Spanner connection pool is not initialized.");
    }
    SpannerAccessor accessor = accessorMap.get(connectionRequestKey);
    if (accessor == null) {
      throw new ConnectionException("No Spanner connection found for key: " + connectionRequestKey);
    }
    return accessor.getDatabaseClient();
  }

  @Override
  public boolean isConnectionPoolInitialized() {
    return !accessorMap.isEmpty();
  }

  public static String connectionKey(SpannerShard shard) {
    return shard.getProjectId() + "/" + shard.getInstanceId() + "/" + shard.getDatabaseId();
  }

  /** For unit testing only. */
  public void setAccessorMap(Map<String, SpannerAccessor> inputMap) {
    accessorMap.clear();
    accessorMap.putAll(inputMap);
  }

  public void close() {
    for (SpannerAccessor accessor : accessorMap.values()) {
      accessor.close();
    }
    accessorMap.clear();
  }
}
