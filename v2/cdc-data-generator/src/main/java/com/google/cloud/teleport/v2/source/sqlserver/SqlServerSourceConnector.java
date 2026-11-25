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
package com.google.cloud.teleport.v2.source.sqlserver;

import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SQL Server implementation of {@link ISourceConnector}. */
public class SqlServerSourceConnector implements ISourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerSourceConnector.class);

  @Override
  public String getSourceType() {
    return Constants.SINK_TYPE_SQLSERVER;
  }

  @Override
  public SinkType getSinkType() {
    return SinkType.SQLSERVER;
  }

  @Override
  public SinkConfig parseConfig(String sinkOptionsPath) throws IOException {
    if (sinkOptionsPath == null || sinkOptionsPath.isEmpty()) {
      throw new IllegalArgumentException("Sink options path must not be null or empty.");
    }

    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkOptionsPath);
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in shard configuration: " + sinkOptionsPath);
    }

    Set<String> usedShardIds = new HashSet<>();
    for (Shard shard : shards) {
      String id = shard.getLogicalShardId();
      if (id != null && !id.isEmpty() && !usedShardIds.add(id)) {
        throw new IllegalArgumentException(
            "Duplicate logicalShardId '"
                + id
                + "' found in shard configuration: "
                + sinkOptionsPath);
      }
    }

    int nextDefaultIndex = 0;
    for (int i = 0; i < shards.size(); i++) {
      Shard shard = shards.get(i);
      if (shard.getLogicalShardId() == null || shard.getLogicalShardId().isEmpty()) {
        String defaultId;
        do {
          defaultId = "shard" + nextDefaultIndex++;
        } while (!usedShardIds.add(defaultId));
        shard.setLogicalShardId(defaultId);
        LOG.info("Assigned default logicalShardId '{}' to shard index {}", defaultId, i);
      }
    }

    return new SqlServerSinkConfig(shards);
  }

  @Override
  public SinkSchemaFetcher getSchemaFetcher() {
    return new SqlServerSchemaFetcher();
  }

  @Override
  public TypeMapper getTypeMapper() {
    return new SqlServerTypeMapper();
  }

  @Override
  public DataWriter createDataWriter(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof SqlServerSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of SqlServerSinkConfig for SQL Server sink"
              + " type.");
    }
    return new SqlServerDataWriter((SqlServerSinkConfig) sinkConfig);
  }

  @Override
  public int resolveKeyParallelism(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof SqlServerSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of SqlServerSinkConfig for SQL Server sink"
              + " type.");
    }
    SqlServerSinkConfig sqlServerConfig = (SqlServerSinkConfig) sinkConfig;
    int shardCount = 1;
    if (sqlServerConfig.getShards() != null && !sqlServerConfig.getShards().isEmpty()) {
      shardCount = sqlServerConfig.getShards().size();
    }
    // Same limit as the other JDBC sinks (MySQL, Oracle): keep key-parallelism low to avoid
    // connection pool exhaustion and lock contention on a single SQL Server instance.
    return 5000 * shardCount;
  }

  @Override
  public List<String> getLogicalShardIds(SinkConfig sinkConfig) {
    if (sinkConfig instanceof SqlServerSinkConfig) {
      SqlServerSinkConfig sqlServerConfig = (SqlServerSinkConfig) sinkConfig;
      if (sqlServerConfig.getShards() != null) {
        return sqlServerConfig.getShards().stream()
            .map(Shard::getLogicalShardId)
            .collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }
}
