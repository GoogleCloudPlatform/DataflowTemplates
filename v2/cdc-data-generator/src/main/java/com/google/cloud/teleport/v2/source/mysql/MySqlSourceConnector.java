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
package com.google.cloud.teleport.v2.source.mysql;

import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.sink.TypeMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link ISourceConnector}. */
public class MySqlSourceConnector implements ISourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceConnector.class);

  @Override
  public String getSourceType() {
    return "MYSQL";
  }

  @Override
  public SinkType getSinkType() {
    return SinkType.MYSQL;
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

    for (int i = 0; i < shards.size(); i++) {
      Shard shard = shards.get(i);
      if (shard.getLogicalShardId() == null || shard.getLogicalShardId().isEmpty()) {
        String defaultId = "shard" + i;
        shard.setLogicalShardId(defaultId);
        LOG.info("Assigned default logicalShardId '{}' to shard index {}", defaultId, i);
      }
    }

    return new MySqlSinkConfig(shards);
  }

  @Override
  public SinkSchemaFetcher getSchemaFetcher() {
    return new MySqlSchemaFetcher();
  }

  @Override
  public TypeMapper getTypeMapper() {
    return new MySqlTypeMapper();
  }

  @Override
  public DataWriter createDataWriter(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof MySqlSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of MySqlSinkConfig for MySQL sink type.");
    }
    return new MySqlDataWriter((MySqlSinkConfig) sinkConfig);
  }

  @Override
  public int resolveKeyParallelism(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof MySqlSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of MySqlSinkConfig for MySQL sink type.");
    }
    MySqlSinkConfig mySqlConfig = (MySqlSinkConfig) sinkConfig;
    int shardCount = 1;
    if (mySqlConfig.getShards() != null && !mySqlConfig.getShards().isEmpty()) {
      shardCount = mySqlConfig.getShards().size();
    }
    // MySQL is not horizontally scalable like Spanner and performs better with fewer concurrent
    // write operations. We keep key-parallelism low to prevent connection pool exhaustion or lock
    // contention on the target instance: 500 DoFn per worker * 10 workers per MySQL shard.
    return 5000 * shardCount;
  }

  @Override
  public List<String> getLogicalShardIds(SinkConfig sinkConfig) {
    if (sinkConfig instanceof MySqlSinkConfig) {
      MySqlSinkConfig mySqlConfig = (MySqlSinkConfig) sinkConfig;
      if (mySqlConfig.getShards() != null) {
        return mySqlConfig.getShards().stream()
            .map(Shard::getLogicalShardId)
            .collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }
}
