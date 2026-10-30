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
package com.google.cloud.teleport.v2.source.oracle;

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

/** Oracle implementation of {@link ISourceConnector}. */
public class OracleSourceConnector implements ISourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceConnector.class);

  @Override
  public String getSourceType() {
    return "ORACLE";
  }

  @Override
  public SinkType getSinkType() {
    return SinkType.ORACLE;
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

    return new OracleSinkConfig(shards);
  }

  @Override
  public SinkSchemaFetcher getSchemaFetcher() {
    return new OracleSchemaFetcher();
  }

  @Override
  public TypeMapper getTypeMapper() {
    return new OracleTypeMapper();
  }

  @Override
  public DataWriter createDataWriter(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof OracleSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of OracleSinkConfig for Oracle sink type.");
    }
    return new OracleDataWriter((OracleSinkConfig) sinkConfig);
  }

  @Override
  public int resolveKeyParallelism(SinkConfig sinkConfig) {
    if (!(sinkConfig instanceof OracleSinkConfig)) {
      throw new IllegalArgumentException(
          "Sink configuration must be an instance of OracleSinkConfig for Oracle sink type.");
    }
    OracleSinkConfig oracleConfig = (OracleSinkConfig) sinkConfig;
    int shardCount = 1;
    if (oracleConfig.getShards() != null && !oracleConfig.getShards().isEmpty()) {
      shardCount = oracleConfig.getShards().size();
    }
    return 5000 * shardCount;
  }

  @Override
  public List<String> getLogicalShardIds(SinkConfig sinkConfig) {
    if (sinkConfig instanceof OracleSinkConfig) {
      OracleSinkConfig oracleConfig = (OracleSinkConfig) sinkConfig;
      if (oracleConfig.getShards() != null) {
        return oracleConfig.getShards().stream()
            .map(Shard::getLogicalShardId)
            .collect(Collectors.toList());
      }
    }
    return Collections.emptyList();
  }
}
