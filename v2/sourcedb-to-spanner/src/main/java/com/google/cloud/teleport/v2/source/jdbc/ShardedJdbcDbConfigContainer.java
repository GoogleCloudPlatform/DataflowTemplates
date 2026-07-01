/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.jdbc;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.ISourceConnector;
import com.google.cloud.teleport.v2.source.SQLDialect;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.JdbcIoWrapper;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.source.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.transforms.Wait;

/** Implementation for sharded JDBC sources. */
public class ShardedJdbcDbConfigContainer implements JdbcDbConfigContainer {

  private final ISourceConnector connector;
  private final ImmutableList<Shard> shards;
  private final SQLDialect sqlDialect;
  private final SourceDbToSpannerOptions options;

  public ShardedJdbcDbConfigContainer(
      ISourceConnector connector,
      List<Shard> shards,
      SQLDialect sqlDialect,
      SourceDbToSpannerOptions options) {
    this.connector = connector;
    this.shards = ImmutableList.copyOf(shards);
    this.sqlDialect = sqlDialect;
    this.options = options;
  }

  @Override
  public JdbcIoWrapperConfigGroup getJdbcIoWrapperConfigGroup(
      List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    String workerZone = OptionsToConfigBuilder.extractWorkerZone(options);
    JdbcIoWrapperConfigGroup.Builder jdbcIoWrapperConfigGroupBuilder =
        JdbcIoWrapperConfigGroup.builder().setSourceDbDialect(sqlDialect);
    for (Shard shard : shards) {
      for (Map.Entry<String, String> entry : shard.getDbNameToLogicalShardIdMap().entrySet()) {
        String shardId = entry.getValue();
        String namespace = Optional.ofNullable(shard.getNamespace()).orElse(options.getNamespace());
        String dbName = entry.getKey();
        JdbcIOWrapperConfig shardConfig =
            OptionsToConfigBuilder.getJdbcIOWrapperConfig(
                connector,
                sqlDialect,
                sourceTables,
                null,
                shard.getHost(),
                shard.getConnectionProperties(),
                Integer.parseInt(shard.getPort()),
                shard.getUserName(),
                shard.getPassword(),
                dbName,
                namespace,
                shardId,
                options.getJdbcDriverClassName(),
                options.getJdbcDriverJars(),
                options.getMaxConnections(),
                options.getNumPartitions(),
                waitOnSignal,
                options.getFetchSize(),
                options.getUniformizationStageCountHint(),
                options.getProjectId(),
                workerZone,
                options.as(DataflowPipelineWorkerPoolOptions.class).getWorkerMachineType());
        jdbcIoWrapperConfigGroupBuilder.addShardConfig(shardConfig);
      }
    }
    return jdbcIoWrapperConfigGroupBuilder.build();
  }

  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return JdbcIoWrapper.of(getJdbcIoWrapperConfigGroup(sourceTables, waitOnSignal), connector);
  }
}
