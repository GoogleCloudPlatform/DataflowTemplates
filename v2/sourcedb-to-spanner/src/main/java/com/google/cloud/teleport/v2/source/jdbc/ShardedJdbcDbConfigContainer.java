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
package com.google.cloud.teleport.v2.source.jdbc;

import com.google.cloud.teleport.v2.options.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.transforms.Wait;

/**
 * Implementation for sharded JDBC sources.
 *
 * <p><b>DLQ Folder Path:</b> The DLQ folder path logic has been simplified. Previously, shard IDs
 * were sometimes embedded in the path. Now, shard IDs are handled as metadata within the DLQ
 * records themselves (see {@link com.google.cloud.teleport.v2.writer.DeadLetterQueue}), allowing
 * for a unified DLQ directory structure. There is no change in the final output format in GCS.
 */
public class ShardedJdbcDbConfigContainer implements JdbcDbConfigContainer {

  private ImmutableList<Shard> shards;
  private SQLDialect sqlDialect;
  private SourceDbToSpannerOptions options;

  public ShardedJdbcDbConfigContainer(
      List<Shard> shards, SQLDialect sqlDialect, SourceDbToSpannerOptions options) {
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
      // TODO Move towards clubbing all physical shards together in a single connection pool.
      for (Map.Entry<String, String> entry : shard.getDbNameToLogicalShardIdMap().entrySet()) {
        // Read data from source
        String shardId = entry.getValue();

        // If a namespace is configured for a shard uses that, otherwise uses the namespace
        // configured in the options if there is one.
        String dbName = entry.getKey();
        JdbcIOWrapperConfig shardConfig =
            OptionsToConfigBuilder.getJdbcIOWrapperConfig(
                sqlDialect,
                sourceTables,
                shard.getHost(),
                shard.getConnectionProperties(),
                Integer.parseInt(shard.getPort()),
                shard.getUserName(),
                shard.getPassword(),
                dbName,
                shard.getNamespace(),
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
}
