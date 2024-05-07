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
package com.google.cloud.teleport.v2.options;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig.builderWithMySqlDefaults;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public final class OptionsToConfigBuilder {

  public static final class MySql {

    public static JdbcIOWrapperConfig configWithMySqlDefaultsFromOptions(
        SourceDbToSpannerOptions options) {
      JdbcIOWrapperConfig.Builder builder = builderWithMySqlDefaults();
      builder =
          builder
              .setSourceHost(options.getSourceHost())
              .setSourcePort(options.getSourcePort())
              .setSourceSchemaReference(
                  SourceSchemaReference.builder().setDbName(options.getSourceDB()).build())
              .setDbAuth(
                  LocalCredentialsProvider.builder()
                      .setUserName(options.getUsername())
                      .setPassword(options.getPassword())
                      .build())
              .setJdbcDriverClassName(options.getJdbcDriverClassName())
              .setJdbcDriverJars(options.getJdbcDriverJars())
              .setShardID("Unsupported"); /*TODO: Support Sharded Migration */
      if (options.getSourceConnectionProperties() != "") {
        builder = builder.setConnectionProperties(options.getSourceConnectionProperties());
      }
      if (options.getMaxConnections() != 0) {
        builder.setMaxConnections((long) options.getMaxConnections());
      }
      if (options.getReconnectsEnabled()) {
        builder.setAutoReconnect(true);
        if (options.getReconnectAttempts() != 0) {
          builder.setReconnectAttempts((long) options.getReconnectAttempts());
        }
      }
      ImmutableMap<String, String> tablesWithPartitionColumns =
          getTablesWithPartitionColumn(options);
      ImmutableList<TableConfig> tableConfigs =
          tablesWithPartitionColumns.entrySet().stream()
              .map(
                  entry -> {
                    TableConfig.Builder configBuilder =
                        TableConfig.builder(entry.getKey()).withPartitionColum(entry.getValue());
                    if (options.getNumPartitions() != 0) {
                      configBuilder = configBuilder.setMaxPartitions(options.getNumPartitions());
                    }
                    if (options.getFetchSize() != 0) {
                      configBuilder = configBuilder.setMaxFetchSize(options.getFetchSize());
                    }
                    return configBuilder.build();
                  })
              .collect(ImmutableList.toImmutableList());
      builder = builder.setTableConfigs(tableConfigs);
      return builder.build();
    }
  }

  private static ImmutableMap<String, String> getTablesWithPartitionColumn(
      SourceDbToSpannerOptions options) {
    String[] tables = options.getTables().split(",");
    String[] partitionColumns = options.getPartitionColumns().split(",");
    if (tables.length != partitionColumns.length) {
      throw new RuntimeException(
          "invalid configuration. Partition column count does not match " + "tables count.");
    }
    ImmutableMap.Builder<String, String> tableWithPartitionColumnBuilder = ImmutableMap.builder();
    for (int i = 0; i < tables.length; i++) {
      tableWithPartitionColumnBuilder.put(tables[i], partitionColumns[i]);
    }
    return tableWithPartitionColumnBuilder.build();
  }

  private OptionsToConfigBuilder() {}
}
