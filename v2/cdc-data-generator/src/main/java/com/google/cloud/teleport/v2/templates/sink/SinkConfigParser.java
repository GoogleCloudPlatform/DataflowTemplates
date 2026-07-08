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
package com.google.cloud.teleport.v2.templates.sink;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import com.google.cloud.teleport.v2.templates.spanner.SpannerConfigFileReader;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to parse sink configuration and pre-fetch database dialect on driver JVM. */
public class SinkConfigParser {

  private static final Logger LOG = LoggerFactory.getLogger(SinkConfigParser.class);

  @VisibleForTesting
  interface DialectProvider {
    Dialect getDialect(String projectId, String instanceId, String databaseId);
  }

  private static DialectProvider dialectProvider =
      (projectId, instanceId, databaseId) -> {
        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        try (Spanner spanner = options.getService()) {
          return spanner.getDatabaseAdminClient().getDatabase(instanceId, databaseId).getDialect();
        }
      };

  @VisibleForTesting
  static void setDialectProvider(DialectProvider provider) {
    dialectProvider = provider;
  }

  public static SinkConfig parse(SinkType sinkType, String sinkOptionsPath) throws IOException {
    if (sinkOptionsPath == null || sinkOptionsPath.isEmpty()) {
      throw new IllegalArgumentException("Sink options path must not be null or empty.");
    }

    if (sinkType == SinkType.SPANNER) {
      SpannerConfigFileReader reader = new SpannerConfigFileReader();
      SpannerSinkConfig config = reader.getSpannerConfig(sinkOptionsPath);
      String projectId = config.getProjectId();
      String instanceId = config.getInstanceId();
      String databaseId = config.getDatabaseId();

      Dialect dialect = dialectProvider.getDialect(projectId, instanceId, databaseId);
      LOG.info("Pre-fetched Spanner dialect on driver: {}", dialect);

      config.setDialect(dialect);
      return config;
    } else if (sinkType == SinkType.MYSQL) {
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
    } else {
      throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
    }
  }
}
