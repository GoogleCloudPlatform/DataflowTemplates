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
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.DbConfigContainer;
import com.google.cloud.teleport.v2.templates.PipelineController;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract class for JDBC source connectors. Handles sharded vs single instance migration. */
public abstract class AbstractJdbcSrcToSpSourceConnector implements IJdbcSrcToSpSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcSrcToSpSourceConnector.class);

  @Override
  public PipelineResult executeMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
    DbConfigContainer dbConfigContainer;
    if (options.getSourceConfigURL().startsWith("gs://")) {
      // TODO
      // Merge logical shards into 1 physical shard
      // Populate completion per shard
      // Take connection properties map
      // Write to common DLQ ?
      SQLDialect sqlDialect = SQLDialect.valueOf(options.getSourceDbDialect());

      List<Shard> shards =
          new ShardFileReader(new SecretManagerAccessorImpl())
              .readForwardMigrationShardingConfig(options.getSourceConfigURL());
      LOG.info(
          "running migration for {} shards: {}",
          shards.stream().count(),
          shards.stream().map(Shard::getHost).collect(Collectors.toList()));
      dbConfigContainer = new ShardedJdbcDbConfigContainer(shards, sqlDialect, options);
    } else {
      dbConfigContainer = new SingleInstanceJdbcDbConfigContainer(options);
    }
    return PipelineController.executeMigrationForDbConfigContainer(
        options, pipeline, spannerConfig, dbConfigContainer);
  }
}
