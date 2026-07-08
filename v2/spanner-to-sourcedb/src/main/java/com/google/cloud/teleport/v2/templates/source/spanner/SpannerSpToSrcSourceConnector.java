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

import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SpannerShardFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SpannerInformationSchemaScanner;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptions;

public class SpannerSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private final IConnectionHelper connectionHelper;

  public SpannerSpToSrcSourceConnector() {
    this.connectionHelper = new SpannerConnectionHelper();
  }

  @VisibleForTesting
  SpannerSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  @Override
  public IDMLGenerator getDmlGenerator() {
    return new SpannerDMLGenerator();
  }

  @Override
  public IConnectionHelper getConnectionHelper() {
    return connectionHelper;
  }

  String getConnectionUrl(Shard shard) {
    if (!(shard instanceof SpannerShard)) {
      throw new IllegalArgumentException(
          "Expected SpannerShard but got: "
              + (shard != null ? shard.getClass().getName() : "null"));
    }
    SpannerShard spannerShard = (SpannerShard) shard;
    return SpannerConnectionHelper.connectionKey(spannerShard);
  }

  @Override
  public IDao getDao(Shard shard) {
    return new SpannerTargetDao(
        SpannerConnectionHelper.connectionKey((SpannerShard) shard),
        (IConnectionHelper<com.google.cloud.spanner.DatabaseClient>) getConnectionHelper());
  }

  @Override
  public void initConnectionHelper(List<Shard> shards, int maxConnections) {
    // SpannerConnectionHelper does not need complex initialization in the same way as JDBC,
    if (!connectionHelper.isConnectionPoolInitialized()) {
      connectionHelper.init(
          new com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest(
              shards, null, maxConnections, null, null, null));
    }
  }

  @Override
  public List<Shard> parseShardConfig(String shardFilePath) throws Exception {
    SpannerShardFileReader spannerShardFileReader = new SpannerShardFileReader();
    return spannerShardFileReader.getSpannerShards(shardFilePath);
  }

  @Override
  public void validate(List<Shard> shards, PipelineOptions options) throws Exception {
    if (shards.size() != 1) {
      throw new IllegalArgumentException("Spanner migration must have exactly 1 shard.");
    }
    if (!(shards.get(0) instanceof SpannerShard)) {
      throw new IllegalArgumentException(
          "Expected SpannerShard but got: " + shards.get(0).getClass());
    }
    SpannerShard spannerShard = (SpannerShard) shards.get(0);

    com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options spannerOptions =
        options.as(com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options.class);
    if (spannerOptions != null
        && spannerOptions.getSpannerProjectId() != null
        && spannerOptions.getMetadataInstance() != null
        && spannerOptions.getMetadataDatabase() != null) {
      if (!spannerOptions.getSpannerProjectId().equals(spannerShard.getProjectId())
          || !spannerOptions.getMetadataInstance().equals(spannerShard.getInstanceId())
          || !spannerOptions.getMetadataDatabase().equals(spannerShard.getDatabaseId())) {
        throw new IllegalArgumentException(
            "For Cloud Spanner target, the metadata database and target database must be the same to ensure atomic operations.");
      }
      // TODO check for read only as well ?
    }
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    SpannerShard spannerShard = (SpannerShard) shards.get(0);
    SpannerConfig targetSpannerConfig =
        SpannerConfig.create()
            .withProjectId(spannerShard.getProjectId())
            .withInstanceId(spannerShard.getInstanceId())
            .withDatabaseId(spannerShard.getDatabaseId());
    return new SpannerInformationSchemaScanner(targetSpannerConfig).scan();
  }

  @Override
  public boolean supportsSharding() {
    return false;
  }

  @Override
  public boolean shouldUpdateReadValuesToSpannerRecord() {
    return true;
  }

  @Override
  public org.apache.beam.sdk.values.TupleTag<String> classifyException(Throwable cause) {
    return null;
  }
}
