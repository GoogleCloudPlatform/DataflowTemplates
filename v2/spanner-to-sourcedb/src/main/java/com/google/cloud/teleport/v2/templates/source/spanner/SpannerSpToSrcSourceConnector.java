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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
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

/**
 * Implementation of SpToSrcConnector which will read from a Spanner database (called
 * originalSpanner) and write to a new database (called targetSpanner). The type of objects being
 * returned maybe tagged as SourceSchema etc as the target being written to is referred to as the
 * source for all other sources.
 */
public class SpannerSpToSrcSourceConnector implements ISpToSrcSourceConnector {

  private final IConnectionHelper connectionHelper;
  private Ddl targetDdl;

  public SpannerSpToSrcSourceConnector() {
    this.connectionHelper = new SpannerConnectionHelper();
  }

  @VisibleForTesting
  SpannerSpToSrcSourceConnector(IConnectionHelper connectionHelper) {
    this.connectionHelper = connectionHelper;
  }

  public void setTargetDdl(Ddl targetDdl) {
    this.targetDdl = targetDdl;
  }

  public Ddl getTargetDdl() {
    return targetDdl;
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
    SpannerShard spannerShard = (SpannerShard) shard;
    checkAndInitTargetDdl(spannerShard);
    return new SpannerTargetDao(
        SpannerConnectionHelper.connectionKey(spannerShard),
        (IConnectionHelper<com.google.cloud.spanner.DatabaseClient>) getConnectionHelper(),
        targetDdl);
  }

  private synchronized void checkAndInitTargetDdl(SpannerShard spannerShard) {
    // TODO - update the flow to set targetDDL in the constructor/init
    if (targetDdl == null) {
      SpannerConfig targetSpannerConfig =
          SpannerConfig.create()
              .withProjectId(spannerShard.getProjectId())
              .withInstanceId(spannerShard.getInstanceId())
              .withDatabaseId(spannerShard.getDatabaseId());
      targetDdl = new SpannerInformationSchemaScanner(targetSpannerConfig).scanDdl();
    }
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
  }

  @Override
  public SourceSchema getInformationSchema(List<Shard> shards) throws Exception {
    SpannerShard spannerShard = (SpannerShard) shards.get(0);
    SpannerConfig targetSpannerConfig =
        SpannerConfig.create()
            .withProjectId(spannerShard.getProjectId())
            .withInstanceId(spannerShard.getInstanceId())
            .withDatabaseId(spannerShard.getDatabaseId());
    SpannerInformationSchemaScanner scanner =
        new SpannerInformationSchemaScanner(targetSpannerConfig);
    targetDdl = scanner.scanDdl();
    return scanner.convertDdlToSourceSchema(targetDdl);
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
