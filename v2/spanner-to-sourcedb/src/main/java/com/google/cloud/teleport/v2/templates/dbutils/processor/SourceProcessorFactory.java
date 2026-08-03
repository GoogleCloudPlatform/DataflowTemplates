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
package com.google.cloud.teleport.v2.templates.dbutils.processor;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.mysql.MySQLSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgres.PostgreSQLSpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.source.spanner.SpannerSpToSrcSourceConnector;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SourceProcessorFactory {

  private static final Map<String, ISpToSrcSourceConnector> sourceMap = new HashMap<>();

  static {
    sourceMap.put(Constants.SOURCE_MYSQL, new MySQLSpToSrcSourceConnector());
    sourceMap.put(Constants.SOURCE_POSTGRESQL, new PostgreSQLSpToSrcSourceConnector());
    sourceMap.put(Constants.SOURCE_CASSANDRA, new CassandraSpToSrcSourceConnector());
    sourceMap.put(Constants.SOURCE_SPANNER, new SpannerSpToSrcSourceConnector());
  }

  public static void registerSource(String sourceName, ISpToSrcSourceConnector source) {
    sourceMap.put(sourceName, source);
  }

  @VisibleForTesting
  public static Map<String, ISpToSrcSourceConnector> getSourceMap() {
    return new HashMap<>(sourceMap);
  }

  @VisibleForTesting
  public static void setSourceMap(Map<String, ISpToSrcSourceConnector> map) {
    sourceMap.clear();
    sourceMap.putAll(map);
  }

  /**
   * Creates a SourceProcessor instance for the specified source type.
   *
   * @param source the type of the source database
   * @param shards the list of shards for the source
   * @param maxConnections the maximum number of connections
   * @return a configured SourceProcessor instance
   * @throws Exception if the source type is invalid
   */
  public static SourceProcessor createSourceProcessor(
      String source, List<Shard> shards, int maxConnections) throws UnsupportedSourceException {
    ISpToSrcSourceConnector sourceInstance = getSource(source);

    IDMLGenerator dmlGenerator = sourceInstance.getDmlGenerator();
    sourceInstance.initConnectionHelper(shards, maxConnections);
    Map<String, IDao> sourceDaoMap = createSourceDaoMap(sourceInstance, shards);

    return SourceProcessor.builder().dmlGenerator(dmlGenerator).sourceDaoMap(sourceDaoMap).build();
  }

  public static ISpToSrcSourceConnector getSource(String source) throws UnsupportedSourceException {
    return Optional.ofNullable(sourceMap.get(source))
        .orElseThrow(() -> new UnsupportedSourceException("Invalid source type: " + source));
  }

  private static Map<String, IDao> createSourceDaoMap(
      ISpToSrcSourceConnector sourceInstance, List<Shard> shards) {
    Map<String, IDao> sourceDaoMap = new HashMap<>();
    for (Shard shard : shards) {
      sourceDaoMap.put(shard.getLogicalShardId(), sourceInstance.getDao(shard));
    }
    return sourceDaoMap;
  }
}
