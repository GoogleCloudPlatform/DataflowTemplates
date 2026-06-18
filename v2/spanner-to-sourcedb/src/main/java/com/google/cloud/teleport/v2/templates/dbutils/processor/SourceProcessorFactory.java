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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.connection.CassandraConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.connection.SpannerConnectionHelper;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.CassandraDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.SpannerTargetDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.dml.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.dml.PostgreSQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.dml.SpannerDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.UnsupportedSourceException;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraSourceConnector;
import com.google.cloud.teleport.v2.templates.source.mysql.MySQLSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgres.PostgreSQLSourceConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SourceProcessorFactory {

  private static final Map<String, ISourceConnector> sourceMap = new HashMap<>();

  static {
    sourceMap.put(Constants.SOURCE_MYSQL, new MySQLSourceConnector());
    sourceMap.put(Constants.SOURCE_POSTGRESQL, new PostgreSQLSourceConnector());
    sourceMap.put(Constants.SOURCE_CASSANDRA, new CassandraSourceConnector());
    sourceMap.put(Constants.SOURCE_SPANNER, new CassandraSourceConnector());
  }

  public static void registerSource(String sourceName, ISourceConnector source) {
    sourceMap.put(sourceName, source);
  }

  // for unit testing purposes
  public static Map<String, ISourceConnector> getSourceMap() {
    return new HashMap<>(sourceMap);
  }

  // for unit testing purposes
  public static void setSourceMap(Map<String, ISourceConnector> map) {
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
    ISourceConnector sourceInstance = getSource(source);

    IDMLGenerator dmlGenerator = sourceInstance.getDmlGenerator();
    sourceInstance.initConnectionHelper(shards, maxConnections);
    Map<String, IDao> sourceDaoMap = createSourceDaoMap(sourceInstance, shards);

    return SourceProcessor.builder().dmlGenerator(dmlGenerator).sourceDaoMap(sourceDaoMap).build();
  }

  public static ISourceConnector getSource(String source) throws UnsupportedSourceException {
    return Optional.ofNullable(sourceMap.get(source))
        .orElseThrow(() -> new UnsupportedSourceException("Invalid source type: " + source));
  }

  private static Map<String, IDao> createSourceDaoMap(
      ISourceConnector sourceInstance, List<Shard> shards) {
    Map<String, IDao> sourceDaoMap = new HashMap<>();
    for (Shard shard : shards) {
      sourceDaoMap.put(shard.getLogicalShardId(), sourceInstance.getDao(shard));
    }
    return sourceDaoMap;
  }
}
