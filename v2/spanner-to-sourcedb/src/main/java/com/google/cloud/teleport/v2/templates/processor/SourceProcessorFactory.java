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
package com.google.cloud.teleport.v2.templates.processor;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dml.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.templates.utils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.utils.connection.MySQLConnectionHelper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class SourceProcessorFactory {
  private static final Map<String, IDMLGenerator> DML_GENERATOR_MAP =
      Map.of(Constants.SOURCE_MYSQL, new MySQLDMLGenerator());

  private static final Map<String, IConnectionHelper> CONNECTION_HELPER_MAP =
      Map.of(Constants.SOURCE_MYSQL, new MySQLConnectionHelper());

  private static final Map<String, String> DRIVER_MAP =
      Map.of(Constants.SOURCE_MYSQL, "com.mysql.cj.jdbc.Driver");

  private static final Map<String, Function<Shard, String>> CONNECTION_URL_GENERATORS =
      Map.of(
          Constants.SOURCE_MYSQL,
          shard ->
              "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName());

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
      String source, List<Shard> shards, int maxConnections) throws Exception {
    IDMLGenerator dmlGenerator = getDMLGenerator(source);
    String driver = getDriver(source);
    IConnectionHelper connectionHelper =
        initializeConnectionHelper(source, shards, maxConnections, driver);
    Map<String, IDao> sourceDaoMap = createSourceDaoMap(source, shards, connectionHelper);

    return SourceProcessor.builder().dmlGenerator(dmlGenerator).sourceDaoMap(sourceDaoMap).build();
  }

  private static IDMLGenerator getDMLGenerator(String source) throws Exception {
    return Optional.ofNullable(DML_GENERATOR_MAP.get(source))
        .orElseThrow(() -> new Exception("Invalid source type for DML generator: " + source));
  }

  private static String getDriver(String source) throws Exception {
    return Optional.ofNullable(DRIVER_MAP.get(source))
        .orElseThrow(() -> new Exception("Invalid source type for driver: " + source));
  }

  private static IConnectionHelper getConnectionHelper(String source) throws Exception {
    return Optional.ofNullable(CONNECTION_HELPER_MAP.get(source))
        .orElseThrow(() -> new Exception("Invalid source type for connection helper: " + source));
  }

  private static IConnectionHelper initializeConnectionHelper(
      String source, List<Shard> shards, int maxConnections, String driver) throws Exception {
    IConnectionHelper connectionHelper = getConnectionHelper(source);
    connectionHelper.init(new ConnectionHelperRequest(shards, null, maxConnections));
    return connectionHelper;
  }

  private static Map<String, IDao> createSourceDaoMap(
      String source, List<Shard> shards, IConnectionHelper connectionHelper) throws Exception {
    Function<Shard, String> urlGenerator =
        Optional.ofNullable(CONNECTION_URL_GENERATORS.get(source))
            .orElseThrow(() -> new Exception("Invalid source type for URL generation: " + source));

    Map<String, IDao> sourceDaoMap = new HashMap<>();
    for (Shard shard : shards) {
      String connectionUrl = urlGenerator.apply(shard);
      IDao sqlDao = new JdbcDao(connectionUrl, shard.getUserName(), connectionHelper);
      sourceDaoMap.put(shard.getLogicalShardId(), sqlDao);
    }
    return sourceDaoMap;
  }
}
