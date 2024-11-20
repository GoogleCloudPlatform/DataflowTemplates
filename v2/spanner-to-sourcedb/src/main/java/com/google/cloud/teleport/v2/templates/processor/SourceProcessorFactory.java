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
import com.google.cloud.teleport.v2.templates.exceptions.InvalidSourceException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.templates.utils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.utils.connection.MySQLConnectionHelper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class SourceProcessorFactory {
  private static Map<String, IDMLGenerator> dmlGeneratorMap =
      Map.of(Constants.SOURCE_MYSQL, new MySQLDMLGenerator());

  private static Map<String, IConnectionHelper> connectionHelperMap =
      Map.of(Constants.SOURCE_MYSQL, new MySQLConnectionHelper());

  private static Map<String, String> driverMap =
      Map.of(Constants.SOURCE_MYSQL, "com.mysql.cj.jdbc.Driver");

  private static Map<String, Function<Shard, String>> connectionUrl =
      Map.of(
          Constants.SOURCE_MYSQL,
          shard ->
              "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName());

  // for unit testing purposes
  public static void setConnectionHelperMap(Map<String, IConnectionHelper> connectionHelper) {
    connectionHelperMap = connectionHelper;
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
      String source, List<Shard> shards, int maxConnections) throws InvalidSourceException {
    IDMLGenerator dmlGenerator = getDMLGenerator(source);
    IConnectionHelper connectionHelper = initializeConnectionHelper(source, shards, maxConnections);
    Map<String, IDao> sourceDaoMap = createSourceDaoMap(source, shards, connectionHelper);

    return SourceProcessor.builder().dmlGenerator(dmlGenerator).sourceDaoMap(sourceDaoMap).build();
  }

  private static IDMLGenerator getDMLGenerator(String source) throws InvalidSourceException {
    return Optional.ofNullable(dmlGeneratorMap.get(source))
        .orElseThrow(
            () -> new InvalidSourceException("Invalid source type for DML generator: " + source));
  }

  private static IConnectionHelper getConnectionHelper(String source)
      throws InvalidSourceException {
    return Optional.ofNullable(connectionHelperMap.get(source))
        .orElseThrow(
            () ->
                new InvalidSourceException("Invalid source type for connection helper: " + source));
  }

  private static IConnectionHelper initializeConnectionHelper(
      String source, List<Shard> shards, int maxConnections) throws InvalidSourceException {
    IConnectionHelper connectionHelper = getConnectionHelper(source);
    connectionHelper.init(new ConnectionHelperRequest(shards, null, maxConnections));
    return connectionHelper;
  }

  private static Map<String, IDao> createSourceDaoMap(
      String source, List<Shard> shards, IConnectionHelper connectionHelper)
      throws InvalidSourceException {
    Function<Shard, String> urlGenerator =
        Optional.ofNullable(connectionUrl.get(source))
            .orElseThrow(
                () ->
                    new InvalidSourceException(
                        "Invalid source type for URL generation: " + source));

    Map<String, IDao> sourceDaoMap = new HashMap<>();
    for (Shard shard : shards) {
      String connectionUrl = urlGenerator.apply(shard);
      IDao sqlDao = new JdbcDao(connectionUrl, shard.getUserName(), connectionHelper);
      sourceDaoMap.put(shard.getLogicalShardId(), sqlDao);
    }
    return sourceDaoMap;
  }
}
