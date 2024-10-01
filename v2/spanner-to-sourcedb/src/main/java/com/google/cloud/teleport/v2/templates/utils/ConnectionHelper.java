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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a per Dataflow worker singleton that holds connection pool. */
public class ConnectionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionHelper.class);
  private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static Map<String, HikariDataSource> connectionPoolMap = null;

  public static synchronized void init(List<Shard> shards, String properties, int maxConnections) {
    if (connectionPoolMap != null) {
      return;
    }
    LOG.info("Initializing connection pool with size: ", maxConnections);
    connectionPoolMap = new HashMap<>();
    for (Shard shard : shards) {
      String sourceConnectionUrl =
          "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(sourceConnectionUrl);
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName(JDBC_DRIVER);
      config.setMaximumPoolSize(maxConnections);
      config.setConnectionInitSql(
          "SET SESSION net_read_timeout=1200"); // to avoid timeouts at network level layer
      Properties jdbcProperties = new Properties();
      if (properties != null && !properties.isEmpty()) {
        try (StringReader reader = new StringReader(properties)) {
          jdbcProperties.load(reader);
        } catch (IOException e) {
          LOG.error("Error converting string to properties: {}", e.getMessage());
        }
      }

      for (String key : jdbcProperties.stringPropertyNames()) {
        String value = jdbcProperties.getProperty(key);
        config.addDataSourceProperty(key, value);
      }
      HikariDataSource ds = new HikariDataSource(config);

      connectionPoolMap.put(sourceConnectionUrl + shard.getUserName() + shard.getPassword(), ds);
    }
  }

  public static Connection getConnection(String sourceConnectionUrl, String user, String password)
      throws java.sql.SQLException {
    if (connectionPoolMap == null) {
      LOG.warn("Connection pool not initialized");
      return null;
    }
    HikariDataSource ds = connectionPoolMap.get(sourceConnectionUrl + user + password);
    if (ds == null) {
      LOG.warn("Connection pool not found for source connection url: {}", sourceConnectionUrl);
      return null;
    }

    return ds.getConnection();
  }

  // for unit testing
  public static void setConnectionPoolMap(Map<String, HikariDataSource> inputMap) {
    connectionPoolMap = inputMap;
  }
}
