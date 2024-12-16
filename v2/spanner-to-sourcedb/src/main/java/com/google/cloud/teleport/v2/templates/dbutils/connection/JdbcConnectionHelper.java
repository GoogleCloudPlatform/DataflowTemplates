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
package com.google.cloud.teleport.v2.templates.dbutils.connection;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a per Dataflow worker singleton that holds connection pool. */
public class JdbcConnectionHelper implements IConnectionHelper<Connection> {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionHelper.class);
  private static Map<String, HikariDataSource> connectionPoolMap = null;

  @Override
  public synchronized boolean isConnectionPoolInitialized() {
    if (connectionPoolMap != null) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
    if (connectionPoolMap != null) {
      return;
    }
    LOG.info(
        "Initializing connection pool with size: ", connectionHelperRequest.getMaxConnections());
    connectionPoolMap = new HashMap<>();
    for (Shard shard : connectionHelperRequest.getShards()) {
      String sourceConnectionUrl =
          "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(sourceConnectionUrl);
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName(connectionHelperRequest.getDriver());
      config.setMaximumPoolSize(connectionHelperRequest.getMaxConnections());
      config.setConnectionInitSql(connectionHelperRequest.getConnectionInitQuery());
      Properties jdbcProperties = new Properties();
      if (connectionHelperRequest.getProperties() != null
          && !connectionHelperRequest.getProperties().isEmpty()) {
        try (StringReader reader = new StringReader(connectionHelperRequest.getProperties())) {
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

      connectionPoolMap.put(sourceConnectionUrl + "/" + shard.getUserName(), ds);
    }
  }

  @Override
  public Connection getConnection(String connectionRequestKey) throws ConnectionException {
    try {
      if (connectionPoolMap == null) {
        LOG.warn("Connection pool not initialized");
        return null;
      }
      HikariDataSource ds = connectionPoolMap.get(connectionRequestKey);
      if (ds == null) {
        LOG.warn("Connection pool not found for source connection : {}", connectionRequestKey);
        return null;
      }

      return ds.getConnection();
    } catch (Exception e) {
      throw new ConnectionException(e);
    }
  }

  // for unit testing
  public void setConnectionPoolMap(Map<String, HikariDataSource> inputMap) {
    connectionPoolMap = inputMap;
  }
}
