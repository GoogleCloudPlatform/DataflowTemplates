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
package com.google.cloud.teleport.v2.spanner.migrations.connection;

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a per Dataflow worker singleton that holds connection pool. */
public class JdbcConnectionHelper implements IConnectionHelper<Connection> {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionHelper.class);
  private static volatile Map<String, HikariDataSource> connectionPoolMap = null;

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
        "Initializing connection pool with size: {}", connectionHelperRequest.getMaxConnections());
    Map<String, HikariDataSource> localMap = new HashMap<>();
    for (Shard shard : connectionHelperRequest.getShards()) {
      String sourceConnectionUrl =
          new StringBuilder()
              .append(connectionHelperRequest.getJdbcUrlPrefix())
              .append(shard.getHost())
              .append(":")
              .append(shard.getPort())
              .append("/")
              .append(shard.getDbName())
              .toString();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(sourceConnectionUrl);
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName(connectionHelperRequest.getDriver());
      config.setMaximumPoolSize(connectionHelperRequest.getMaxConnections());
      config.setConnectionInitSql(connectionHelperRequest.getConnectionInitQuery());
      config.setInitializationFailTimeout(-1); // do not fail during pool construction
      config.setMinimumIdle(0); // avoid pre-filling connections
      Properties jdbcProperties = new Properties();
      if (shard.getConnectionProperties() != null && !shard.getConnectionProperties().isEmpty()) {
        Properties parsedProps = parseProperties(shard.getConnectionProperties());
        for (String key : parsedProps.stringPropertyNames()) {
          jdbcProperties.setProperty(key, parsedProps.getProperty(key));
        }
      }

      for (String key : jdbcProperties.stringPropertyNames()) {
        String value = jdbcProperties.getProperty(key);
        config.addDataSourceProperty(key, value);
      }
      HikariDataSource ds = new HikariDataSource(config);
      localMap.put(sourceConnectionUrl + "/" + shard.getUserName(), ds);
    }
    connectionPoolMap = localMap;
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

  /**
   * Parses connection properties from a string into a {@link Properties} object.
   *
   * <p>Supports both newline-delimited Java properties format and URL-encoded query parameters
   * (separated by '&' or ';'). URL-encoded values are automatically decoded.
   *
   * @param connectionProperties The connection properties string.
   * @return A Properties object containing the parsed key-value pairs.
   */
  public static Properties parseProperties(String connectionProperties) {
    Properties jdbcProperties = new Properties();
    if (connectionProperties == null || connectionProperties.isEmpty()) {
      return jdbcProperties;
    }

    if (connectionProperties.contains("&") || connectionProperties.contains(";")) {
      String[] pairs = connectionProperties.split("[&;]");
      for (String pair : pairs) {
        String[] kv = pair.split("=", 2);
        if (kv.length == 2) {
          try {
            String decodedKey = java.net.URLDecoder.decode(kv[0], java.nio.charset.StandardCharsets.UTF_8.name());
            String decodedValue = java.net.URLDecoder.decode(kv[1], java.nio.charset.StandardCharsets.UTF_8.name());
            jdbcProperties.setProperty(decodedKey, decodedValue);
          } catch (java.io.UnsupportedEncodingException e) {
            LOG.error("UTF-8 encoding not supported", e);
          }
        }
      }
    } else {
      try (java.io.StringReader reader = new java.io.StringReader(connectionProperties)) {
        jdbcProperties.load(reader);
      } catch (java.io.IOException e) {
        LOG.error("Failed to parse connection properties", e);
      }
    }
    return jdbcProperties;
  }
}
