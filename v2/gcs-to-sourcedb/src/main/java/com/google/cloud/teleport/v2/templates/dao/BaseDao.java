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
package com.google.cloud.teleport.v2.templates.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDao implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDao.class);
  protected String sqlUrl;
  protected final String sqlUser;
  protected final String sqlPasswd;
  protected final String shardId;
  protected String fullPoolName;
  protected PoolingDriver driver;
  protected String poolName;

  public BaseDao(
      String sqlUrl, String sqlUser, String sqlPasswd, String shardId, String fullPoolName) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
    this.shardId = shardId;
    this.fullPoolName = fullPoolName;
  }

  protected ObjectPool getObjectPool(String sqlUrl, String sqlUser, String sqlPasswd) {
    ConnectionFactory driverManagerConnectionFactory =
        new DriverManagerConnectionFactory(sqlUrl, sqlUser, sqlPasswd);

    PoolableConnectionFactory poolFactory =
        new PoolableConnectionFactory(driverManagerConnectionFactory, null);
    ObjectPool connectionPool = new GenericObjectPool(poolFactory);
    poolFactory.setPool(connectionPool);
    return connectionPool;
  }

  public void batchWrite(List<String> batchStatements) throws SQLException {
    Connection connObj = null;
    Statement statement = null;
    boolean status = false;
    while (!status) {
      try {
        connObj = DriverManager.getConnection(this.fullPoolName);

        statement = connObj.createStatement();
        for (String stmt : batchStatements) {
          statement.addBatch(stmt);
        }
        statement.executeBatch();
        status = true;
      } catch (java.sql.SQLNonTransientConnectionException e) {
        if (e.getMessage().contains("Server shutdown in progress")) {
          LOG.warn(
              "Connection exception while executing SQL for shard : "
                  + shardId
                  + ", will retry : "
                  + e.getMessage());
          // gives indication that the shard is being retried
          Metrics.counter(BaseDao.class, "db_connection_retry_" + shardId).inc();
          try {
            Thread.sleep(1000);
          } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        } else {
          throw e;
        }
      } catch (java.sql.SQLException e) {
        // This exception happens when the DB is shutting down
        if (e.getMessage().contains("No operations allowed after statement closed")) {
          LOG.warn(
              "Connection exception while executing SQL for shard : "
                  + shardId
                  + ", will retry : "
                  + e.getMessage());
          // gives indication that the shard is being retried
          Metrics.counter(BaseDao.class, "db_connection_retry_" + shardId).inc();
          try {
            Thread.sleep(1000);
          } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        } else {
          throw e;
        }

      } finally {
        // closing the connection and statement objects
        if (statement != null) {
          statement.close();
        }
        if (connObj != null) {
          connObj.close();
        }
      }
    }
  }

  protected void validateClassDependencies(List<String> classDependencies)
      throws ClassNotFoundException {
    for (String className : classDependencies) {
      Class.forName(className);
    }
  }

  // frees up the pooling resources
  public void cleanup() throws Exception {
    driver.closePool(this.poolName);
  }
}
