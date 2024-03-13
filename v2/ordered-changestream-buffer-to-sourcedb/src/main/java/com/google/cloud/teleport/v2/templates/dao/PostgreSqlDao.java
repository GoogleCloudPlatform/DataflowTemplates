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
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes data to PostgreSQL. */
public class PostgreSqlDao implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlDao.class);

  static final String JDBC_DRIVER = "org.postgresql.Driver";
  private PoolingDriver driver = null;
  private String poolName = "";
  private String fullPoolName = "";

  private String sqlUrl = "";

  public PostgreSqlDao(
      String sqlUrl,
      String sqlUser,
      String sqlPasswd,
      String shardId,
      Boolean enableSsl,
      Boolean enableSslValidation) {
    if (enableSsl) {
      sqlUrl = sqlUrl + "?ssl=verify-ca";
      if (!enableSslValidation) {
        sqlUrl = sqlUrl + "?ssl=verify-ca&sslfactory=org.postgresql.ssl.NonValidatingFactory";
      }
    }
    try {
      Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      LOG.error("Not able to find the driver class");
    }

    ConnectionFactory driverManagerConnectionFactory =
        new DriverManagerConnectionFactory(sqlUrl, sqlUser, sqlPasswd);

    PoolableConnectionFactory poolFactory =
        new PoolableConnectionFactory(driverManagerConnectionFactory, null);
    ObjectPool connectionPool = new GenericObjectPool(poolFactory);

    poolFactory.setPool(connectionPool);
    try {
      Class.forName("org.apache.commons.dbcp2.PoolingDriver");
    } catch (ClassNotFoundException e) {
      LOG.error("There was not able to find the driver class");
    }
    try {
      driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
    } catch (SQLException e) {
      LOG.error("There was an error: " + e.getMessage());
    }
    this.poolName = "buffer-to-source-" + shardId;
    this.fullPoolName = "jdbc:apache:commons:dbcp:" + this.poolName;
    driver.registerPool(this.poolName, connectionPool);
  }

  // writes to the database in a batch
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
      } catch (org.postgresql.util.PSQLException e) {
        // TODO: retry handling is configurable with retry count
        LOG.warn("Connection exception while executing SQL, will retry : " + e.getMessage());
      } finally {

        if (statement != null) {
          statement.close();
        }
        if (connObj != null) {
          connObj.close();
        }
      }
    }
  }

  // frees up the pooling resources
  public void cleanup() throws Exception {
    driver.closePool(this.poolName);
  }
}
