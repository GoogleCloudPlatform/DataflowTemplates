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
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for implementing various Dao implementations. */
public abstract class Dao implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(Dao.class);

  protected String sqlUrl;

  protected final String sqlUser;
  protected final String sqlPasswd;
  protected final String shardId;
  protected final Boolean enableSsl;
  protected final Boolean enableSslValidation;
  protected String fullPoolName;

  public Dao(
      String sqlUrl,
      String sqlUser,
      String sqlPasswd,
      String shardId,
      Boolean enableSsl,
      Boolean enableSslValidation,
      String fullPoolName) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
    this.shardId = shardId;
    this.enableSsl = enableSsl;
    this.enableSslValidation = enableSslValidation;
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
        LOG.warn(
            "PostgreSQL Connection exception while executing SQL, will retry : " + e.getMessage());
      } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException e) {
        // TODO: retry handling is configurable with retry count
        LOG.warn("MySQL Connection exception while executing SQL, will retry : " + e.getMessage());
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

  protected void validateClassDependencies(List<String> classDependencies)
      throws ClassNotFoundException {
    for (String className : classDependencies) {
      Class.forName(className);
    }
  }

  abstract String getSslEnabledSqlUrl(
      String sqlUrl, Boolean enableSsl, Boolean enableSslValidation);

  // frees up the pooling resources
  public abstract void cleanup() throws Exception;
}
