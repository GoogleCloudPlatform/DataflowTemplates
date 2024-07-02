/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.common;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes data to MySQL. */
public class MySqlDao implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlDao.class);

  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  /*private PoolingDriver driver = null;
  private String fullPoolName;
  private String poolName;*/
  // private HikariDataSource ds;
  String sqlUrl;
  String sqlUser;
  String sqlPasswd;
  String properties;

  public MySqlDao(String dummy) {
    // ds = null;
  }

  public MySqlDao(String sqlUrl, String sqlUser, String sqlPasswd, String properties) {
    // sqlUrl = sqlUrl + "?rewriteBatchedStatements=true";
    try {
      Class dirverClass = Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      LOG.error("There was not able to find the driver class");
    }
    /*
    Properties jdbcProperties = new Properties();
    if (properties != null && !properties.isEmpty()) {
      try (StringReader reader = new StringReader(properties)) {
        jdbcProperties.load(reader);
      } catch (IOException e) {
        System.err.println("Error converting string to properties: " + e.getMessage());
      }
    }
    jdbcProperties.setProperty("user", sqlUser);
    jdbcProperties.setProperty("password", sqlPasswd);
    ConnectionFactory driverManagerConnectionFactory =
        new DriverManagerConnectionFactory(sqlUrl, jdbcProperties);

    PoolableConnectionFactory poolfactory =
        new PoolableConnectionFactory(driverManagerConnectionFactory, null);
    ObjectPool connectionPool = new GenericObjectPool(poolfactory);

    poolfactory.setPool(connectionPool);
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
    this.poolName = "spanner-to-source";
    this.fullPoolName = "jdbc:apache:commons:dbcp:" + this.poolName;
    driver.registerPool(this.poolName, connectionPool);*/

    /* HikariConfig config = new HikariConfig();
    config.setJdbcUrl(sqlUrl);
    config.setUsername(sqlUser);
    config.setPassword(sqlPasswd);
    config.setMaximumPoolSize(1);

    Properties jdbcProperties = new Properties();
    if (properties != null && !properties.isEmpty()) {
      try (StringReader reader = new StringReader(properties)) {
        jdbcProperties.load(reader);
      } catch (IOException e) {
        System.err.println("Error converting string to properties: " + e.getMessage());
      }
    }

    for (String key : jdbcProperties.stringPropertyNames()) {
      String value = jdbcProperties.getProperty(key);
      config.addDataSourceProperty(key, value);
    }
    boolean status = false;
    while (!status) {
      try {
        ds = new HikariDataSource(config);
        status = true;
      } catch (com.zaxxer.hikari.pool.HikariPool.PoolInitializationException e) {
        // TODO: retry handling is configurable with retry count
        Metrics.counter(MySqlDao.class, "mySQL_retry_pool_init").inc();
        try {
          Thread.sleep(1000);
        } catch (java.lang.InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }*/
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
    this.properties = properties;
  }

  // writes to database in a batch
  public void batchWrite(List<String> batchStatements) throws SQLException {
    Connection connObj = null;
    Statement statement = null;
    boolean status = false;
    while (!status) {
      try {
        // connObj = DriverManager.getConnection(this.fullPoolName);
        // connObj = ds.getConnection();
        connObj = DriverManager.getConnection(this.sqlUrl, this.sqlUser, this.sqlPasswd);
        connObj.setAutoCommit(false);
        statement = connObj.createStatement();
        for (String stmt : batchStatements) {
          statement.addBatch(stmt);
        }
        statement.executeBatch();
        connObj.commit();
        status = true;
      } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException
          | java.sql.SQLTransientConnectionException e) {

        // TODO: retry handling is configurable with retry count
        LOG.warn("Connection exception while executing SQL, will retry : " + e.getMessage());
        // gives indication that the shard is being retried
        Metrics.counter(MySqlDao.class, "mySQL_retry_sql").inc();
        status = true;
      } catch (java.sql.SQLNonTransientConnectionException e) {
        if (e.getMessage().contains("Server shutdown in progress")) {
          LOG.warn(
              "Connection exception while executing SQL for shard : "
                  + ", will retry : "
                  + e.getMessage());
          // gives indication that the shard is being retried
          Metrics.counter(MySqlDao.class, "mySQL_retry_sql").inc();
          status = true;

        } else {
          throw e;
        }
      } catch (java.sql.SQLException e) {
        // This exception happens when the DB is shutting down
        if (e.getMessage().contains("No operations allowed after statement closed")) {
          LOG.warn(
              "Connection exception while executing SQL for shard : "
                  + ", will retry : "
                  + e.getMessage());
          // gives indication that the shard is being retried
          Metrics.counter(MySqlDao.class, "mySQL_retry_sql").inc();
          status = true;

        } else {
          throw e;
        }
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
    // driver.closePool(this.poolName);
    // ds.close();
  }
}
