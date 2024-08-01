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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes data to MySQL. */
public class MySqlDao implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlDao.class);

  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  String sqlUrl;
  String sqlUser;
  String sqlPasswd;

  public MySqlDao(String sqlUrl, String sqlUser, String sqlPasswd) {
    try {
      Class dirverClass = Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      LOG.error("There was not able to find the driver class");
    }

    // TODO: add the connection pool as needed
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
  }

  // writes to database
  public void write(String sqlStatement) throws SQLException {
    Connection connObj = null;
    Statement statement = null;
    boolean status = false;
    while (!status) {
      try {

        connObj = DriverManager.getConnection(this.sqlUrl, this.sqlUser, this.sqlPasswd);
        connObj.setAutoCommit(false);
        statement = connObj.createStatement();
        statement.executeUpdate(sqlStatement);
        connObj.commit();
        status = true;
      } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException
          | java.sql.SQLTransientConnectionException e) {

        // TODO: retry handling is configurable with retry count
        LOG.warn("Connection exception while executing SQL, will retry : " + e.getMessage());
        // gives indication that the shard is being retried
        Metrics.counter(MySqlDao.class, "mySQL_retry_sql").inc();
      } catch (java.sql.SQLNonTransientConnectionException e) {
        if (e.getMessage().contains("Server shutdown in progress")) {
          LOG.warn(
              "Connection exception while executing SQL for shard : "
                  + ", will retry : "
                  + e.getMessage());
          // gives indication that the shard is being retried
          Metrics.counter(MySqlDao.class, "mySQL_retry_sql").inc();
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
    // TODO: add the cleanup logic when connection pool is added
  }
}
