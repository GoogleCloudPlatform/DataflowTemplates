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
package com.google.cloud.teleport.v2.templates.dao;

import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes data to MySQL. */
public class MySQLDao extends BaseDao {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLDao.class);

  static final String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";

  public MySQLDao(
      String sqlUrl, String sqlUser, String sqlPasswd, String shardId, String fullPoolName) {
    super(sqlUrl, sqlUser, sqlPasswd, shardId, fullPoolName);
    this.poolName = "mysql-gcs-to-sourcedb-" + shardId;
    this.fullPoolName = Constants.COMMONS_DBCP_DRIVER_URL_PREFIX + this.poolName;
    sqlUrl = sqlUrl + "?rewriteBatchedStatements=true";
    try {
      validateClassDependencies(
          List.of(MYSQL_JDBC_DRIVER, Constants.COMMONS_DBCP_2_POOLING_DRIVER));
    } catch (ClassNotFoundException e) {
      LOG.error("Not able to validate the class dependencies." + e.getMessage());
    }
    try {
      this.driver =
          (PoolingDriver) DriverManager.getDriver(Constants.COMMONS_DBCP_DRIVER_URL_PREFIX);
    } catch (SQLException e) {
      LOG.error("There was an error in getting the PoolingDriver: " + e.getMessage());
    }
    ObjectPool connectionPool = getObjectPool(sqlUrl, sqlUser, sqlPasswd);
    this.driver.registerPool(this.poolName, connectionPool);
  }

  public void batchWrite(List<String> batchStatements) throws SQLException {
    // catch exception specific to MySQL connection errors
    try {
      super.batchWrite(batchStatements);
    } catch (com.mysql.cj.jdbc.exceptions.CommunicationsException e) {
      // TODO: retry handling is configurable with retry count
      LOG.warn(
          "MySQL Connection exception while executing SQL for shard : "
              + this.shardId
              + ", will retry : "
              + e.getMessage());
      // gives indication that the shard is being retried
      Metrics.counter(MySQLDao.class, "db_connection_retry_" + shardId).inc();

      // handling the connection retry
      try {
        Thread.sleep(1000);
      } catch (java.lang.InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
