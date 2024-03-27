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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes data to PostgreSQL. */
public class PostgreSqlDao extends BaseDao {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlDao.class);

  private static final String JDBC_DRIVER = "org.postgresql.Driver";

  private static final String COMMONS_DBCP_DRIVER_URL = "jdbc:apache:commons:dbcp:";

  private static final String COMMONS_DBCP_2_POOLING_DRIVER =
      "org.apache.commons.dbcp2.PoolingDriver";

  private PoolingDriver driver;
  private final String poolName;

  public PostgreSqlDao(
      String sqlUrl, String sqlUser, String sqlPasswd, String shardId, String fullPoolName) {
    super(sqlUrl, sqlUser, sqlPasswd, shardId, fullPoolName);
    this.poolName = "buffer-to-source-" + shardId;
    this.fullPoolName = COMMONS_DBCP_DRIVER_URL + this.poolName;
    try {
      validateClassDependencies(List.of(JDBC_DRIVER, COMMONS_DBCP_2_POOLING_DRIVER));
    } catch (ClassNotFoundException e) {
      LOG.error("Not able to validate the class dependencies");
    }
    try {
      this.driver = (PoolingDriver) DriverManager.getDriver(COMMONS_DBCP_DRIVER_URL);
    } catch (SQLException e) {
      LOG.error("There was an error: " + e.getMessage());
    }
    ObjectPool connectionPool = getObjectPool(sqlUrl, sqlUser, sqlPasswd);
    this.driver.registerPool(this.poolName, connectionPool);
  }

  // frees up the pooling resources
  @Override
  public void cleanup() throws SQLException {
    this.driver.closePool(this.poolName);
  }
}
