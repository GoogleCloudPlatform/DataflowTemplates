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

public class MySqlDao {

  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private PoolingDriver driver = null;

  public MySqlDao(String sqlUrl, String sqlUser, String sqlPasswd) {

    try {
      Class dirverClass = Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      System.err.println("There was not able to find the driver class");
    }

    ConnectionFactory driverManagerConnectionFactory =
        new DriverManagerConnectionFactory(sqlUrl, sqlUser, sqlPasswd);

    PoolableConnectionFactory poolfactory =
        new PoolableConnectionFactory(driverManagerConnectionFactory, null);
    ObjectPool connectionPool = new GenericObjectPool(poolfactory);

    poolfactory.setPool(connectionPool);
    try {
      Class.forName("org.apache.commons.dbcp2.PoolingDriver");
    } catch (ClassNotFoundException e) {
      System.err.println("There was not able to find the driver class");
    }
    try {
      driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
    } catch (SQLException e) {
      System.err.println("There was an error: " + e.getMessage());
    }
    driver.registerPool("kafka-to-source", connectionPool);
  }

  public void batchWrite(List<String> batchStatements) throws SQLException {
    Connection connObj = null;
    Statement statement = null;
    try {
      connObj = DriverManager.getConnection("jdbc:apache:commons:dbcp:kafka-to-source");

      statement = connObj.createStatement();
      for (String stmt : batchStatements) {
        statement.addBatch(stmt);
      }
      statement.executeBatch();
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
