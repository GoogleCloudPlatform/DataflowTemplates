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
package com.google.cloud.teleport.v2.templates.dbutils.dao.source;

import com.google.cloud.teleport.v2.templates.dbutils.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import java.sql.Connection;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDao implements IDao<String> {
  private String sqlUrl;
  private String sqlUser;

  private final IConnectionHelper connectionHelper;

  private static final Logger LOG = LoggerFactory.getLogger(JdbcDao.class);

  public JdbcDao(String sqlUrl, String sqlUser, IConnectionHelper connectionHelper) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(String sqlStatement, TransactionalCheck transactionalCheck) throws Exception {
    Connection connObj = null;
    Statement statement = null;

    try {
      connObj = (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser);
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      connObj.setAutoCommit(false);
      statement = connObj.createStatement();
      statement.executeUpdate(sqlStatement);

      if (transactionalCheck != null) {
        transactionalCheck.check();
      }
      connObj.commit();

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
