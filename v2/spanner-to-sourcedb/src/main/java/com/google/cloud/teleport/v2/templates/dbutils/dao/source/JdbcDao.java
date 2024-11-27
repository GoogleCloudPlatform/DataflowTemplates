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
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcDao implements IDao<String> {
  private String sqlUrl;
  private String sqlUser;

  private final IConnectionHelper connectionHelper;

  public JdbcDao(String sqlUrl, String sqlUser, IConnectionHelper connectionHelper) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.connectionHelper = connectionHelper;
  }

  @Override
  public void write(String sqlStatement) throws SQLException, ConnectionException {
    Connection connObj = null;
    Statement statement = null;

    try {

      connObj = (Connection) connectionHelper.getConnection(this.sqlUrl + "/" + this.sqlUser);
      if (connObj == null) {
        throw new ConnectionException("Connection is null");
      }
      statement = connObj.createStatement();
      statement.executeUpdate(sqlStatement);

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
