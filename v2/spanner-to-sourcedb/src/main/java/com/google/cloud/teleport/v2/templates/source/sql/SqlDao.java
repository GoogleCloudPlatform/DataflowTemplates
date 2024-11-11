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
package com.google.cloud.teleport.v2.templates.source.sql;

import com.google.cloud.teleport.v2.templates.source.common.ISourceDao;
import com.google.cloud.teleport.v2.templates.utils.ConnectionException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlDao implements ISourceDao {
  private String sqlUrl;
  private String sqlUser;
  private String sqlPasswd;

  public SqlDao(String sqlUrl, String sqlUser, String sqlPasswd) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
  }

  public String getSourceConnectionUrl() {
    return sqlUrl;
  }

  public void write(String sqlStatement) throws SQLException, ConnectionException {
    Connection connObj = null;
    Statement statement = null;

    try {

      connObj = SQLConnectionHelper.getConnection(this.sqlUrl, this.sqlUser, this.sqlPasswd);
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
