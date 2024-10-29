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
package com.google.cloud.teleport.v2.templates.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import com.google.cloud.teleport.v2.templates.utils.ConnectionHelper;
import com.google.cloud.teleport.v2.templates.utils.ISourceDao;

public class MySqlDao implements ISourceDao {
  private String sqlUrl;
  private String sqlUser;
  private String sqlPasswd;
  private Connection connObj;

  @Override
  public void initialize(String url, String user, String password) throws Exception {
    this.sqlUrl = url;
    this.sqlUser = user;
    this.sqlPasswd = password;
    Class.forName("com.mysql.cj.jdbc.Driver");
    this.connObj = ConnectionHelper.getConnection(this.sqlUrl, this.sqlUser, this.sqlPasswd);
  }

  @Override
  public void write(String sqlStatement) throws SQLException {
    try (Statement statement = connObj.createStatement()) {
      statement.executeUpdate(sqlStatement);
    }
  }

  @Override
  public void close() throws SQLException {
    if (connObj != null && !connObj.isClosed()) {
      connObj.close();
    }
  }
}
