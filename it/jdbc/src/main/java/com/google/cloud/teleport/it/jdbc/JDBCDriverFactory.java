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
package com.google.cloud.teleport.it.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** JDBC Driver Factory class. */
public class JDBCDriverFactory {

  public JDBCDriverFactory() {}

  /**
   * Returns a Connection session to the given database uri.
   *
   * @param uri the jdbc connection string to connect to.
   * @param username the username used to log in to the database.
   * @param password the password used to log in to the database.
   * @return the Connection session.
   * @throws SQLException if there is an error creating a connection.
   */
  public Connection getConnection(String uri, String username, String password)
      throws SQLException {
    return DriverManager.getConnection(uri, username, password);
  }
}
