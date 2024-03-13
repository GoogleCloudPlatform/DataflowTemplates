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

/** DaoFactory, currently only supports MySql. */
public class DaoFactory {
  private String dbHost = "";
  private String dbPort = "";
  private String dbName = "";
  private String sqlUrl = "";
  private String sqlUser = "";
  private String sqlPasswd = "";

  private MySqlDao mySqlDao;

  private PostgreSqlDao postgreSqlDao;

  public DaoFactory(String sqlUrl, String sqlUser, String sqlPasswd) {
    this.sqlUrl = sqlUrl;
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
  }

  public DaoFactory(
      String dbType,
      String dbHost,
      String dbPort,
      String dbName,
      String sqlUser,
      String sqlPasswd) {
    if ("mysql".equals(dbType)) {
      this.sqlUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
    } else if ("postgresql".equals(dbType)) {
      this.sqlUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName;
    }
    this.sqlUser = sqlUser;
    this.sqlPasswd = sqlPasswd;
  }

  public MySqlDao getMySqlDao(String dbHost, String dbPort, String dbName, String shardId) {
    if (this.mySqlDao != null) {
      return this.mySqlDao;
    }
    String sqlUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
    this.mySqlDao = new MySqlDao(sqlUrl, this.sqlUser, this.sqlPasswd, shardId);
    return this.mySqlDao;
  }

  public MySqlDao getMySqlDao(String shardId) {
    if (this.mySqlDao != null) {
      return this.mySqlDao;
    }
    this.mySqlDao = new MySqlDao(this.sqlUrl, this.sqlUser, this.sqlPasswd, shardId);
    return this.mySqlDao;
  }

  public PostgreSqlDao getPostgreSqlDao(
      String shardId, Boolean enableSsl, Boolean enableSslValidation) {
    if (this.postgreSqlDao != null) {
      return this.postgreSqlDao;
    }
    this.postgreSqlDao =
        new PostgreSqlDao(
            this.sqlUrl, this.sqlUser, this.sqlPasswd, shardId, enableSsl, enableSslValidation);
    return this.postgreSqlDao;
  }

  public PostgreSqlDao getPostgreSqlDao(String shardId) {
    if (this.postgreSqlDao != null) {
      return this.postgreSqlDao;
    }
    this.postgreSqlDao =
        new PostgreSqlDao(this.sqlUrl, this.sqlUser, this.sqlPasswd, shardId, true, true);
    return this.postgreSqlDao;
  }
}
