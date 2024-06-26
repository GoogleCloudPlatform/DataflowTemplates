/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql;

import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Dummy container class so that {@link CloudSqlResourceManager} can leverage {@link
 * AbstractJDBCResourceManager} framework which leverages Testcontainers, and therefore requires a
 * container.
 */
class CloudSqlContainer<T extends CloudSqlContainer<T>> extends JdbcDatabaseContainer<T> {

  private String username;
  private String password;
  private String databaseName;

  CloudSqlContainer(DockerImageName dockerImageName) {
    super(DockerImageName.parse(""));
  }

  static CloudSqlContainer<?> of() {
    return new CloudSqlContainer<>(DockerImageName.parse(""));
  }

  @Override
  public String getDriverClassName() {
    return null;
  }

  @Override
  public String getJdbcUrl() {
    return null;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  protected String getTestQueryString() {
    return null;
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  public T withUsername(String username) {
    this.username = username;
    return this.self();
  }

  public T withPassword(String password) {
    this.password = password;
    return this.self();
  }

  public T withDatabaseName(String dbName) {
    this.databaseName = dbName;
    return this.self();
  }
}
