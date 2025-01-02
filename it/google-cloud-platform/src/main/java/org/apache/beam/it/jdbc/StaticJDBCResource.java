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
package org.apache.beam.it.jdbc;

/** Parent class for Static JDBC Resources. */
public abstract class StaticJDBCResource {
  private final String hostname;
  private final String username;
  private final String password;
  private final int port;

  private final String database;

  enum SourceType {
    ORACLE,
    MYSQL,
    POSTGRESQL,
  }

  StaticJDBCResource(org.apache.beam.it.jdbc.StaticJDBCResource.Builder<?> builder) {
    this.hostname = builder.hostname;
    this.username = builder.username;
    this.password = builder.password;
    this.port = builder.port;
    this.database = builder.database;
  }

  public abstract org.apache.beam.it.jdbc.StaticJDBCResource.SourceType type();

  public String hostname() {
    return this.hostname;
  }

  public String username() {
    return this.username;
  }

  public String password() {
    return this.password;
  }

  public int port() {
    return this.port;
  }

  public String database() {
    return this.database;
  }

  public abstract String getJDBCPrefix();

  // TODO: exclude the StaticJDBCResource files from the codecov checks
  public String getconnectionURL() {
    return String.format("jdbc:%s://%s:%d/%s", getJDBCPrefix(), hostname, port, database);
  }

  public abstract static class Builder<T extends org.apache.beam.it.jdbc.StaticJDBCResource> {
    private final String hostname;
    private final String username;
    private final String password;
    private final int port;

    private final String database;

    public Builder(String hostname, String username, String password, int port, String database) {
      this.hostname = hostname;
      this.username = username;
      this.password = password;
      this.port = port;
      this.database = database;
    }

    public abstract T build();
  }
}
