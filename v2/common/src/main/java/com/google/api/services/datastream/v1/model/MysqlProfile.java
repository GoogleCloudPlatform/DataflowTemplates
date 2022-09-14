/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1.model;

/**
 * MySQL database profile.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlProfile extends com.google.api.client.json.GenericJson {

  /** Required. Hostname for the MySQL connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String hostname;

  /** Required. Input only. Password for the MySQL connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String password;

  /** Port for the MySQL connection, default value is 3306. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer port;

  /** SSL configuration for the MySQL connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlSslConfig sslConfig;

  /** Required. Username for the MySQL connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String username;

  /**
   * Required. Hostname for the MySQL connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getHostname() {
    return hostname;
  }

  /**
   * Required. Hostname for the MySQL connection.
   *
   * @param hostname hostname or {@code null} for none
   */
  public MysqlProfile setHostname(java.lang.String hostname) {
    this.hostname = hostname;
    return this;
  }

  /**
   * Required. Input only. Password for the MySQL connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPassword() {
    return password;
  }

  /**
   * Required. Input only. Password for the MySQL connection.
   *
   * @param password password or {@code null} for none
   */
  public MysqlProfile setPassword(java.lang.String password) {
    this.password = password;
    return this;
  }

  /**
   * Port for the MySQL connection, default value is 3306.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getPort() {
    return port;
  }

  /**
   * Port for the MySQL connection, default value is 3306.
   *
   * @param port port or {@code null} for none
   */
  public MysqlProfile setPort(java.lang.Integer port) {
    this.port = port;
    return this;
  }

  /**
   * SSL configuration for the MySQL connection.
   *
   * @return value or {@code null} for none
   */
  public MysqlSslConfig getSslConfig() {
    return sslConfig;
  }

  /**
   * SSL configuration for the MySQL connection.
   *
   * @param sslConfig sslConfig or {@code null} for none
   */
  public MysqlProfile setSslConfig(MysqlSslConfig sslConfig) {
    this.sslConfig = sslConfig;
    return this;
  }

  /**
   * Required. Username for the MySQL connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUsername() {
    return username;
  }

  /**
   * Required. Username for the MySQL connection.
   *
   * @param username username or {@code null} for none
   */
  public MysqlProfile setUsername(java.lang.String username) {
    this.username = username;
    return this;
  }

  @Override
  public MysqlProfile set(String fieldName, Object value) {
    return (MysqlProfile) super.set(fieldName, value);
  }

  @Override
  public MysqlProfile clone() {
    return (MysqlProfile) super.clone();
  }
}
