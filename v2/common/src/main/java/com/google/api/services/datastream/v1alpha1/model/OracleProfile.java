/*
 * Copyright (C) 2021 Google LLC
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
package com.google.api.services.datastream.v1alpha1.model;

/**
 * Oracle database profile.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class OracleProfile extends com.google.api.client.json.GenericJson {

  /** Connection string attributes The value may be {@code null}. */
  @com.google.api.client.util.Key
  private java.util.Map<String, java.lang.String> connectionAttributes;

  /** Required. Database for the Oracle connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String databaseService;

  /** Required. Hostname for the Oracle connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String hostname;

  /** Required. Password for the Oracle connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String password;

  /** Port for the Oracle connection, default value is 1521. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer port;

  /** Required. Username for the Oracle connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String username;

  /**
   * Connection string attributes
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getConnectionAttributes() {
    return connectionAttributes;
  }

  /**
   * Connection string attributes
   *
   * @param connectionAttributes connectionAttributes or {@code null} for none
   */
  public OracleProfile setConnectionAttributes(
      java.util.Map<String, java.lang.String> connectionAttributes) {
    this.connectionAttributes = connectionAttributes;
    return this;
  }

  /**
   * Required. Database for the Oracle connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDatabaseService() {
    return databaseService;
  }

  /**
   * Required. Database for the Oracle connection.
   *
   * @param databaseService databaseService or {@code null} for none
   */
  public OracleProfile setDatabaseService(java.lang.String databaseService) {
    this.databaseService = databaseService;
    return this;
  }

  /**
   * Required. Hostname for the Oracle connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getHostname() {
    return hostname;
  }

  /**
   * Required. Hostname for the Oracle connection.
   *
   * @param hostname hostname or {@code null} for none
   */
  public OracleProfile setHostname(java.lang.String hostname) {
    this.hostname = hostname;
    return this;
  }

  /**
   * Required. Password for the Oracle connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPassword() {
    return password;
  }

  /**
   * Required. Password for the Oracle connection.
   *
   * @param password password or {@code null} for none
   */
  public OracleProfile setPassword(java.lang.String password) {
    this.password = password;
    return this;
  }

  /**
   * Port for the Oracle connection, default value is 1521.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getPort() {
    return port;
  }

  /**
   * Port for the Oracle connection, default value is 1521.
   *
   * @param port port or {@code null} for none
   */
  public OracleProfile setPort(java.lang.Integer port) {
    this.port = port;
    return this;
  }

  /**
   * Required. Username for the Oracle connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUsername() {
    return username;
  }

  /**
   * Required. Username for the Oracle connection.
   *
   * @param username username or {@code null} for none
   */
  public OracleProfile setUsername(java.lang.String username) {
    this.username = username;
    return this;
  }

  @Override
  public OracleProfile set(String fieldName, Object value) {
    return (OracleProfile) super.set(fieldName, value);
  }

  @Override
  public OracleProfile clone() {
    return (OracleProfile) super.clone();
  }
}
