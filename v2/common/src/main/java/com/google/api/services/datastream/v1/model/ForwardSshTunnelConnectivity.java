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
 * Forward SSH Tunnel connectivity.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ForwardSshTunnelConnectivity extends com.google.api.client.json.GenericJson {

  /** Required. Hostname for the SSH tunnel. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String hostname;

  /** Input only. SSH password. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String password;

  /** Port for the SSH tunnel, default value is 22. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer port;

  /** Input only. SSH private key. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String privateKey;

  /** Required. Username for the SSH tunnel. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String username;

  /**
   * Required. Hostname for the SSH tunnel.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getHostname() {
    return hostname;
  }

  /**
   * Required. Hostname for the SSH tunnel.
   *
   * @param hostname hostname or {@code null} for none
   */
  public ForwardSshTunnelConnectivity setHostname(java.lang.String hostname) {
    this.hostname = hostname;
    return this;
  }

  /**
   * Input only. SSH password.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPassword() {
    return password;
  }

  /**
   * Input only. SSH password.
   *
   * @param password password or {@code null} for none
   */
  public ForwardSshTunnelConnectivity setPassword(java.lang.String password) {
    this.password = password;
    return this;
  }

  /**
   * Port for the SSH tunnel, default value is 22.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getPort() {
    return port;
  }

  /**
   * Port for the SSH tunnel, default value is 22.
   *
   * @param port port or {@code null} for none
   */
  public ForwardSshTunnelConnectivity setPort(java.lang.Integer port) {
    this.port = port;
    return this;
  }

  /**
   * Input only. SSH private key.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPrivateKey() {
    return privateKey;
  }

  /**
   * Input only. SSH private key.
   *
   * @param privateKey privateKey or {@code null} for none
   */
  public ForwardSshTunnelConnectivity setPrivateKey(java.lang.String privateKey) {
    this.privateKey = privateKey;
    return this;
  }

  /**
   * Required. Username for the SSH tunnel.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUsername() {
    return username;
  }

  /**
   * Required. Username for the SSH tunnel.
   *
   * @param username username or {@code null} for none
   */
  public ForwardSshTunnelConnectivity setUsername(java.lang.String username) {
    this.username = username;
    return this;
  }

  @Override
  public ForwardSshTunnelConnectivity set(String fieldName, Object value) {
    return (ForwardSshTunnelConnectivity) super.set(fieldName, value);
  }

  @Override
  public ForwardSshTunnelConnectivity clone() {
    return (ForwardSshTunnelConnectivity) super.clone();
  }
}
