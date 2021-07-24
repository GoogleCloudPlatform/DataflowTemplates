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
 * Model definition for ConnectionProfile.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ConnectionProfile extends com.google.api.client.json.GenericJson {

  /** Output only. The create time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Forward SSH tunnel connectivity. The value may be {@code null}. */
  @com.google.api.client.util.Key private ForwardSshTunnelConnectivity forwardSshConnectivity;

  /** Cloud Storage ConnectionProfile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private GcsProfile gcsProfile;

  /** Labels. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** MySQL ConnectionProfile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlProfile mysqlProfile;

  /** Output only. The resource's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** No connectivity option chosen. The value may be {@code null}. */
  @com.google.api.client.util.Key private NoConnectivitySettings noConnectivity;

  /** Oracle ConnectionProfile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleProfile oracleProfile;

  /** Private connectivity. The value may be {@code null}. */
  @com.google.api.client.util.Key private PrivateConnectivity privateConnectivity;

  /** Static Service IP connectivity. The value may be {@code null}. */
  @com.google.api.client.util.Key private StaticServiceIpConnectivity staticServiceIpConnectivity;

  /** Output only. The update time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. The create time of the resource.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The create time of the resource.
   *
   * @param createTime createTime or {@code null} for none
   */
  public ConnectionProfile setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Required. Display name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * Required. Display name.
   *
   * @param displayName displayName or {@code null} for none
   */
  public ConnectionProfile setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Forward SSH tunnel connectivity.
   *
   * @return value or {@code null} for none
   */
  public ForwardSshTunnelConnectivity getForwardSshConnectivity() {
    return forwardSshConnectivity;
  }

  /**
   * Forward SSH tunnel connectivity.
   *
   * @param forwardSshConnectivity forwardSshConnectivity or {@code null} for none
   */
  public ConnectionProfile setForwardSshConnectivity(
      ForwardSshTunnelConnectivity forwardSshConnectivity) {
    this.forwardSshConnectivity = forwardSshConnectivity;
    return this;
  }

  /**
   * Cloud Storage ConnectionProfile configuration.
   *
   * @return value or {@code null} for none
   */
  public GcsProfile getGcsProfile() {
    return gcsProfile;
  }

  /**
   * Cloud Storage ConnectionProfile configuration.
   *
   * @param gcsProfile gcsProfile or {@code null} for none
   */
  public ConnectionProfile setGcsProfile(GcsProfile gcsProfile) {
    this.gcsProfile = gcsProfile;
    return this;
  }

  /**
   * Labels.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Labels.
   *
   * @param labels labels or {@code null} for none
   */
  public ConnectionProfile setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * MySQL ConnectionProfile configuration.
   *
   * @return value or {@code null} for none
   */
  public MysqlProfile getMysqlProfile() {
    return mysqlProfile;
  }

  /**
   * MySQL ConnectionProfile configuration.
   *
   * @param mysqlProfile mysqlProfile or {@code null} for none
   */
  public ConnectionProfile setMysqlProfile(MysqlProfile mysqlProfile) {
    this.mysqlProfile = mysqlProfile;
    return this;
  }

  /**
   * Output only. The resource's name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The resource's name.
   *
   * @param name name or {@code null} for none
   */
  public ConnectionProfile setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * No connectivity option chosen.
   *
   * @return value or {@code null} for none
   */
  public NoConnectivitySettings getNoConnectivity() {
    return noConnectivity;
  }

  /**
   * No connectivity option chosen.
   *
   * @param noConnectivity noConnectivity or {@code null} for none
   */
  public ConnectionProfile setNoConnectivity(NoConnectivitySettings noConnectivity) {
    this.noConnectivity = noConnectivity;
    return this;
  }

  /**
   * Oracle ConnectionProfile configuration.
   *
   * @return value or {@code null} for none
   */
  public OracleProfile getOracleProfile() {
    return oracleProfile;
  }

  /**
   * Oracle ConnectionProfile configuration.
   *
   * @param oracleProfile oracleProfile or {@code null} for none
   */
  public ConnectionProfile setOracleProfile(OracleProfile oracleProfile) {
    this.oracleProfile = oracleProfile;
    return this;
  }

  /**
   * Private connectivity.
   *
   * @return value or {@code null} for none
   */
  public PrivateConnectivity getPrivateConnectivity() {
    return privateConnectivity;
  }

  /**
   * Private connectivity.
   *
   * @param privateConnectivity privateConnectivity or {@code null} for none
   */
  public ConnectionProfile setPrivateConnectivity(PrivateConnectivity privateConnectivity) {
    this.privateConnectivity = privateConnectivity;
    return this;
  }

  /**
   * Static Service IP connectivity.
   *
   * @return value or {@code null} for none
   */
  public StaticServiceIpConnectivity getStaticServiceIpConnectivity() {
    return staticServiceIpConnectivity;
  }

  /**
   * Static Service IP connectivity.
   *
   * @param staticServiceIpConnectivity staticServiceIpConnectivity or {@code null} for none
   */
  public ConnectionProfile setStaticServiceIpConnectivity(
      StaticServiceIpConnectivity staticServiceIpConnectivity) {
    this.staticServiceIpConnectivity = staticServiceIpConnectivity;
    return this;
  }

  /**
   * Output only. The update time of the resource.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The update time of the resource.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public ConnectionProfile setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public ConnectionProfile set(String fieldName, Object value) {
    return (ConnectionProfile) super.set(fieldName, value);
  }

  @Override
  public ConnectionProfile clone() {
    return (ConnectionProfile) super.clone();
  }
}
