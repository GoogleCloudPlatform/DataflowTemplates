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
 * The PrivateConnection resource is used to establish private connectivity between Datastream and a
 * customer's network.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class PrivateConnection extends com.google.api.client.json.GenericJson {

  /** Output only. The create time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /**
   * Output only. In case of error, the details of the error in a user-friendly format. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private Error error;

  /** Labels. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** Output only. The resource's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Output only. The state of the Private Connection. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Output only. The update time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /** VPC Peering Config. The value may be {@code null}. */
  @com.google.api.client.util.Key private VpcPeeringConfig vpcPeeringConfig;

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
  public PrivateConnection setCreateTime(String createTime) {
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
  public PrivateConnection setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Output only. In case of error, the details of the error in a user-friendly format.
   *
   * @return value or {@code null} for none
   */
  public Error getError() {
    return error;
  }

  /**
   * Output only. In case of error, the details of the error in a user-friendly format.
   *
   * @param error error or {@code null} for none
   */
  public PrivateConnection setError(Error error) {
    this.error = error;
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
  public PrivateConnection setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
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
  public PrivateConnection setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Output only. The state of the Private Connection.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. The state of the Private Connection.
   *
   * @param state state or {@code null} for none
   */
  public PrivateConnection setState(java.lang.String state) {
    this.state = state;
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
  public PrivateConnection setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  /**
   * VPC Peering Config.
   *
   * @return value or {@code null} for none
   */
  public VpcPeeringConfig getVpcPeeringConfig() {
    return vpcPeeringConfig;
  }

  /**
   * VPC Peering Config.
   *
   * @param vpcPeeringConfig vpcPeeringConfig or {@code null} for none
   */
  public PrivateConnection setVpcPeeringConfig(VpcPeeringConfig vpcPeeringConfig) {
    this.vpcPeeringConfig = vpcPeeringConfig;
    return this;
  }

  @Override
  public PrivateConnection set(String fieldName, Object value) {
    return (PrivateConnection) super.set(fieldName, value);
  }

  @Override
  public PrivateConnection clone() {
    return (PrivateConnection) super.clone();
  }
}
