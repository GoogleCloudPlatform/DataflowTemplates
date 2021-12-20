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
 * The Route resource is the child of the PrivateConnection resource. It used to define a route for
 * a PrivateConnection setup.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Route extends com.google.api.client.json.GenericJson {

  /** Output only. The create time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Destination address for connection The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String destinationAddress;

  /** Destination port for connection The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer destinationPort;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Labels. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** Output only. The resource's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

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
  public Route setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Required. Destination address for connection
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDestinationAddress() {
    return destinationAddress;
  }

  /**
   * Required. Destination address for connection
   *
   * @param destinationAddress destinationAddress or {@code null} for none
   */
  public Route setDestinationAddress(java.lang.String destinationAddress) {
    this.destinationAddress = destinationAddress;
    return this;
  }

  /**
   * Destination port for connection
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getDestinationPort() {
    return destinationPort;
  }

  /**
   * Destination port for connection
   *
   * @param destinationPort destinationPort or {@code null} for none
   */
  public Route setDestinationPort(java.lang.Integer destinationPort) {
    this.destinationPort = destinationPort;
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
  public Route setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
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
  public Route setLabels(java.util.Map<String, java.lang.String> labels) {
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
  public Route setName(java.lang.String name) {
    this.name = name;
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
  public Route setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public Route set(String fieldName, Object value) {
    return (Route) super.set(fieldName, value);
  }

  @Override
  public Route clone() {
    return (Route) super.clone();
  }
}
