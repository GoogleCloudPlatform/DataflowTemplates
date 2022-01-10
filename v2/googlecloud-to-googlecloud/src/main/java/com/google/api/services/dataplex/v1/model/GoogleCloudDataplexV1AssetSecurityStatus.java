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
package com.google.api.services.dataplex.v1.model;

/**
 * Security policy status of the asset. Data security policy, i.e., readers, writers & owners,
 * should be specified in the lake/zone/asset IAM policy.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetSecurityStatus
    extends com.google.api.client.json.GenericJson {

  /**
   * Cumulative set of owner groups that were last applied on the managed resource. These groups may
   * have been specified at lake, zone or asset levels. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> appliedOwnerGroups;

  /**
   * Cumulative set of reader groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> appliedReaderGroups;

  /**
   * Cumulative set of writer groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> appliedWriterGroups;

  /** Additional information about the current state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /**
   * The current state of the security policy applied to the attached resource. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Last update time of the status. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Cumulative set of owner groups that were last applied on the managed resource. These groups may
   * have been specified at lake, zone or asset levels.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getAppliedOwnerGroups() {
    return appliedOwnerGroups;
  }

  /**
   * Cumulative set of owner groups that were last applied on the managed resource. These groups may
   * have been specified at lake, zone or asset levels.
   *
   * @param appliedOwnerGroups appliedOwnerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setAppliedOwnerGroups(
      java.util.List<java.lang.String> appliedOwnerGroups) {
    this.appliedOwnerGroups = appliedOwnerGroups;
    return this;
  }

  /**
   * Cumulative set of reader groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getAppliedReaderGroups() {
    return appliedReaderGroups;
  }

  /**
   * Cumulative set of reader groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels.
   *
   * @param appliedReaderGroups appliedReaderGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setAppliedReaderGroups(
      java.util.List<java.lang.String> appliedReaderGroups) {
    this.appliedReaderGroups = appliedReaderGroups;
    return this;
  }

  /**
   * Cumulative set of writer groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getAppliedWriterGroups() {
    return appliedWriterGroups;
  }

  /**
   * Cumulative set of writer groups that were last applied on the managed resource. These groups
   * may have been specified at lake, zone or asset levels.
   *
   * @param appliedWriterGroups appliedWriterGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setAppliedWriterGroups(
      java.util.List<java.lang.String> appliedWriterGroups) {
    this.appliedWriterGroups = appliedWriterGroups;
    return this;
  }

  /**
   * Additional information about the current state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Additional information about the current state.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The current state of the security policy applied to the attached resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * The current state of the security policy applied to the attached resource.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Last update time of the status.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Last update time of the status.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetSecurityStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetSecurityStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetSecurityStatus clone() {
    return (GoogleCloudDataplexV1AssetSecurityStatus) super.clone();
  }
}
