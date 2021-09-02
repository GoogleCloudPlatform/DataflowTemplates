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
 * Environment represents a user-visible compute infrastructure for analytics within a lake.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Environment extends com.google.api.client.json.GenericJson {

  /** Output only. Environment creation time. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Optional. Description of the environment. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /**
   * Output only. URI Endpoints to access sessions associated with the Environment. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1EnvironmentEndpoints endpoints;

  /** Required. Infrastructure specification for the Environment. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EnvironmentInfrastructureSpec infrastructureSpec;

  /** Optional. User defined labels for the environment. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Output only. The relative resource name of the environment, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id} The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Optional. Configuration for sessions created for this environment. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1EnvironmentSessionSpec sessionSpec;

  /**
   * Output only. Status of sessions created for this environment. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EnvironmentSessionStatus sessionStatus;

  /** Output only. Current state of the environment. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /**
   * Output only. System generated globally unique ID for the environment. This ID will be different
   * if the environment is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the environment was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. Environment creation time.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. Environment creation time.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Optional. Description of the environment.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the environment.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Optional. User friendly display name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * Optional. User friendly display name.
   *
   * @param displayName displayName or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Output only. URI Endpoints to access sessions associated with the Environment.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentEndpoints getEndpoints() {
    return endpoints;
  }

  /**
   * Output only. URI Endpoints to access sessions associated with the Environment.
   *
   * @param endpoints endpoints or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setEndpoints(
      GoogleCloudDataplexV1EnvironmentEndpoints endpoints) {
    this.endpoints = endpoints;
    return this;
  }

  /**
   * Required. Infrastructure specification for the Environment.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpec getInfrastructureSpec() {
    return infrastructureSpec;
  }

  /**
   * Required. Infrastructure specification for the Environment.
   *
   * @param infrastructureSpec infrastructureSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setInfrastructureSpec(
      GoogleCloudDataplexV1EnvironmentInfrastructureSpec infrastructureSpec) {
    this.infrastructureSpec = infrastructureSpec;
    return this;
  }

  /**
   * Optional. User defined labels for the environment.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User defined labels for the environment.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setLabels(
      java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The relative resource name of the environment, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the environment, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Optional. Configuration for sessions created for this environment.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentSessionSpec getSessionSpec() {
    return sessionSpec;
  }

  /**
   * Optional. Configuration for sessions created for this environment.
   *
   * @param sessionSpec sessionSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setSessionSpec(
      GoogleCloudDataplexV1EnvironmentSessionSpec sessionSpec) {
    this.sessionSpec = sessionSpec;
    return this;
  }

  /**
   * Output only. Status of sessions created for this environment.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentSessionStatus getSessionStatus() {
    return sessionStatus;
  }

  /**
   * Output only. Status of sessions created for this environment.
   *
   * @param sessionStatus sessionStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setSessionStatus(
      GoogleCloudDataplexV1EnvironmentSessionStatus sessionStatus) {
    this.sessionStatus = sessionStatus;
    return this;
  }

  /**
   * Output only. Current state of the environment.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Current state of the environment.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the environment. This ID will be different
   * if the environment is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the environment. This ID will be different
   * if the environment is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the environment was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the environment was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Environment setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Environment set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Environment) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Environment clone() {
    return (GoogleCloudDataplexV1Environment) super.clone();
  }
}
