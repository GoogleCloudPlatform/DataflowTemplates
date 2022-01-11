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
 * An asset represents a cloud resource that is being managed within a lake as a member of a zone.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Asset extends com.google.api.client.json.GenericJson {

  /**
   * Output only. The current set of actions required of the administrator for this asset. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<GoogleCloudDataplexV1Action> actions;

  static {
    // hack to force ProGuard to consider GoogleCloudDataplexV1Action used, since otherwise it would
    // be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(GoogleCloudDataplexV1Action.class);
  }

  /** Output only. The time when the asset was created. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Optional. Description of the asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /**
   * Required. Specification of the discovery feature applied to data referenced by this asset. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetDiscoverySpec discoverySpec;

  /**
   * Output only. Status of the discovery feature applied to data referenced by this asset. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetDiscoveryStatus discoveryStatus;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Optional. User defined labels for the asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id} The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Required. Specification of the resource that is referenced by this asset. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetResourceSpec resourceSpec;

  /**
   * Output only. Status of the resource referenced by this asset. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetResourceStatus resourceStatus;

  /**
   * Optional. Specification of the security policy applied to resource referenced by this asset.
   * Typically it should take a few minutes for the security policy to fully propagate. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetSecuritySpec securitySpec;

  /**
   * Output only. Status of the security policy applied to resource referenced by this asset. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetSecurityStatus securityStatus;

  /** Output only. Current state of the asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /**
   * Output only. System generated globally unique ID for the asset. This ID will be different if
   * the asset is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the asset was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. The current set of actions required of the administrator for this asset.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1Action> getActions() {
    return actions;
  }

  /**
   * Output only. The current set of actions required of the administrator for this asset.
   *
   * @param actions actions or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setActions(
      java.util.List<GoogleCloudDataplexV1Action> actions) {
    this.actions = actions;
    return this;
  }

  /**
   * Output only. The time when the asset was created.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The time when the asset was created.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Optional. Description of the asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the asset.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Required. Specification of the discovery feature applied to data referenced by this asset.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec getDiscoverySpec() {
    return discoverySpec;
  }

  /**
   * Required. Specification of the discovery feature applied to data referenced by this asset.
   *
   * @param discoverySpec discoverySpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setDiscoverySpec(
      GoogleCloudDataplexV1AssetDiscoverySpec discoverySpec) {
    this.discoverySpec = discoverySpec;
    return this;
  }

  /**
   * Output only. Status of the discovery feature applied to data referenced by this asset.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus getDiscoveryStatus() {
    return discoveryStatus;
  }

  /**
   * Output only. Status of the discovery feature applied to data referenced by this asset.
   *
   * @param discoveryStatus discoveryStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setDiscoveryStatus(
      GoogleCloudDataplexV1AssetDiscoveryStatus discoveryStatus) {
    this.discoveryStatus = discoveryStatus;
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
  public GoogleCloudDataplexV1Asset setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Optional. User defined labels for the asset.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User defined labels for the asset.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. Specification of the resource that is referenced by this asset.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceSpec getResourceSpec() {
    return resourceSpec;
  }

  /**
   * Required. Specification of the resource that is referenced by this asset.
   *
   * @param resourceSpec resourceSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setResourceSpec(
      GoogleCloudDataplexV1AssetResourceSpec resourceSpec) {
    this.resourceSpec = resourceSpec;
    return this;
  }

  /**
   * Output only. Status of the resource referenced by this asset.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceStatus getResourceStatus() {
    return resourceStatus;
  }

  /**
   * Output only. Status of the resource referenced by this asset.
   *
   * @param resourceStatus resourceStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setResourceStatus(
      GoogleCloudDataplexV1AssetResourceStatus resourceStatus) {
    this.resourceStatus = resourceStatus;
    return this;
  }

  /**
   * Optional. Specification of the security policy applied to resource referenced by this asset.
   * Typically it should take a few minutes for the security policy to fully propagate.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecuritySpec getSecuritySpec() {
    return securitySpec;
  }

  /**
   * Optional. Specification of the security policy applied to resource referenced by this asset.
   * Typically it should take a few minutes for the security policy to fully propagate.
   *
   * @param securitySpec securitySpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setSecuritySpec(
      GoogleCloudDataplexV1AssetSecuritySpec securitySpec) {
    this.securitySpec = securitySpec;
    return this;
  }

  /**
   * Output only. Status of the security policy applied to resource referenced by this asset.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecurityStatus getSecurityStatus() {
    return securityStatus;
  }

  /**
   * Output only. Status of the security policy applied to resource referenced by this asset.
   *
   * @param securityStatus securityStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setSecurityStatus(
      GoogleCloudDataplexV1AssetSecurityStatus securityStatus) {
    this.securityStatus = securityStatus;
    return this;
  }

  /**
   * Output only. Current state of the asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Current state of the asset.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the asset. This ID will be different if
   * the asset is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the asset. This ID will be different if
   * the asset is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the asset was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the asset was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Asset setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Asset set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Asset) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Asset clone() {
    return (GoogleCloudDataplexV1Asset) super.clone();
  }
}
