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
 * A zone represents a logical group of related assets within a lake. A zone can be used to map to
 * organizational structure or represent stages of data readiness from raw to curated. It provides
 * managing behavior that is shared or inherited by all contained assets.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Zone extends com.google.api.client.json.GenericJson {

  /**
   * Output only. The current set of actions required of the administrator for this zone. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<GoogleCloudDataplexV1Action> actions;

  static {
    // hack to force ProGuard to consider GoogleCloudDataplexV1Action used, since otherwise it would
    // be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(GoogleCloudDataplexV1Action.class);
  }

  /**
   * Output only. Aggregated status of the underlying assets of the zone. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetStatus assetStatus;

  /** Output only. The time when the zone was created. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Optional. Description of the zone. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /**
   * Optional. Specification of the discovery feature applied to data in this zone. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ZoneDiscoverySpec discoverySpec;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Optional. User defined labels for the zone. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id} The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Required. Specification of the resources that are referenced by the assets within this zone.
   * The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ZoneResourceSpec resourceSpec;

  /**
   * Optional. Specification of the security policy applied to data in this zone. Typically it
   * should take a few minutes for the security policy to fully propagate. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ZoneSecuritySpec securitySpec;

  /**
   * Output only. Status of the security policy applied to data in this zone. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1SecurityStatus securityStatus;

  /** Output only. Current state of the zone. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Required. Immutable. The type of the zone. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String type;

  /**
   * Output only. System generated globally unique ID for the zone. This ID will be different if the
   * zone is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the zone was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. The current set of actions required of the administrator for this zone.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1Action> getActions() {
    return actions;
  }

  /**
   * Output only. The current set of actions required of the administrator for this zone.
   *
   * @param actions actions or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setActions(java.util.List<GoogleCloudDataplexV1Action> actions) {
    this.actions = actions;
    return this;
  }

  /**
   * Output only. Aggregated status of the underlying assets of the zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetStatus getAssetStatus() {
    return assetStatus;
  }

  /**
   * Output only. Aggregated status of the underlying assets of the zone.
   *
   * @param assetStatus assetStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setAssetStatus(GoogleCloudDataplexV1AssetStatus assetStatus) {
    this.assetStatus = assetStatus;
    return this;
  }

  /**
   * Output only. The time when the zone was created.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The time when the zone was created.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Optional. Description of the zone.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the zone.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Optional. Specification of the discovery feature applied to data in this zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec getDiscoverySpec() {
    return discoverySpec;
  }

  /**
   * Optional. Specification of the discovery feature applied to data in this zone.
   *
   * @param discoverySpec discoverySpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setDiscoverySpec(
      GoogleCloudDataplexV1ZoneDiscoverySpec discoverySpec) {
    this.discoverySpec = discoverySpec;
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
  public GoogleCloudDataplexV1Zone setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Optional. User defined labels for the zone.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User defined labels for the zone.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. Specification of the resources that are referenced by the assets within this zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneResourceSpec getResourceSpec() {
    return resourceSpec;
  }

  /**
   * Required. Specification of the resources that are referenced by the assets within this zone.
   *
   * @param resourceSpec resourceSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setResourceSpec(
      GoogleCloudDataplexV1ZoneResourceSpec resourceSpec) {
    this.resourceSpec = resourceSpec;
    return this;
  }

  /**
   * Optional. Specification of the security policy applied to data in this zone. Typically it
   * should take a few minutes for the security policy to fully propagate.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneSecuritySpec getSecuritySpec() {
    return securitySpec;
  }

  /**
   * Optional. Specification of the security policy applied to data in this zone. Typically it
   * should take a few minutes for the security policy to fully propagate.
   *
   * @param securitySpec securitySpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setSecuritySpec(
      GoogleCloudDataplexV1ZoneSecuritySpec securitySpec) {
    this.securitySpec = securitySpec;
    return this;
  }

  /**
   * Output only. Status of the security policy applied to data in this zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1SecurityStatus getSecurityStatus() {
    return securityStatus;
  }

  /**
   * Output only. Status of the security policy applied to data in this zone.
   *
   * @param securityStatus securityStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setSecurityStatus(
      GoogleCloudDataplexV1SecurityStatus securityStatus) {
    this.securityStatus = securityStatus;
    return this;
  }

  /**
   * Output only. Current state of the zone.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Current state of the zone.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Required. Immutable. The type of the zone.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Required. Immutable. The type of the zone.
   *
   * @param type type or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setType(java.lang.String type) {
    this.type = type;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the zone. This ID will be different if the
   * zone is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the zone. This ID will be different if the
   * zone is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the zone was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the zone was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Zone setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Zone set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Zone) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Zone clone() {
    return (GoogleCloudDataplexV1Zone) super.clone();
  }
}
