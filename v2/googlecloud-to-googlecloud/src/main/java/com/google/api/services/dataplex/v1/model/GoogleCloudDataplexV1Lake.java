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
 * A lake is a centralized repository for managing enterprise data across the organization
 * distributed across many cloud projects, and stored in a variety of storage services such as
 * Google Cloud Storage and BigQuery. The resources attached to a lake are referred to as managed
 * resources. Data within these managed resources can be structured or unstructured. A lake provides
 * data admins with tools to organize, secure and manage their data at scale, and provides data
 * scientists and data engineers an integrated experience to easily search, discover, analyze and
 * transform data and associated metadata.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Lake extends com.google.api.client.json.GenericJson {

  /**
   * Output only. The current set of actions required of the administrator for this lake. The value
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
   * Output only. Aggregated status of the underlying assets of the lake. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetStatus assetStatus;

  /** Output only. The time when the lake was created. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Optional. Description of the lake. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /**
   * Settings for development/test use by the Dataplex team. This is not used by users of the API.
   * The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1LakeInternalSpec internalSpec;

  /** Optional. User-defined labels for the lake. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Optional. Settings to manage lake and Dataproc Metastore service instance association. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1LakeMetastore metastore;

  /** Output only. Metastore status of the lake. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1LakeMetastoreStatus metastoreStatus;

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id} The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Optional. Specification of the security policy applied to data in this lake. Typically it
   * should take a few minutes for the security policy to fully propagate. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1LakeSecuritySpec securitySpec;

  /**
   * Output only. Status of the security policy applied to data in this lake. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1SecurityStatus securityStatus;

  /**
   * Output only. Service account associated with this lake. This service account must be authorized
   * to access or operate on resources managed by the lake. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String serviceAccount;

  /** Output only. Current state of the lake. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /**
   * Output only. System generated globally unique ID for the lake. This ID will be different if the
   * lake is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the lake was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. The current set of actions required of the administrator for this lake.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1Action> getActions() {
    return actions;
  }

  /**
   * Output only. The current set of actions required of the administrator for this lake.
   *
   * @param actions actions or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setActions(java.util.List<GoogleCloudDataplexV1Action> actions) {
    this.actions = actions;
    return this;
  }

  /**
   * Output only. Aggregated status of the underlying assets of the lake.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetStatus getAssetStatus() {
    return assetStatus;
  }

  /**
   * Output only. Aggregated status of the underlying assets of the lake.
   *
   * @param assetStatus assetStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setAssetStatus(GoogleCloudDataplexV1AssetStatus assetStatus) {
    this.assetStatus = assetStatus;
    return this;
  }

  /**
   * Output only. The time when the lake was created.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The time when the lake was created.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Optional. Description of the lake.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the lake.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setDescription(java.lang.String description) {
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
  public GoogleCloudDataplexV1Lake setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Settings for development/test use by the Dataplex team. This is not used by users of the API.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeInternalSpec getInternalSpec() {
    return internalSpec;
  }

  /**
   * Settings for development/test use by the Dataplex team. This is not used by users of the API.
   *
   * @param internalSpec internalSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setInternalSpec(
      GoogleCloudDataplexV1LakeInternalSpec internalSpec) {
    this.internalSpec = internalSpec;
    return this;
  }

  /**
   * Optional. User-defined labels for the lake.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User-defined labels for the lake.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Optional. Settings to manage lake and Dataproc Metastore service instance association.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastore getMetastore() {
    return metastore;
  }

  /**
   * Optional. Settings to manage lake and Dataproc Metastore service instance association.
   *
   * @param metastore metastore or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setMetastore(GoogleCloudDataplexV1LakeMetastore metastore) {
    this.metastore = metastore;
    return this;
  }

  /**
   * Output only. Metastore status of the lake.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastoreStatus getMetastoreStatus() {
    return metastoreStatus;
  }

  /**
   * Output only. Metastore status of the lake.
   *
   * @param metastoreStatus metastoreStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setMetastoreStatus(
      GoogleCloudDataplexV1LakeMetastoreStatus metastoreStatus) {
    this.metastoreStatus = metastoreStatus;
    return this;
  }

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Optional. Specification of the security policy applied to data in this lake. Typically it
   * should take a few minutes for the security policy to fully propagate.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeSecuritySpec getSecuritySpec() {
    return securitySpec;
  }

  /**
   * Optional. Specification of the security policy applied to data in this lake. Typically it
   * should take a few minutes for the security policy to fully propagate.
   *
   * @param securitySpec securitySpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setSecuritySpec(
      GoogleCloudDataplexV1LakeSecuritySpec securitySpec) {
    this.securitySpec = securitySpec;
    return this;
  }

  /**
   * Output only. Status of the security policy applied to data in this lake.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1SecurityStatus getSecurityStatus() {
    return securityStatus;
  }

  /**
   * Output only. Status of the security policy applied to data in this lake.
   *
   * @param securityStatus securityStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setSecurityStatus(
      GoogleCloudDataplexV1SecurityStatus securityStatus) {
    this.securityStatus = securityStatus;
    return this;
  }

  /**
   * Output only. Service account associated with this lake. This service account must be authorized
   * to access or operate on resources managed by the lake.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getServiceAccount() {
    return serviceAccount;
  }

  /**
   * Output only. Service account associated with this lake. This service account must be authorized
   * to access or operate on resources managed by the lake.
   *
   * @param serviceAccount serviceAccount or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setServiceAccount(java.lang.String serviceAccount) {
    this.serviceAccount = serviceAccount;
    return this;
  }

  /**
   * Output only. Current state of the lake.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Current state of the lake.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the lake. This ID will be different if the
   * lake is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the lake. This ID will be different if the
   * lake is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the lake was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the lake was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Lake setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Lake set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Lake) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Lake clone() {
    return (GoogleCloudDataplexV1Lake) super.clone();
  }
}
