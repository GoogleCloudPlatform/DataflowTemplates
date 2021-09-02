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
 * Action represents an issue requiring administrator action for resolution.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Action extends com.google.api.client.json.GenericJson {

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id} The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String asset;

  /** The category of issue associated with the action. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String category;

  /**
   * The list of data locations associated with this action. Cloud Storage locations are represented
   * as URI paths(E.g. gs://bucket/table1/year=2020/month=Jan/). BigQuery locations refer to
   * resource names(E.g. //bigquery.googleapis.com/projects/project-id/datasets/dataset-id). The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> dataLocations;

  /** The time that the issue was detected. The value may be {@code null}. */
  @com.google.api.client.util.Key private String detectTime;

  /** Details for issues related to applying security policy. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionFailedSecurityPolicyApply failedSecurityPolicyApply;

  /**
   * Details for issues related to incompatible schemas detected within data. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionIncompatibleDataSchema incompatibleDataSchema;

  /**
   * Details for issues related to invalid or unsupported data formats. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidDataFormat invalidDataFormat;

  /** Details for issues related to invalid data arrangement. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidDataOrganization invalidDataOrganization;

  /**
   * Details for issues related to invalid or unsupported data partition structure. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidDataPartition invalidDataPartition;

  /**
   * Details for issues related to invalid discovery configuration files. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidDiscoveryConfig invalidDiscoveryConfig;

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore caused by issues in the
   * metastore service instance, e.g., wrong metastore configuration. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidMetastore invalidMetastore;

  /**
   * Details for issues related to invalid security policy specifications. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionInvalidSecurityPolicy invalidSecurityPolicy;

  /** Detailed description of the issue requiring action. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String issue;

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id} The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String lake;

  /**
   * The list of data locations associated with this action. Paths reflect the underlying storage
   * service. Cloud Storage locations are represented as URI paths. BigQuery locations refer to
   * resource names. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private java.util.List<GoogleCloudDataplexV1ActionLocation> locations;

  /**
   * Details for issues related to absence of data within managed resources. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ActionMissingData missingData;

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore due to a missing
   * metastore service instance. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionMissingMetastore missingMetastore;

  /** Details for issues related to absence of a managed resource. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionMissingResource missingResource;

  /**
   * Output only. The relative resource name of the action, of the form:
   * projects/{project}/locations/{location}/lakes/{lake}/actions/{action}
   * projects/{project}/locations/{location}/lakes/{lake}/zones/{zone}/actions/{action} projects/{pr
   * oject}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}/actions/{action} The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Details for issues related to metadata publishing to BigQuery due to unauthorized errors. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionUnauthorizedDataset unauthorizedDataset;

  /**
   * Details for issues related to lack of permissions to access data resources. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ActionUnauthorizedResource unauthorizedResource;

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id} The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String zone;

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAsset() {
    return asset;
  }

  /**
   * Output only. The relative resource name of the asset, of the form: projects/{project_number}/lo
   * cations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
   *
   * @param asset asset or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setAsset(java.lang.String asset) {
    this.asset = asset;
    return this;
  }

  /**
   * The category of issue associated with the action.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCategory() {
    return category;
  }

  /**
   * The category of issue associated with the action.
   *
   * @param category category or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setCategory(java.lang.String category) {
    this.category = category;
    return this;
  }

  /**
   * The list of data locations associated with this action. Cloud Storage locations are represented
   * as URI paths(E.g. gs://bucket/table1/year=2020/month=Jan/). BigQuery locations refer to
   * resource names(E.g. //bigquery.googleapis.com/projects/project-id/datasets/dataset-id).
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getDataLocations() {
    return dataLocations;
  }

  /**
   * The list of data locations associated with this action. Cloud Storage locations are represented
   * as URI paths(E.g. gs://bucket/table1/year=2020/month=Jan/). BigQuery locations refer to
   * resource names(E.g. //bigquery.googleapis.com/projects/project-id/datasets/dataset-id).
   *
   * @param dataLocations dataLocations or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setDataLocations(
      java.util.List<java.lang.String> dataLocations) {
    this.dataLocations = dataLocations;
    return this;
  }

  /**
   * The time that the issue was detected.
   *
   * @return value or {@code null} for none
   */
  public String getDetectTime() {
    return detectTime;
  }

  /**
   * The time that the issue was detected.
   *
   * @param detectTime detectTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setDetectTime(String detectTime) {
    this.detectTime = detectTime;
    return this;
  }

  /**
   * Details for issues related to applying security policy.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionFailedSecurityPolicyApply getFailedSecurityPolicyApply() {
    return failedSecurityPolicyApply;
  }

  /**
   * Details for issues related to applying security policy.
   *
   * @param failedSecurityPolicyApply failedSecurityPolicyApply or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setFailedSecurityPolicyApply(
      GoogleCloudDataplexV1ActionFailedSecurityPolicyApply failedSecurityPolicyApply) {
    this.failedSecurityPolicyApply = failedSecurityPolicyApply;
    return this;
  }

  /**
   * Details for issues related to incompatible schemas detected within data.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema getIncompatibleDataSchema() {
    return incompatibleDataSchema;
  }

  /**
   * Details for issues related to incompatible schemas detected within data.
   *
   * @param incompatibleDataSchema incompatibleDataSchema or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setIncompatibleDataSchema(
      GoogleCloudDataplexV1ActionIncompatibleDataSchema incompatibleDataSchema) {
    this.incompatibleDataSchema = incompatibleDataSchema;
    return this;
  }

  /**
   * Details for issues related to invalid or unsupported data formats.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidDataFormat getInvalidDataFormat() {
    return invalidDataFormat;
  }

  /**
   * Details for issues related to invalid or unsupported data formats.
   *
   * @param invalidDataFormat invalidDataFormat or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidDataFormat(
      GoogleCloudDataplexV1ActionInvalidDataFormat invalidDataFormat) {
    this.invalidDataFormat = invalidDataFormat;
    return this;
  }

  /**
   * Details for issues related to invalid data arrangement.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidDataOrganization getInvalidDataOrganization() {
    return invalidDataOrganization;
  }

  /**
   * Details for issues related to invalid data arrangement.
   *
   * @param invalidDataOrganization invalidDataOrganization or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidDataOrganization(
      GoogleCloudDataplexV1ActionInvalidDataOrganization invalidDataOrganization) {
    this.invalidDataOrganization = invalidDataOrganization;
    return this;
  }

  /**
   * Details for issues related to invalid or unsupported data partition structure.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidDataPartition getInvalidDataPartition() {
    return invalidDataPartition;
  }

  /**
   * Details for issues related to invalid or unsupported data partition structure.
   *
   * @param invalidDataPartition invalidDataPartition or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidDataPartition(
      GoogleCloudDataplexV1ActionInvalidDataPartition invalidDataPartition) {
    this.invalidDataPartition = invalidDataPartition;
    return this;
  }

  /**
   * Details for issues related to invalid discovery configuration files.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidDiscoveryConfig getInvalidDiscoveryConfig() {
    return invalidDiscoveryConfig;
  }

  /**
   * Details for issues related to invalid discovery configuration files.
   *
   * @param invalidDiscoveryConfig invalidDiscoveryConfig or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidDiscoveryConfig(
      GoogleCloudDataplexV1ActionInvalidDiscoveryConfig invalidDiscoveryConfig) {
    this.invalidDiscoveryConfig = invalidDiscoveryConfig;
    return this;
  }

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore caused by issues in the
   * metastore service instance, e.g., wrong metastore configuration.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidMetastore getInvalidMetastore() {
    return invalidMetastore;
  }

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore caused by issues in the
   * metastore service instance, e.g., wrong metastore configuration.
   *
   * @param invalidMetastore invalidMetastore or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidMetastore(
      GoogleCloudDataplexV1ActionInvalidMetastore invalidMetastore) {
    this.invalidMetastore = invalidMetastore;
    return this;
  }

  /**
   * Details for issues related to invalid security policy specifications.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionInvalidSecurityPolicy getInvalidSecurityPolicy() {
    return invalidSecurityPolicy;
  }

  /**
   * Details for issues related to invalid security policy specifications.
   *
   * @param invalidSecurityPolicy invalidSecurityPolicy or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setInvalidSecurityPolicy(
      GoogleCloudDataplexV1ActionInvalidSecurityPolicy invalidSecurityPolicy) {
    this.invalidSecurityPolicy = invalidSecurityPolicy;
    return this;
  }

  /**
   * Detailed description of the issue requiring action.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getIssue() {
    return issue;
  }

  /**
   * Detailed description of the issue requiring action.
   *
   * @param issue issue or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setIssue(java.lang.String issue) {
    this.issue = issue;
    return this;
  }

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLake() {
    return lake;
  }

  /**
   * Output only. The relative resource name of the lake, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
   *
   * @param lake lake or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setLake(java.lang.String lake) {
    this.lake = lake;
    return this;
  }

  /**
   * The list of data locations associated with this action. Paths reflect the underlying storage
   * service. Cloud Storage locations are represented as URI paths. BigQuery locations refer to
   * resource names.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1ActionLocation> getLocations() {
    return locations;
  }

  /**
   * The list of data locations associated with this action. Paths reflect the underlying storage
   * service. Cloud Storage locations are represented as URI paths. BigQuery locations refer to
   * resource names.
   *
   * @param locations locations or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setLocations(
      java.util.List<GoogleCloudDataplexV1ActionLocation> locations) {
    this.locations = locations;
    return this;
  }

  /**
   * Details for issues related to absence of data within managed resources.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionMissingData getMissingData() {
    return missingData;
  }

  /**
   * Details for issues related to absence of data within managed resources.
   *
   * @param missingData missingData or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setMissingData(
      GoogleCloudDataplexV1ActionMissingData missingData) {
    this.missingData = missingData;
    return this;
  }

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore due to a missing
   * metastore service instance.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionMissingMetastore getMissingMetastore() {
    return missingMetastore;
  }

  /**
   * Details for issues related to metadata publishing to Dataproc Metastore due to a missing
   * metastore service instance.
   *
   * @param missingMetastore missingMetastore or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setMissingMetastore(
      GoogleCloudDataplexV1ActionMissingMetastore missingMetastore) {
    this.missingMetastore = missingMetastore;
    return this;
  }

  /**
   * Details for issues related to absence of a managed resource.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionMissingResource getMissingResource() {
    return missingResource;
  }

  /**
   * Details for issues related to absence of a managed resource.
   *
   * @param missingResource missingResource or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setMissingResource(
      GoogleCloudDataplexV1ActionMissingResource missingResource) {
    this.missingResource = missingResource;
    return this;
  }

  /**
   * Output only. The relative resource name of the action, of the form:
   * projects/{project}/locations/{location}/lakes/{lake}/actions/{action}
   * projects/{project}/locations/{location}/lakes/{lake}/zones/{zone}/actions/{action} projects/{pr
   * oject}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}/actions/{action}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the action, of the form:
   * projects/{project}/locations/{location}/lakes/{lake}/actions/{action}
   * projects/{project}/locations/{location}/lakes/{lake}/zones/{zone}/actions/{action} projects/{pr
   * oject}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}/actions/{action}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Details for issues related to metadata publishing to BigQuery due to unauthorized errors.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionUnauthorizedDataset getUnauthorizedDataset() {
    return unauthorizedDataset;
  }

  /**
   * Details for issues related to metadata publishing to BigQuery due to unauthorized errors.
   *
   * @param unauthorizedDataset unauthorizedDataset or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setUnauthorizedDataset(
      GoogleCloudDataplexV1ActionUnauthorizedDataset unauthorizedDataset) {
    this.unauthorizedDataset = unauthorizedDataset;
    return this;
  }

  /**
   * Details for issues related to lack of permissions to access data resources.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionUnauthorizedResource getUnauthorizedResource() {
    return unauthorizedResource;
  }

  /**
   * Details for issues related to lack of permissions to access data resources.
   *
   * @param unauthorizedResource unauthorizedResource or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setUnauthorizedResource(
      GoogleCloudDataplexV1ActionUnauthorizedResource unauthorizedResource) {
    this.unauthorizedResource = unauthorizedResource;
    return this;
  }

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getZone() {
    return zone;
  }

  /**
   * Output only. The relative resource name of the zone, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   *
   * @param zone zone or {@code null} for none
   */
  public GoogleCloudDataplexV1Action setZone(java.lang.String zone) {
    this.zone = zone;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Action set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Action) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Action clone() {
    return (GoogleCloudDataplexV1Action) super.clone();
  }
}
