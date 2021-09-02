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
 * Settings to manage the metadata discovery and publishing for an asset.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetDiscoverySpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. Whether discovery is enabled. When inheritance_mode is set to INHERIT this field is
   * unset and ignored. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean enabled;

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names. When
   * inheritance_mode is set to INHERIT this field is unset and ignored. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> excludePatterns;

  /**
   * Optional. The list of patterns to apply for selecting data to include during discovery if only
   * a subset of the data should considered. For Cloud Storage bucket assets, these are interpreted
   * as glob patterns used to match object names. For BigQuery dataset assets, these are interpreted
   * as patterns to match table names. When inheritance_mode is set to INHERIT this field is unset
   * and ignored. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> includePatterns;

  /**
   * Optional. Inheritance behavior for this config. By default, all fields in this config override
   * any values specified at the zone level. When configured as INHERIT some fields in this config
   * are ignored and the zone level configuration is used instead. All fields that behave this way
   * are called out as such via documentation. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String inheritanceMode;

  /**
   * Optional. Settings to manage metadata publishing for the zone. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing publishing;

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String schedule;

  /**
   * Optional. Whether discovery is enabled. When inheritance_mode is set to INHERIT this field is
   * unset and ignored.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getEnabled() {
    return enabled;
  }

  /**
   * Optional. Whether discovery is enabled. When inheritance_mode is set to INHERIT this field is
   * unset and ignored.
   *
   * @param enabled enabled or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setEnabled(java.lang.Boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names. When
   * inheritance_mode is set to INHERIT this field is unset and ignored.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getExcludePatterns() {
    return excludePatterns;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names. When
   * inheritance_mode is set to INHERIT this field is unset and ignored.
   *
   * @param excludePatterns excludePatterns or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setExcludePatterns(
      java.util.List<java.lang.String> excludePatterns) {
    this.excludePatterns = excludePatterns;
    return this;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to include during discovery if only
   * a subset of the data should considered. For Cloud Storage bucket assets, these are interpreted
   * as glob patterns used to match object names. For BigQuery dataset assets, these are interpreted
   * as patterns to match table names. When inheritance_mode is set to INHERIT this field is unset
   * and ignored.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getIncludePatterns() {
    return includePatterns;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to include during discovery if only
   * a subset of the data should considered. For Cloud Storage bucket assets, these are interpreted
   * as glob patterns used to match object names. For BigQuery dataset assets, these are interpreted
   * as patterns to match table names. When inheritance_mode is set to INHERIT this field is unset
   * and ignored.
   *
   * @param includePatterns includePatterns or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setIncludePatterns(
      java.util.List<java.lang.String> includePatterns) {
    this.includePatterns = includePatterns;
    return this;
  }

  /**
   * Optional. Inheritance behavior for this config. By default, all fields in this config override
   * any values specified at the zone level. When configured as INHERIT some fields in this config
   * are ignored and the zone level configuration is used instead. All fields that behave this way
   * are called out as such via documentation.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getInheritanceMode() {
    return inheritanceMode;
  }

  /**
   * Optional. Inheritance behavior for this config. By default, all fields in this config override
   * any values specified at the zone level. When configured as INHERIT some fields in this config
   * are ignored and the zone level configuration is used instead. All fields that behave this way
   * are called out as such via documentation.
   *
   * @param inheritanceMode inheritanceMode or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setInheritanceMode(
      java.lang.String inheritanceMode) {
    this.inheritanceMode = inheritanceMode;
    return this;
  }

  /**
   * Optional. Settings to manage metadata publishing for the zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing getPublishing() {
    return publishing;
  }

  /**
   * Optional. Settings to manage metadata publishing for the zone.
   *
   * @param publishing publishing or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setPublishing(
      GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing publishing) {
    this.publishing = publishing;
    return this;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchedule() {
    return schedule;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart.
   *
   * @param schedule schedule or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpec setSchedule(java.lang.String schedule) {
    this.schedule = schedule;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoverySpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetDiscoverySpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoverySpec clone() {
    return (GoogleCloudDataplexV1AssetDiscoverySpec) super.clone();
  }
}
