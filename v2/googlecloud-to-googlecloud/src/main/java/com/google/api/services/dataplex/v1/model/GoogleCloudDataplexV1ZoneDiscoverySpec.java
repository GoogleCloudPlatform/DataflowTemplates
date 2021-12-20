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
 * Settings to manage the metadata discovery and publishing in a zone.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ZoneDiscoverySpec
    extends com.google.api.client.json.GenericJson {

  /** Required. Whether discovery is enabled. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean enabled;

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> excludePatterns;

  /**
   * Optional. The list of patterns to apply for selecting data to include during discovery if only
   * a subset of the data should considered. For Cloud Storage bucket assets, these are interpreted
   * as glob patterns used to match object names. For BigQuery dataset assets, these are interpreted
   * as patterns to match table names. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> includePatterns;

  /**
   * Optional. Settings to manage metadata publishing from the zone. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishing publishing;

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart. To explicitly set a
   * timezone to the cron tab, apply a prefix in the cron tab: "CRON_TZ=${IANA_TIME_ZONE}" or
   * "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a valid string from IANA time
   * zone database. For example, "CRON_TZ=America/New_York 1 * * * *", or "TZ=America/New_York 1 * *
   * * *". The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String schedule;

  /**
   * Required. Whether discovery is enabled.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getEnabled() {
    return enabled;
  }

  /**
   * Required. Whether discovery is enabled.
   *
   * @param enabled enabled or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec setEnabled(java.lang.Boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getExcludePatterns() {
    return excludePatterns;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to exclude during discovery. For
   * Cloud Storage bucket assets, these are interpreted as glob patterns used to match object names.
   * For BigQuery dataset assets, these are interpreted as patterns to match table names.
   *
   * @param excludePatterns excludePatterns or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec setExcludePatterns(
      java.util.List<java.lang.String> excludePatterns) {
    this.excludePatterns = excludePatterns;
    return this;
  }

  /**
   * Optional. The list of patterns to apply for selecting data to include during discovery if only
   * a subset of the data should considered. For Cloud Storage bucket assets, these are interpreted
   * as glob patterns used to match object names. For BigQuery dataset assets, these are interpreted
   * as patterns to match table names.
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
   * as patterns to match table names.
   *
   * @param includePatterns includePatterns or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec setIncludePatterns(
      java.util.List<java.lang.String> includePatterns) {
    this.includePatterns = includePatterns;
    return this;
  }

  /**
   * Optional. Settings to manage metadata publishing from the zone.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishing getPublishing() {
    return publishing;
  }

  /**
   * Optional. Settings to manage metadata publishing from the zone.
   *
   * @param publishing publishing or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec setPublishing(
      GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishing publishing) {
    this.publishing = publishing;
    return this;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart. To explicitly set a
   * timezone to the cron tab, apply a prefix in the cron tab: "CRON_TZ=${IANA_TIME_ZONE}" or
   * "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a valid string from IANA time
   * zone database. For example, "CRON_TZ=America/New_York 1 * * * *", or "TZ=America/New_York 1 * *
   * * *".
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchedule() {
    return schedule;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running discovery jobs
   * periodically. Discovery jobs must be scheduled at least 30 minutes apart. To explicitly set a
   * timezone to the cron tab, apply a prefix in the cron tab: "CRON_TZ=${IANA_TIME_ZONE}" or
   * "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a valid string from IANA time
   * zone database. For example, "CRON_TZ=America/New_York 1 * * * *", or "TZ=America/New_York 1 * *
   * * *".
   *
   * @param schedule schedule or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpec setSchedule(java.lang.String schedule) {
    this.schedule = schedule;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ZoneDiscoverySpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpec clone() {
    return (GoogleCloudDataplexV1ZoneDiscoverySpec) super.clone();
  }
}
