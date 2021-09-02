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
 * Settings to manage metadata publishing to a Hive Metastore from a zone.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore
    extends com.google.api.client.json.GenericJson {

  /**
   * Immutable. The name of the metastore database associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The database is created in the metastore
   * instance associated with the parent lake. The specified name must not already be in use. Upon
   * creation, this field is updated to reflect the actual database name. Maximum length is 128. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String databaseName;

  /** Required. Whether to publish metadata to metastore. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean enabled;

  /**
   * Immutable. The name of the metastore database associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The database is created in the metastore
   * instance associated with the parent lake. The specified name must not already be in use. Upon
   * creation, this field is updated to reflect the actual database name. Maximum length is 128.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDatabaseName() {
    return databaseName;
  }

  /**
   * Immutable. The name of the metastore database associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The database is created in the metastore
   * instance associated with the parent lake. The specified name must not already be in use. Upon
   * creation, this field is updated to reflect the actual database name. Maximum length is 128.
   *
   * @param databaseName databaseName or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore setDatabaseName(
      java.lang.String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  /**
   * Required. Whether to publish metadata to metastore.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getEnabled() {
    return enabled;
  }

  /**
   * Required. Whether to publish metadata to metastore.
   *
   * @param enabled enabled or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore setEnabled(
      java.lang.Boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore)
        super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore clone() {
    return (GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingMetastore) super.clone();
  }
}
