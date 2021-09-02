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
 * Settings to manage metadata publishing to BigQuery from a zone.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery
    extends com.google.api.client.json.GenericJson {

  /**
   * Immutable. The name of the BigQuery dataset associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The dataset is created in the project
   * associated with the parent lake. The specified name must not already be in use. Upon creation,
   * this field is updated to reflect the actual dataset name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String datasetName;

  /** Required. Whether to publish metadata to BigQuery. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean enabled;

  /**
   * Immutable. The name of the BigQuery dataset associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The dataset is created in the project
   * associated with the parent lake. The specified name must not already be in use. Upon creation,
   * this field is updated to reflect the actual dataset name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDatasetName() {
    return datasetName;
  }

  /**
   * Immutable. The name of the BigQuery dataset associated with the zone. The specified value is
   * interpreted as a name template that can refer to ${lake_id} and ${zone_id} placeholders. If
   * unspecified, this defaults to "${lake_id}_${zone_id}". The dataset is created in the project
   * associated with the parent lake. The specified name must not already be in use. Upon creation,
   * this field is updated to reflect the actual dataset name.
   *
   * @param datasetName datasetName or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery setDatasetName(
      java.lang.String datasetName) {
    this.datasetName = datasetName;
    return this;
  }

  /**
   * Required. Whether to publish metadata to BigQuery.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getEnabled() {
    return enabled;
  }

  /**
   * Required. Whether to publish metadata to BigQuery.
   *
   * @param enabled enabled or {@code null} for none
   */
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery setEnabled(
      java.lang.Boolean enabled) {
    this.enabled = enabled;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery)
        super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery clone() {
    return (GoogleCloudDataplexV1ZoneDiscoverySpecMetadataPublishingBigQuery) super.clone();
  }
}
