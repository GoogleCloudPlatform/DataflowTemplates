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
 * Details about discovery jobs.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1JobEventDiscoveryDetails
    extends com.google.api.client.json.GenericJson {

  /** Whether the job resulted in actions on the asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean actionsDetected;

  /** The id of the associated asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String assetId;

  /** The number of data items within the asset that were read. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataItemsRead;

  /** The number of metadata events logged in this job. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer metadataEventCount;

  /** The id of the associated zone. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String zoneId;

  /**
   * Whether the job resulted in actions on the asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getActionsDetected() {
    return actionsDetected;
  }

  /**
   * Whether the job resulted in actions on the asset.
   *
   * @param actionsDetected actionsDetected or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails setActionsDetected(
      java.lang.Boolean actionsDetected) {
    this.actionsDetected = actionsDetected;
    return this;
  }

  /**
   * The id of the associated asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAssetId() {
    return assetId;
  }

  /**
   * The id of the associated asset.
   *
   * @param assetId assetId or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails setAssetId(java.lang.String assetId) {
    this.assetId = assetId;
    return this;
  }

  /**
   * The number of data items within the asset that were read.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataItemsRead() {
    return dataItemsRead;
  }

  /**
   * The number of data items within the asset that were read.
   *
   * @param dataItemsRead dataItemsRead or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails setDataItemsRead(
      java.lang.Long dataItemsRead) {
    this.dataItemsRead = dataItemsRead;
    return this;
  }

  /**
   * The number of metadata events logged in this job.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getMetadataEventCount() {
    return metadataEventCount;
  }

  /**
   * The number of metadata events logged in this job.
   *
   * @param metadataEventCount metadataEventCount or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails setMetadataEventCount(
      java.lang.Integer metadataEventCount) {
    this.metadataEventCount = metadataEventCount;
    return this;
  }

  /**
   * The id of the associated zone.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getZoneId() {
    return zoneId;
  }

  /**
   * The id of the associated zone.
   *
   * @param zoneId zoneId or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails setZoneId(java.lang.String zoneId) {
    this.zoneId = zoneId;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1JobEventDiscoveryDetails set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1JobEventDiscoveryDetails) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1JobEventDiscoveryDetails clone() {
    return (GoogleCloudDataplexV1JobEventDiscoveryDetails) super.clone();
  }
}
