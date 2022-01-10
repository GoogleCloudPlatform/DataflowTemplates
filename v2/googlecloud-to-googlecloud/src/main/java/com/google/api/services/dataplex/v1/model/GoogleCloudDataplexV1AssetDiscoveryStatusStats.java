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
 * The aggregated data statistics for the asset reported by discovery. The data is collected from
 * the last run of the discovery jobs.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetDiscoveryStatusStats
    extends com.google.api.client.json.GenericJson {

  /** The count of data items within the referenced resource. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataItems;

  /**
   * The latest update timestamp of data in the attached resource (e.g. Cloud Storage objects, or
   * BigQuery tables). The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String dataRangeEndTime;

  /**
   * The earilest creation timestamp of data in the attached resource (e.g. Cloud Storage objects,
   * or BigQuery tables). The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String dataRangeStartTime;

  /**
   * The number of stored data bytes within the referenced resource. The value may be {@code null}.
   */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataSize;

  /**
   * The count of fileset entities within the referenced resource. The value may be {@code null}.
   */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long filesets;

  /** The count of table entities within the referenced resource. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long tables;

  /**
   * The count of data items within the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataItems() {
    return dataItems;
  }

  /**
   * The count of data items within the referenced resource.
   *
   * @param dataItems dataItems or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setDataItems(java.lang.Long dataItems) {
    this.dataItems = dataItems;
    return this;
  }

  /**
   * The latest update timestamp of data in the attached resource (e.g. Cloud Storage objects, or
   * BigQuery tables).
   *
   * @return value or {@code null} for none
   */
  public String getDataRangeEndTime() {
    return dataRangeEndTime;
  }

  /**
   * The latest update timestamp of data in the attached resource (e.g. Cloud Storage objects, or
   * BigQuery tables).
   *
   * @param dataRangeEndTime dataRangeEndTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setDataRangeEndTime(
      String dataRangeEndTime) {
    this.dataRangeEndTime = dataRangeEndTime;
    return this;
  }

  /**
   * The earilest creation timestamp of data in the attached resource (e.g. Cloud Storage objects,
   * or BigQuery tables).
   *
   * @return value or {@code null} for none
   */
  public String getDataRangeStartTime() {
    return dataRangeStartTime;
  }

  /**
   * The earilest creation timestamp of data in the attached resource (e.g. Cloud Storage objects,
   * or BigQuery tables).
   *
   * @param dataRangeStartTime dataRangeStartTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setDataRangeStartTime(
      String dataRangeStartTime) {
    this.dataRangeStartTime = dataRangeStartTime;
    return this;
  }

  /**
   * The number of stored data bytes within the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataSize() {
    return dataSize;
  }

  /**
   * The number of stored data bytes within the referenced resource.
   *
   * @param dataSize dataSize or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setDataSize(java.lang.Long dataSize) {
    this.dataSize = dataSize;
    return this;
  }

  /**
   * The count of fileset entities within the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getFilesets() {
    return filesets;
  }

  /**
   * The count of fileset entities within the referenced resource.
   *
   * @param filesets filesets or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setFilesets(java.lang.Long filesets) {
    this.filesets = filesets;
    return this;
  }

  /**
   * The count of table entities within the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getTables() {
    return tables;
  }

  /**
   * The count of table entities within the referenced resource.
   *
   * @param tables tables or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats setTables(java.lang.Long tables) {
    this.tables = tables;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetDiscoveryStatusStats) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats clone() {
    return (GoogleCloudDataplexV1AssetDiscoveryStatusStats) super.clone();
  }
}
