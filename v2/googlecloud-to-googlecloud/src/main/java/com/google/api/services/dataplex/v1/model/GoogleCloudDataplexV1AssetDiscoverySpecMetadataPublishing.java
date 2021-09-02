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
 * Settings to manage metadata publishing for an asset.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing
    extends com.google.api.client.json.GenericJson {

  /**
   * Immutable. The prefix for fileset names. If provided, fileset names are prefixed with the
   * specified value. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String filesetNamePrefix;

  /**
   * Immutable. The prefix for table names. If provided, table names are prefixed with the specified
   * value. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String tableNamePrefix;

  /**
   * Immutable. The prefix for fileset names. If provided, fileset names are prefixed with the
   * specified value.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getFilesetNamePrefix() {
    return filesetNamePrefix;
  }

  /**
   * Immutable. The prefix for fileset names. If provided, fileset names are prefixed with the
   * specified value.
   *
   * @param filesetNamePrefix filesetNamePrefix or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing setFilesetNamePrefix(
      java.lang.String filesetNamePrefix) {
    this.filesetNamePrefix = filesetNamePrefix;
    return this;
  }

  /**
   * Immutable. The prefix for table names. If provided, table names are prefixed with the specified
   * value.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTableNamePrefix() {
    return tableNamePrefix;
  }

  /**
   * Immutable. The prefix for table names. If provided, table names are prefixed with the specified
   * value.
   *
   * @param tableNamePrefix tableNamePrefix or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing setTableNamePrefix(
      java.lang.String tableNamePrefix) {
    this.tableNamePrefix = tableNamePrefix;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing clone() {
    return (GoogleCloudDataplexV1AssetDiscoverySpecMetadataPublishing) super.clone();
  }
}
