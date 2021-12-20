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
 * Provides information about compatibility with various metadata stores.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EntityCompatibilityStatus
    extends com.google.api.client.json.GenericJson {

  /**
   * Output only. Whether this entity is compatible with BigQuery. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility bigquery;

  /**
   * Output only. Whether this entity is compatible with Hive Metastore. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility hiveMetastore;

  /**
   * Output only. Whether this entity is compatible with BigQuery.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility getBigquery() {
    return bigquery;
  }

  /**
   * Output only. Whether this entity is compatible with BigQuery.
   *
   * @param bigquery bigquery or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibilityStatus setBigquery(
      GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility bigquery) {
    this.bigquery = bigquery;
    return this;
  }

  /**
   * Output only. Whether this entity is compatible with Hive Metastore.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility getHiveMetastore() {
    return hiveMetastore;
  }

  /**
   * Output only. Whether this entity is compatible with Hive Metastore.
   *
   * @param hiveMetastore hiveMetastore or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibilityStatus setHiveMetastore(
      GoogleCloudDataplexV1EntityCompatibilityStatusCompatibility hiveMetastore) {
    this.hiveMetastore = hiveMetastore;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EntityCompatibilityStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1EntityCompatibilityStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EntityCompatibilityStatus clone() {
    return (GoogleCloudDataplexV1EntityCompatibilityStatus) super.clone();
  }
}
