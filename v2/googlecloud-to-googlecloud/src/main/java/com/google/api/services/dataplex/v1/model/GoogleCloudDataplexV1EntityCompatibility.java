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
 * Metadata stores the entity is compatible with.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EntityCompatibility
    extends com.google.api.client.json.GenericJson {

  /**
   * Output only. Whether the entity can be represented in BigQuery. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean bigquery;

  /**
   * Output only. Whether the entity can be represented in Hive Metastore. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean hiveMetastore;

  /**
   * Output only. Whether the entity can be represented in BigQuery.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getBigquery() {
    return bigquery;
  }

  /**
   * Output only. Whether the entity can be represented in BigQuery.
   *
   * @param bigquery bigquery or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibility setBigquery(java.lang.Boolean bigquery) {
    this.bigquery = bigquery;
    return this;
  }

  /**
   * Output only. Whether the entity can be represented in Hive Metastore.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getHiveMetastore() {
    return hiveMetastore;
  }

  /**
   * Output only. Whether the entity can be represented in Hive Metastore.
   *
   * @param hiveMetastore hiveMetastore or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibility setHiveMetastore(
      java.lang.Boolean hiveMetastore) {
    this.hiveMetastore = hiveMetastore;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EntityCompatibility set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1EntityCompatibility) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EntityCompatibility clone() {
    return (GoogleCloudDataplexV1EntityCompatibility) super.clone();
  }
}
