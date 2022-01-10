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
 * Model definition for GoogleCloudDataplexV1EnvironmentEndpoints.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EnvironmentEndpoints
    extends com.google.api.client.json.GenericJson {

  /** Output only. URI to serve notebook APIs The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String notebooks;

  /** Output only. URI to serve SQL APIs The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String sql;

  /**
   * Output only. URI to serve notebook APIs
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNotebooks() {
    return notebooks;
  }

  /**
   * Output only. URI to serve notebook APIs
   *
   * @param notebooks notebooks or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentEndpoints setNotebooks(java.lang.String notebooks) {
    this.notebooks = notebooks;
    return this;
  }

  /**
   * Output only. URI to serve SQL APIs
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSql() {
    return sql;
  }

  /**
   * Output only. URI to serve SQL APIs
   *
   * @param sql sql or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentEndpoints setSql(java.lang.String sql) {
    this.sql = sql;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentEndpoints set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1EnvironmentEndpoints) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentEndpoints clone() {
    return (GoogleCloudDataplexV1EnvironmentEndpoints) super.clone();
  }
}
