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
 * Status of Lake and Dataproc Metastore service instance association.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1LakeMetastoreStatus
    extends com.google.api.client.json.GenericJson {

  /** Additional information about the current status. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** Current state of association. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Last update time of the metastore status of the lake. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Additional information about the current status.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Additional information about the current status.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastoreStatus setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * Current state of association.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Current state of association.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastoreStatus setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Last update time of the metastore status of the lake.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Last update time of the metastore status of the lake.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastoreStatus setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1LakeMetastoreStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1LakeMetastoreStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1LakeMetastoreStatus clone() {
    return (GoogleCloudDataplexV1LakeMetastoreStatus) super.clone();
  }
}
