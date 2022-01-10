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
 * Status of the security policy specified on lakes & zones.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1SecurityStatus
    extends com.google.api.client.json.GenericJson {

  /** Additional information about the current state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** The current state of the security policy applied to data. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Last update time of the status. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Additional information about the current state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Additional information about the current state.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1SecurityStatus setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The current state of the security policy applied to data.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * The current state of the security policy applied to data.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1SecurityStatus setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Last update time of the status.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Last update time of the status.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1SecurityStatus setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1SecurityStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1SecurityStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1SecurityStatus clone() {
    return (GoogleCloudDataplexV1SecurityStatus) super.clone();
  }
}
