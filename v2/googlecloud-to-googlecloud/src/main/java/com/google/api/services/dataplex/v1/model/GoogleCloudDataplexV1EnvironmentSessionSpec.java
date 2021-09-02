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
 * Model definition for GoogleCloudDataplexV1EnvironmentSessionSpec.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EnvironmentSessionSpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The idle time configuration of the session. The session will be auto-terminated at
   * the end of this period. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String maxIdleDuration;

  /**
   * Optional. The idle time configuration of the session. The session will be auto-terminated at
   * the end of this period.
   *
   * @return value or {@code null} for none
   */
  public String getMaxIdleDuration() {
    return maxIdleDuration;
  }

  /**
   * Optional. The idle time configuration of the session. The session will be auto-terminated at
   * the end of this period.
   *
   * @param maxIdleDuration maxIdleDuration or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentSessionSpec setMaxIdleDuration(String maxIdleDuration) {
    this.maxIdleDuration = maxIdleDuration;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentSessionSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1EnvironmentSessionSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentSessionSpec clone() {
    return (GoogleCloudDataplexV1EnvironmentSessionSpec) super.clone();
  }
}
