/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1.model;

/**
 * The configuration of the stream destination.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class DestinationConfig extends com.google.api.client.json.GenericJson {

  /**
   * Required. Destination connection profile resource. Format:
   * `projects/{project}/locations/{location}/connectionProfiles/{name}` The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String destinationConnectionProfile;

  /**
   * A configuration for how data should be loaded to Cloud Storage. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GcsDestinationConfig gcsDestinationConfig;

  /**
   * Required. Destination connection profile resource. Format:
   * `projects/{project}/locations/{location}/connectionProfiles/{name}`
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDestinationConnectionProfile() {
    return destinationConnectionProfile;
  }

  /**
   * Required. Destination connection profile resource. Format:
   * `projects/{project}/locations/{location}/connectionProfiles/{name}`
   *
   * @param destinationConnectionProfile destinationConnectionProfile or {@code null} for none
   */
  public DestinationConfig setDestinationConnectionProfile(
      java.lang.String destinationConnectionProfile) {
    this.destinationConnectionProfile = destinationConnectionProfile;
    return this;
  }

  /**
   * A configuration for how data should be loaded to Cloud Storage.
   *
   * @return value or {@code null} for none
   */
  public GcsDestinationConfig getGcsDestinationConfig() {
    return gcsDestinationConfig;
  }

  /**
   * A configuration for how data should be loaded to Cloud Storage.
   *
   * @param gcsDestinationConfig gcsDestinationConfig or {@code null} for none
   */
  public DestinationConfig setGcsDestinationConfig(GcsDestinationConfig gcsDestinationConfig) {
    this.gcsDestinationConfig = gcsDestinationConfig;
    return this;
  }

  @Override
  public DestinationConfig set(String fieldName, Object value) {
    return (DestinationConfig) super.set(fieldName, value);
  }

  @Override
  public DestinationConfig clone() {
    return (DestinationConfig) super.clone();
  }
}
