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
 * A resource that represents Google Cloud Platform location.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudLocationLocation extends com.google.api.client.json.GenericJson {

  /**
   * The friendly name for this location, typically a nearby city name. For example, "Tokyo". The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /**
   * Cross-service attributes for the location. For example {"cloud.googleapis.com/region": "us-
   * east1"} The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** The canonical id for this location. For example: "us-east1". The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String locationId;

  /**
   * Service-specific metadata. For example the available capacity at the given location. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.Object> metadata;

  /**
   * Resource name for the location, which may vary between implementations. For example: "projects
   * /example-project/locations/us-east1" The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * The friendly name for this location, typically a nearby city name. For example, "Tokyo".
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * The friendly name for this location, typically a nearby city name. For example, "Tokyo".
   *
   * @param displayName displayName or {@code null} for none
   */
  public GoogleCloudLocationLocation setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Cross-service attributes for the location. For example {"cloud.googleapis.com/region": "us-
   * east1"}
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Cross-service attributes for the location. For example {"cloud.googleapis.com/region": "us-
   * east1"}
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudLocationLocation setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * The canonical id for this location. For example: "us-east1".
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLocationId() {
    return locationId;
  }

  /**
   * The canonical id for this location. For example: "us-east1".
   *
   * @param locationId locationId or {@code null} for none
   */
  public GoogleCloudLocationLocation setLocationId(java.lang.String locationId) {
    this.locationId = locationId;
    return this;
  }

  /**
   * Service-specific metadata. For example the available capacity at the given location.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.Object> getMetadata() {
    return metadata;
  }

  /**
   * Service-specific metadata. For example the available capacity at the given location.
   *
   * @param metadata metadata or {@code null} for none
   */
  public GoogleCloudLocationLocation setMetadata(java.util.Map<String, java.lang.Object> metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Resource name for the location, which may vary between implementations. For example: "projects
   * /example-project/locations/us-east1"
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Resource name for the location, which may vary between implementations. For example: "projects
   * /example-project/locations/us-east1"
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudLocationLocation setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  @Override
  public GoogleCloudLocationLocation set(String fieldName, Object value) {
    return (GoogleCloudLocationLocation) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudLocationLocation clone() {
    return (GoogleCloudLocationLocation) super.clone();
  }
}
