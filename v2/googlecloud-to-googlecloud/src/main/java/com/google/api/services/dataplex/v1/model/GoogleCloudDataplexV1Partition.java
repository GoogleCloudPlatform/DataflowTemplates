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
 * Represents partition metadata contained within Entity instances.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Partition extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The etag for this partitioin. Required for update requests, it must match the
   * server's etag. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String etag;

  /**
   * Required. Immutable. The location of the entity data within the partition. Eg.
   * gs://bucket/path/to/entity/key1=value1/key2=value2 The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String location;

  /**
   * Output only. The resource name of the entity, of the form: projects/{project_number}/locations/
   * {location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition} The
   * {partition} is a generated unique ID. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Required. Immutable. The set of values representing the partition. These correspond to the
   * partition schema defined in the parent entity. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> values;

  /**
   * Optional. The etag for this partitioin. Required for update requests, it must match the
   * server's etag.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEtag() {
    return etag;
  }

  /**
   * Optional. The etag for this partitioin. Required for update requests, it must match the
   * server's etag.
   *
   * @param etag etag or {@code null} for none
   */
  public GoogleCloudDataplexV1Partition setEtag(java.lang.String etag) {
    this.etag = etag;
    return this;
  }

  /**
   * Required. Immutable. The location of the entity data within the partition. Eg.
   * gs://bucket/path/to/entity/key1=value1/key2=value2
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLocation() {
    return location;
  }

  /**
   * Required. Immutable. The location of the entity data within the partition. Eg.
   * gs://bucket/path/to/entity/key1=value1/key2=value2
   *
   * @param location location or {@code null} for none
   */
  public GoogleCloudDataplexV1Partition setLocation(java.lang.String location) {
    this.location = location;
    return this;
  }

  /**
   * Output only. The resource name of the entity, of the form: projects/{project_number}/locations/
   * {location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition} The
   * {partition} is a generated unique ID.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The resource name of the entity, of the form: projects/{project_number}/locations/
   * {location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition} The
   * {partition} is a generated unique ID.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Partition setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. Immutable. The set of values representing the partition. These correspond to the
   * partition schema defined in the parent entity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getValues() {
    return values;
  }

  /**
   * Required. Immutable. The set of values representing the partition. These correspond to the
   * partition schema defined in the parent entity.
   *
   * @param values values or {@code null} for none
   */
  public GoogleCloudDataplexV1Partition setValues(java.util.List<java.lang.String> values) {
    this.values = values;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Partition set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Partition) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Partition clone() {
    return (GoogleCloudDataplexV1Partition) super.clone();
  }
}
