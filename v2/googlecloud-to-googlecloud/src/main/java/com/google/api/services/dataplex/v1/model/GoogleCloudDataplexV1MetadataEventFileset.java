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
 * Details about fileset entities being created, updated or deleted.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1MetadataEventFileset
    extends com.google.api.client.json.GenericJson {

  /** The format of the data within the fileset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String dataFormat;

  /** The number of data items in the fileset. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataItemsCount;

  /** The data location of the fileset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String location;

  /** The name of the fileset entity. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * The format of the data within the fileset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataFormat() {
    return dataFormat;
  }

  /**
   * The format of the data within the fileset.
   *
   * @param dataFormat dataFormat or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventFileset setDataFormat(java.lang.String dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  /**
   * The number of data items in the fileset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataItemsCount() {
    return dataItemsCount;
  }

  /**
   * The number of data items in the fileset.
   *
   * @param dataItemsCount dataItemsCount or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventFileset setDataItemsCount(
      java.lang.Long dataItemsCount) {
    this.dataItemsCount = dataItemsCount;
    return this;
  }

  /**
   * The data location of the fileset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLocation() {
    return location;
  }

  /**
   * The data location of the fileset.
   *
   * @param location location or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventFileset setLocation(java.lang.String location) {
    this.location = location;
    return this;
  }

  /**
   * The name of the fileset entity.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * The name of the fileset entity.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventFileset setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventFileset set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1MetadataEventFileset) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventFileset clone() {
    return (GoogleCloudDataplexV1MetadataEventFileset) super.clone();
  }
}
