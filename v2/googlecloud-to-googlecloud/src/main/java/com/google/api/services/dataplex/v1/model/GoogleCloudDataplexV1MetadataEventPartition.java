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
 * Details about paritions of entities being created or deleted.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1MetadataEventPartition
    extends com.google.api.client.json.GenericJson {

  /** The number of data items in the partition. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataItemsCount;

  /**
   * The name of the table or fileset entity containing this partition. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String entity;

  /** The set of key/value pairs identifying the partition. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> keys;

  /**
   * The number of data items in the partition.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataItemsCount() {
    return dataItemsCount;
  }

  /**
   * The number of data items in the partition.
   *
   * @param dataItemsCount dataItemsCount or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventPartition setDataItemsCount(
      java.lang.Long dataItemsCount) {
    this.dataItemsCount = dataItemsCount;
    return this;
  }

  /**
   * The name of the table or fileset entity containing this partition.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEntity() {
    return entity;
  }

  /**
   * The name of the table or fileset entity containing this partition.
   *
   * @param entity entity or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventPartition setEntity(java.lang.String entity) {
    this.entity = entity;
    return this;
  }

  /**
   * The set of key/value pairs identifying the partition.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getKeys() {
    return keys;
  }

  /**
   * The set of key/value pairs identifying the partition.
   *
   * @param keys keys or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventPartition setKeys(
      java.util.Map<String, java.lang.String> keys) {
    this.keys = keys;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventPartition set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1MetadataEventPartition) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventPartition clone() {
    return (GoogleCloudDataplexV1MetadataEventPartition) super.clone();
  }
}
