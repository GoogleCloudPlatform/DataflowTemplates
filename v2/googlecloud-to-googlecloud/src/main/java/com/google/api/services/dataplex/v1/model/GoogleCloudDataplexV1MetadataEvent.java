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
 * The payload associated with Metadata logs that contains events describing entities that were
 * discovered.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1MetadataEvent
    extends com.google.api.client.json.GenericJson {

  /** The id of the associated asset. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String assetId;

  /** The type of the entity being created, updated or deleted. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String entityType;

  /** The type of event. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String eventType;

  /** The fileset being created, updated or deleted. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1MetadataEventFileset fileset;

  /**
   * The unique id of the discovery job that resulted in this event. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String jobId;

  /** The log message. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** The partition being created or deleted. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1MetadataEventPartition partition;

  /** The table being created, updated or deleted. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1MetadataEventTable table;

  /**
   * The id of the associated asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAssetId() {
    return assetId;
  }

  /**
   * The id of the associated asset.
   *
   * @param assetId assetId or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setAssetId(java.lang.String assetId) {
    this.assetId = assetId;
    return this;
  }

  /**
   * The type of the entity being created, updated or deleted.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEntityType() {
    return entityType;
  }

  /**
   * The type of the entity being created, updated or deleted.
   *
   * @param entityType entityType or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setEntityType(java.lang.String entityType) {
    this.entityType = entityType;
    return this;
  }

  /**
   * The type of event.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEventType() {
    return eventType;
  }

  /**
   * The type of event.
   *
   * @param eventType eventType or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setEventType(java.lang.String eventType) {
    this.eventType = eventType;
    return this;
  }

  /**
   * The fileset being created, updated or deleted.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventFileset getFileset() {
    return fileset;
  }

  /**
   * The fileset being created, updated or deleted.
   *
   * @param fileset fileset or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setFileset(
      GoogleCloudDataplexV1MetadataEventFileset fileset) {
    this.fileset = fileset;
    return this;
  }

  /**
   * The unique id of the discovery job that resulted in this event.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getJobId() {
    return jobId;
  }

  /**
   * The unique id of the discovery job that resulted in this event.
   *
   * @param jobId jobId or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setJobId(java.lang.String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * The log message.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * The log message.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The partition being created or deleted.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventPartition getPartition() {
    return partition;
  }

  /**
   * The partition being created or deleted.
   *
   * @param partition partition or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setPartition(
      GoogleCloudDataplexV1MetadataEventPartition partition) {
    this.partition = partition;
    return this;
  }

  /**
   * The table being created, updated or deleted.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable getTable() {
    return table;
  }

  /**
   * The table being created, updated or deleted.
   *
   * @param table table or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEvent setTable(
      GoogleCloudDataplexV1MetadataEventTable table) {
    this.table = table;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1MetadataEvent set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1MetadataEvent) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1MetadataEvent clone() {
    return (GoogleCloudDataplexV1MetadataEvent) super.clone();
  }
}
