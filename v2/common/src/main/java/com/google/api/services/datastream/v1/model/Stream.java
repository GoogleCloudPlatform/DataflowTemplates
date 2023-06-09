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
 * A resource representing streaming data from a source to a destination.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Stream extends com.google.api.client.json.GenericJson {

  /**
   * Automatically backfill objects included in the stream source configuration. Specific objects
   * can be excluded. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private BackfillAllStrategy backfillAll;

  /** Do not automatically backfill any objects. The value may be {@code null}. */
  @com.google.api.client.util.Key private BackfillNoneStrategy backfillNone;

  /** Output only. The creation time of the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /**
   * Immutable. A reference to a KMS encryption key. If provided, it will be used to encrypt the
   * data. If left blank, data will be encrypted using an internal Stream-specific encryption key
   * provisioned through KMS. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String customerManagedEncryptionKey;

  /** Required. Destination connection profile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private DestinationConfig destinationConfig;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Output only. Errors on the Stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Error> errors;

  /** Labels. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** Output only. The stream's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Required. Source connection profile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private SourceConfig sourceConfig;

  /** The state of the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Output only. The last update time of the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Automatically backfill objects included in the stream source configuration. Specific objects
   * can be excluded.
   *
   * @return value or {@code null} for none
   */
  public BackfillAllStrategy getBackfillAll() {
    return backfillAll;
  }

  /**
   * Automatically backfill objects included in the stream source configuration. Specific objects
   * can be excluded.
   *
   * @param backfillAll backfillAll or {@code null} for none
   */
  public Stream setBackfillAll(BackfillAllStrategy backfillAll) {
    this.backfillAll = backfillAll;
    return this;
  }

  /**
   * Do not automatically backfill any objects.
   *
   * @return value or {@code null} for none
   */
  public BackfillNoneStrategy getBackfillNone() {
    return backfillNone;
  }

  /**
   * Do not automatically backfill any objects.
   *
   * @param backfillNone backfillNone or {@code null} for none
   */
  public Stream setBackfillNone(BackfillNoneStrategy backfillNone) {
    this.backfillNone = backfillNone;
    return this;
  }

  /**
   * Output only. The creation time of the stream.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The creation time of the stream.
   *
   * @param createTime createTime or {@code null} for none
   */
  public Stream setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Immutable. A reference to a KMS encryption key. If provided, it will be used to encrypt the
   * data. If left blank, data will be encrypted using an internal Stream-specific encryption key
   * provisioned through KMS.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCustomerManagedEncryptionKey() {
    return customerManagedEncryptionKey;
  }

  /**
   * Immutable. A reference to a KMS encryption key. If provided, it will be used to encrypt the
   * data. If left blank, data will be encrypted using an internal Stream-specific encryption key
   * provisioned through KMS.
   *
   * @param customerManagedEncryptionKey customerManagedEncryptionKey or {@code null} for none
   */
  public Stream setCustomerManagedEncryptionKey(java.lang.String customerManagedEncryptionKey) {
    this.customerManagedEncryptionKey = customerManagedEncryptionKey;
    return this;
  }

  /**
   * Required. Destination connection profile configuration.
   *
   * @return value or {@code null} for none
   */
  public DestinationConfig getDestinationConfig() {
    return destinationConfig;
  }

  /**
   * Required. Destination connection profile configuration.
   *
   * @param destinationConfig destinationConfig or {@code null} for none
   */
  public Stream setDestinationConfig(DestinationConfig destinationConfig) {
    this.destinationConfig = destinationConfig;
    return this;
  }

  /**
   * Required. Display name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * Required. Display name.
   *
   * @param displayName displayName or {@code null} for none
   */
  public Stream setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Output only. Errors on the Stream.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Error> getErrors() {
    return errors;
  }

  /**
   * Output only. Errors on the Stream.
   *
   * @param errors errors or {@code null} for none
   */
  public Stream setErrors(java.util.List<Error> errors) {
    this.errors = errors;
    return this;
  }

  /**
   * Labels.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Labels.
   *
   * @param labels labels or {@code null} for none
   */
  public Stream setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The stream's name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The stream's name.
   *
   * @param name name or {@code null} for none
   */
  public Stream setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. Source connection profile configuration.
   *
   * @return value or {@code null} for none
   */
  public SourceConfig getSourceConfig() {
    return sourceConfig;
  }

  /**
   * Required. Source connection profile configuration.
   *
   * @param sourceConfig sourceConfig or {@code null} for none
   */
  public Stream setSourceConfig(SourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;
    return this;
  }

  /**
   * The state of the stream.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * The state of the stream.
   *
   * @param state state or {@code null} for none
   */
  public Stream setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. The last update time of the stream.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The last update time of the stream.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public Stream setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public Stream set(String fieldName, Object value) {
    return (Stream) super.set(fieldName, value);
  }

  @Override
  public Stream clone() {
    return (Stream) super.clone();
  }
}
