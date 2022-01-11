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
package com.google.api.services.datastream.v1alpha1.model;

/**
 * Model definition for Stream.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Stream extends com.google.api.client.json.GenericJson {

  /** Backfill Strategy to backfill all Stream’s source objects. The value may be {@code null}. */
  @com.google.api.client.util.Key private BackfillAllStrategy backfillAll;

  /**
   * Backfill Strategy to disable backfill for all Stream’s source objects. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private BackfillNoneStrategy backfillNone;

  /** Output only. The create time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Destination connection profile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private DestinationConfig destinationConfig;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Labels. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /** Output only. The resource's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Required. Source connection profile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private SourceConfig sourceConfig;

  /** The state of the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Output only. The update time of the resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Backfill Strategy to backfill all Stream’s source objects.
   *
   * @return value or {@code null} for none
   */
  public BackfillAllStrategy getBackfillAll() {
    return backfillAll;
  }

  /**
   * Backfill Strategy to backfill all Stream’s source objects.
   *
   * @param backfillAll backfillAll or {@code null} for none
   */
  public Stream setBackfillAll(BackfillAllStrategy backfillAll) {
    this.backfillAll = backfillAll;
    return this;
  }

  /**
   * Backfill Strategy to disable backfill for all Stream’s source objects.
   *
   * @return value or {@code null} for none
   */
  public BackfillNoneStrategy getBackfillNone() {
    return backfillNone;
  }

  /**
   * Backfill Strategy to disable backfill for all Stream’s source objects.
   *
   * @param backfillNone backfillNone or {@code null} for none
   */
  public Stream setBackfillNone(BackfillNoneStrategy backfillNone) {
    this.backfillNone = backfillNone;
    return this;
  }

  /**
   * Output only. The create time of the resource.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The create time of the resource.
   *
   * @param createTime createTime or {@code null} for none
   */
  public Stream setCreateTime(String createTime) {
    this.createTime = createTime;
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
   * Output only. The resource's name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The resource's name.
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
   * Output only. The update time of the resource.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The update time of the resource.
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
