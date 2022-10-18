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
 * A specific stream object (e.g a specific DB table).
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class StreamObject extends com.google.api.client.json.GenericJson {

  /**
   * The latest backfill job that was initiated for the stream object. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private BackfillJob backfillJob;

  /** Output only. The creation time of the object. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Output only. Active errors on the object. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Error> errors;

  static {
    // hack to force ProGuard to consider Error used, since otherwise it would be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(Error.class);
  }

  /** Output only. The object resource's name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** The object identifier in the data source. The value may be {@code null}. */
  @com.google.api.client.util.Key private SourceObjectIdentifier sourceObject;

  /** Output only. The last update time of the object. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * The latest backfill job that was initiated for the stream object.
   *
   * @return value or {@code null} for none
   */
  public BackfillJob getBackfillJob() {
    return backfillJob;
  }

  /**
   * The latest backfill job that was initiated for the stream object.
   *
   * @param backfillJob backfillJob or {@code null} for none
   */
  public StreamObject setBackfillJob(BackfillJob backfillJob) {
    this.backfillJob = backfillJob;
    return this;
  }

  /**
   * Output only. The creation time of the object.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The creation time of the object.
   *
   * @param createTime createTime or {@code null} for none
   */
  public StreamObject setCreateTime(String createTime) {
    this.createTime = createTime;
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
  public StreamObject setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Output only. Active errors on the object.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Error> getErrors() {
    return errors;
  }

  /**
   * Output only. Active errors on the object.
   *
   * @param errors errors or {@code null} for none
   */
  public StreamObject setErrors(java.util.List<Error> errors) {
    this.errors = errors;
    return this;
  }

  /**
   * Output only. The object resource's name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The object resource's name.
   *
   * @param name name or {@code null} for none
   */
  public StreamObject setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * The object identifier in the data source.
   *
   * @return value or {@code null} for none
   */
  public SourceObjectIdentifier getSourceObject() {
    return sourceObject;
  }

  /**
   * The object identifier in the data source.
   *
   * @param sourceObject sourceObject or {@code null} for none
   */
  public StreamObject setSourceObject(SourceObjectIdentifier sourceObject) {
    this.sourceObject = sourceObject;
    return this;
  }

  /**
   * Output only. The last update time of the object.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The last update time of the object.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public StreamObject setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public StreamObject set(String fieldName, Object value) {
    return (StreamObject) super.set(fieldName, value);
  }

  @Override
  public StreamObject clone() {
    return (StreamObject) super.clone();
  }
}
