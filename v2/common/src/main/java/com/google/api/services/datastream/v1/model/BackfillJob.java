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
 * Represents a backfill job on a specific stream object.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class BackfillJob extends com.google.api.client.json.GenericJson {

  /** Output only. Errors which caused the backfill job to fail. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Error> errors;

  static {
    // hack to force ProGuard to consider Error used, since otherwise it would be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(Error.class);
  }

  /** Output only. Backfill job's end time. The value may be {@code null}. */
  @com.google.api.client.util.Key private String lastEndTime;

  /** Output only. Backfill job's start time. The value may be {@code null}. */
  @com.google.api.client.util.Key private String lastStartTime;

  /** Backfill job state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Backfill job's triggering reason. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String trigger;

  /**
   * Output only. Errors which caused the backfill job to fail.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Error> getErrors() {
    return errors;
  }

  /**
   * Output only. Errors which caused the backfill job to fail.
   *
   * @param errors errors or {@code null} for none
   */
  public BackfillJob setErrors(java.util.List<Error> errors) {
    this.errors = errors;
    return this;
  }

  /**
   * Output only. Backfill job's end time.
   *
   * @return value or {@code null} for none
   */
  public String getLastEndTime() {
    return lastEndTime;
  }

  /**
   * Output only. Backfill job's end time.
   *
   * @param lastEndTime lastEndTime or {@code null} for none
   */
  public BackfillJob setLastEndTime(String lastEndTime) {
    this.lastEndTime = lastEndTime;
    return this;
  }

  /**
   * Output only. Backfill job's start time.
   *
   * @return value or {@code null} for none
   */
  public String getLastStartTime() {
    return lastStartTime;
  }

  /**
   * Output only. Backfill job's start time.
   *
   * @param lastStartTime lastStartTime or {@code null} for none
   */
  public BackfillJob setLastStartTime(String lastStartTime) {
    this.lastStartTime = lastStartTime;
    return this;
  }

  /**
   * Backfill job state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Backfill job state.
   *
   * @param state state or {@code null} for none
   */
  public BackfillJob setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Backfill job's triggering reason.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTrigger() {
    return trigger;
  }

  /**
   * Backfill job's triggering reason.
   *
   * @param trigger trigger or {@code null} for none
   */
  public BackfillJob setTrigger(java.lang.String trigger) {
    this.trigger = trigger;
    return this;
  }

  @Override
  public BackfillJob set(String fieldName, Object value) {
    return (BackfillJob) super.set(fieldName, value);
  }

  @Override
  public BackfillJob clone() {
    return (BackfillJob) super.clone();
  }
}
