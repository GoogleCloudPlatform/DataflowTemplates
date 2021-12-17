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
 * Represent user-facing validation result message.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ValidationMessage extends com.google.api.client.json.GenericJson {

  /** A custom code identifying this specific message. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String code;

  /** Message level. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String level;

  /** Should be actionable. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** Any metadata related to the result. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> metadata;

  /**
   * A custom code identifying this specific message.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCode() {
    return code;
  }

  /**
   * A custom code identifying this specific message.
   *
   * @param code code or {@code null} for none
   */
  public ValidationMessage setCode(java.lang.String code) {
    this.code = code;
    return this;
  }

  /**
   * Message level.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLevel() {
    return level;
  }

  /**
   * Message level.
   *
   * @param level level or {@code null} for none
   */
  public ValidationMessage setLevel(java.lang.String level) {
    this.level = level;
    return this;
  }

  /**
   * Should be actionable.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Should be actionable.
   *
   * @param message message or {@code null} for none
   */
  public ValidationMessage setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * Any metadata related to the result.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getMetadata() {
    return metadata;
  }

  /**
   * Any metadata related to the result.
   *
   * @param metadata metadata or {@code null} for none
   */
  public ValidationMessage setMetadata(java.util.Map<String, java.lang.String> metadata) {
    this.metadata = metadata;
    return this;
  }

  @Override
  public ValidationMessage set(String fieldName, Object value) {
    return (ValidationMessage) super.set(fieldName, value);
  }

  @Override
  public ValidationMessage clone() {
    return (ValidationMessage) super.clone();
  }
}
