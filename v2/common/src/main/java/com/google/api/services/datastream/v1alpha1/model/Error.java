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
 * Represent a user-facing Error.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Error extends com.google.api.client.json.GenericJson {

  /** Extra debugging information about the error. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> details;

  /** The time when the error occurred. The value may be {@code null}. */
  @com.google.api.client.util.Key private String errorTime;

  /**
   * A unique identifier for this specific error, allowing it to be traced throughout the system in
   * logs and API responses. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String errorUuid;

  /**
   * A message containing more information about the error that occurred. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String message;

  /** A title that explains the reason to the error. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String reason;

  /**
   * Extra debugging information about the error.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getDetails() {
    return details;
  }

  /**
   * Extra debugging information about the error.
   *
   * @param details details or {@code null} for none
   */
  public Error setDetails(java.util.Map<String, java.lang.String> details) {
    this.details = details;
    return this;
  }

  /**
   * The time when the error occurred.
   *
   * @return value or {@code null} for none
   */
  public String getErrorTime() {
    return errorTime;
  }

  /**
   * The time when the error occurred.
   *
   * @param errorTime errorTime or {@code null} for none
   */
  public Error setErrorTime(String errorTime) {
    this.errorTime = errorTime;
    return this;
  }

  /**
   * A unique identifier for this specific error, allowing it to be traced throughout the system in
   * logs and API responses.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getErrorUuid() {
    return errorUuid;
  }

  /**
   * A unique identifier for this specific error, allowing it to be traced throughout the system in
   * logs and API responses.
   *
   * @param errorUuid errorUuid or {@code null} for none
   */
  public Error setErrorUuid(java.lang.String errorUuid) {
    this.errorUuid = errorUuid;
    return this;
  }

  /**
   * A message containing more information about the error that occurred.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * A message containing more information about the error that occurred.
   *
   * @param message message or {@code null} for none
   */
  public Error setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * A title that explains the reason to the error.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getReason() {
    return reason;
  }

  /**
   * A title that explains the reason to the error.
   *
   * @param reason reason or {@code null} for none
   */
  public Error setReason(java.lang.String reason) {
    this.reason = reason;
    return this;
  }

  @Override
  public Error set(String fieldName, Object value) {
    return (Error) super.set(fieldName, value);
  }

  @Override
  public Error clone() {
    return (Error) super.clone();
  }
}
