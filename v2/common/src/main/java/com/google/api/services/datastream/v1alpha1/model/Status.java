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
 * The `Status` type defines a logical error model that is suitable for different programming
 * environments, including REST APIs and RPC APIs. It is used by [gRPC](https://github.com/grpc).
 * Each `Status` message contains three pieces of data: error code, error message, and error
 * details. You can find out more about this error model and how to work with it in the [API Design
 * Guide](https://cloud.google.com/apis/design/errors).
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Status extends com.google.api.client.json.GenericJson {

  /**
   * The status code, which should be an enum value of google.rpc.Code. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.Integer code;

  /**
   * A list of messages that carry the error details. There is a common set of message types for
   * APIs to use. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private java.util.List<java.util.Map<String, java.lang.Object>> details;

  /**
   * A developer-facing error message, which should be in English. Any user-facing error message
   * should be localized and sent in the google.rpc.Status.details field, or localized by the
   * client. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String message;

  /**
   * The status code, which should be an enum value of google.rpc.Code.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getCode() {
    return code;
  }

  /**
   * The status code, which should be an enum value of google.rpc.Code.
   *
   * @param code code or {@code null} for none
   */
  public Status setCode(java.lang.Integer code) {
    this.code = code;
    return this;
  }

  /**
   * A list of messages that carry the error details. There is a common set of message types for
   * APIs to use.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.util.Map<String, java.lang.Object>> getDetails() {
    return details;
  }

  /**
   * A list of messages that carry the error details. There is a common set of message types for
   * APIs to use.
   *
   * @param details details or {@code null} for none
   */
  public Status setDetails(java.util.List<java.util.Map<String, java.lang.Object>> details) {
    this.details = details;
    return this;
  }

  /**
   * A developer-facing error message, which should be in English. Any user-facing error message
   * should be localized and sent in the google.rpc.Status.details field, or localized by the
   * client.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * A developer-facing error message, which should be in English. Any user-facing error message
   * should be localized and sent in the google.rpc.Status.details field, or localized by the
   * client.
   *
   * @param message message or {@code null} for none
   */
  public Status setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  @Override
  public Status set(String fieldName, Object value) {
    return (Status) super.set(fieldName, value);
  }

  @Override
  public Status clone() {
    return (Status) super.clone();
  }
}
