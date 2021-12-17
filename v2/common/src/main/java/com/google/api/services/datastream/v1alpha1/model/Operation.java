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
 * This resource represents a long-running operation that is the result of a network API call.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class Operation extends com.google.api.client.json.GenericJson {

  /**
   * If the value is `false`, it means the operation is still in progress. If `true`, the operation
   * is completed, and either `error` or `response` is available. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean done;

  /**
   * The error result of the operation in case of failure or cancellation. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private Status error;

  /**
   * Service-specific metadata associated with the operation. It typically contains progress
   * information and common metadata such as create time. Some services might not provide such
   * metadata. Any method that returns a long-running operation should document the metadata type,
   * if any. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.Object> metadata;

  /**
   * The server-assigned name, which is only unique within the same service that originally returns
   * it. If you use the default HTTP mapping, the `name` should be a resource name ending with
   * `operations/{unique_id}`. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * The normal response of the operation in case of success. If the original method returns no data
   * on success, such as `Delete`, the response is `google.protobuf.Empty`. If the original method
   * is standard `Get`/`Create`/`Update`, the response should be the resource. For other methods,
   * the response should have the type `XxxResponse`, where `Xxx` is the original method name. For
   * example, if the original method name is `TakeSnapshot()`, the inferred response type is
   * `TakeSnapshotResponse`. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.Object> response;

  /**
   * If the value is `false`, it means the operation is still in progress. If `true`, the operation
   * is completed, and either `error` or `response` is available.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getDone() {
    return done;
  }

  /**
   * If the value is `false`, it means the operation is still in progress. If `true`, the operation
   * is completed, and either `error` or `response` is available.
   *
   * @param done done or {@code null} for none
   */
  public Operation setDone(java.lang.Boolean done) {
    this.done = done;
    return this;
  }

  /**
   * The error result of the operation in case of failure or cancellation.
   *
   * @return value or {@code null} for none
   */
  public Status getError() {
    return error;
  }

  /**
   * The error result of the operation in case of failure or cancellation.
   *
   * @param error error or {@code null} for none
   */
  public Operation setError(Status error) {
    this.error = error;
    return this;
  }

  /**
   * Service-specific metadata associated with the operation. It typically contains progress
   * information and common metadata such as create time. Some services might not provide such
   * metadata. Any method that returns a long-running operation should document the metadata type,
   * if any.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.Object> getMetadata() {
    return metadata;
  }

  /**
   * Service-specific metadata associated with the operation. It typically contains progress
   * information and common metadata such as create time. Some services might not provide such
   * metadata. Any method that returns a long-running operation should document the metadata type,
   * if any.
   *
   * @param metadata metadata or {@code null} for none
   */
  public Operation setMetadata(java.util.Map<String, java.lang.Object> metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * The server-assigned name, which is only unique within the same service that originally returns
   * it. If you use the default HTTP mapping, the `name` should be a resource name ending with
   * `operations/{unique_id}`.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * The server-assigned name, which is only unique within the same service that originally returns
   * it. If you use the default HTTP mapping, the `name` should be a resource name ending with
   * `operations/{unique_id}`.
   *
   * @param name name or {@code null} for none
   */
  public Operation setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * The normal response of the operation in case of success. If the original method returns no data
   * on success, such as `Delete`, the response is `google.protobuf.Empty`. If the original method
   * is standard `Get`/`Create`/`Update`, the response should be the resource. For other methods,
   * the response should have the type `XxxResponse`, where `Xxx` is the original method name. For
   * example, if the original method name is `TakeSnapshot()`, the inferred response type is
   * `TakeSnapshotResponse`.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.Object> getResponse() {
    return response;
  }

  /**
   * The normal response of the operation in case of success. If the original method returns no data
   * on success, such as `Delete`, the response is `google.protobuf.Empty`. If the original method
   * is standard `Get`/`Create`/`Update`, the response should be the resource. For other methods,
   * the response should have the type `XxxResponse`, where `Xxx` is the original method name. For
   * example, if the original method name is `TakeSnapshot()`, the inferred response type is
   * `TakeSnapshotResponse`.
   *
   * @param response response or {@code null} for none
   */
  public Operation setResponse(java.util.Map<String, java.lang.Object> response) {
    this.response = response;
    return this;
  }

  @Override
  public Operation set(String fieldName, Object value) {
    return (Operation) super.set(fieldName, value);
  }

  @Override
  public Operation clone() {
    return (Operation) super.clone();
  }
}
