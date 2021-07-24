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
 * Response message for a 'FetchErrors' response.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class FetchErrorsResponse extends com.google.api.client.json.GenericJson {

  /** The list of errors on the Stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Error> errors;

  static {
    // hack to force ProGuard to consider Error used, since otherwise it would be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(Error.class);
  }

  /**
   * The list of errors on the Stream.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Error> getErrors() {
    return errors;
  }

  /**
   * The list of errors on the Stream.
   *
   * @param errors errors or {@code null} for none
   */
  public FetchErrorsResponse setErrors(java.util.List<Error> errors) {
    this.errors = errors;
    return this;
  }

  @Override
  public FetchErrorsResponse set(String fieldName, Object value) {
    return (FetchErrorsResponse) super.set(fieldName, value);
  }

  @Override
  public FetchErrorsResponse clone() {
    return (FetchErrorsResponse) super.clone();
  }
}
